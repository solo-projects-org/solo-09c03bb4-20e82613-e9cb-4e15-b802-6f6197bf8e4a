"""按站点保留期策略与过期已发布接收的清退。

规则：
- 清退以执行当天的 UTC 日期为基准，只清退「观测日 < 当天 - 保留天数」的已发布接收；
- 未配置保留策略的站点永不清退；
- 候选清单只是快照：逐件清退前在全局锁 + 写事务内重新核对
  (a) 接收身份（站号/观测日/声明摘要/成品摘要/创建与更新时间与快照完全一致）——
      同号删除重建的新接收不会被旧清单误删；
  (b) 当前策略——保留期被延长或策略被删除后，旧清单中的接收不再清退；
- 每件清退是独立短临界区（全局锁 + 接收锁 + 写事务），清点期间其他站点照常入湖；
- 接收登记删除与 blob 引用计数在同一事务提交；成品文件在提交后、全局锁内删除。
  因此双实例同时清退或中断重试不会重复回收额度；崩溃只可能留下无登记的孤儿
  文件（由对账清理），绝不会留下「成功记录指向缺件」。
"""

from __future__ import annotations

import datetime as dt
import os

from .config import Settings
from .db import Database
from .errors import RetentionPolicyNotFound
from .storage import FileLock, blob_path, global_lock, safe_remove

# 接收身份字段：候选快照与清退时逐字段比对，任一变化都视为“不是同一条接收”
_IDENTITY_FIELDS = (
    "station_id",
    "obs_date",
    "declared_sha256",
    "blob_sha256",
    "created_at",
    "updated_at",
)


def _utc_today() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


# ---------------------------------------------------------------- 策略 CRUD

def set_retention(db: Database, station_id: str, retain_days: int) -> dict:
    """设置（或更新）站点保留天数；同站重复设置幂等。"""
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO retention_policies(station_id, retain_days) VALUES (?, ?) "
            "ON CONFLICT(station_id) DO UPDATE SET "
            "retain_days = excluded.retain_days, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')",
            (station_id, retain_days),
        )
        return dict(
            conn.execute(
                "SELECT * FROM retention_policies WHERE station_id = ?", (station_id,)
            ).fetchone()
        )


def get_retention(db: Database, station_id: str) -> dict:
    row = db.query_one(
        "SELECT * FROM retention_policies WHERE station_id = ?", (station_id,)
    )
    if row is None:
        raise RetentionPolicyNotFound(f"站点 {station_id} 未配置保留策略")
    return dict(row)


def list_retentions(db: Database) -> list[dict]:
    return [
        dict(r)
        for r in db.query_all(
            "SELECT * FROM retention_policies ORDER BY station_id"
        )
    ]


def delete_retention(db: Database, station_id: str) -> None:
    """删除站点保留策略；未配置时 404。删除后该站点不再参与清退。"""
    with db.transaction() as conn:
        cur = conn.execute(
            "DELETE FROM retention_policies WHERE station_id = ?", (station_id,)
        )
        if cur.rowcount == 0:
            raise RetentionPolicyNotFound(f"站点 {station_id} 未配置保留策略")


# ---------------------------------------------------------------- 清退

def _expired_candidates(
    db: Database, purge_date: dt.date, station_id: str | None
) -> list[dict]:
    """候选快照：已发布、站点已配置策略、观测日早于 基准日-保留天数。

    不加锁的普通只读查询；快照内容在逐件清退时会被重新核对，
    因此清单过期（接收被删/重建、策略被改）不会造成误删。
    """
    sql = (
        "SELECT r.receipt_id, r.station_id, r.obs_date, r.declared_sha256, "
        "r.blob_sha256, r.created_at, r.updated_at, p.retain_days "
        "FROM receipts r JOIN retention_policies p ON p.station_id = r.station_id "
        "WHERE r.state = 'PUBLISHED'"
    )
    params: tuple = ()
    if station_id is not None:
        sql += " AND r.station_id = ?"
        params = (station_id,)
    sql += " ORDER BY r.station_id, r.obs_date, r.receipt_id"
    out: list[dict] = []
    for r in db.query_all(sql, params):
        cutoff = purge_date - dt.timedelta(days=int(r["retain_days"]))
        if dt.date.fromisoformat(r["obs_date"]) < cutoff:
            out.append(
                {
                    "receipt_id": r["receipt_id"],
                    "station_id": r["station_id"],
                    "obs_date": r["obs_date"],
                    "declared_sha256": r["declared_sha256"],
                    "blob_sha256": r["blob_sha256"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                }
            )
    return out


def _purge_one(
    settings: Settings, db: Database, snap: dict, *, purge_date: dt.date
) -> dict:
    """逐件清退：短临界区内复核身份与当前策略后才删除。

    锁顺序与 delete_receipt 一致（全局锁 → 接收锁），接收锁非阻塞：
    拿不到说明该接收正被上传会话占用，本轮跳过而不是阻塞清点。
    """
    receipt_id = snap["receipt_id"]

    def skip(reason: str) -> dict:
        return {"purged": False, "receipt_id": receipt_id, "reason": reason}

    with global_lock(settings):
        lock = FileLock(os.path.join(settings.lock_dir, f"rcpt-{receipt_id}.lock"))
        if not lock.acquire(blocking=False):
            return skip("receipt_busy")
        try:
            remove_path: str | None = None
            freed = 0
            sha: str | None = None
            with db.transaction() as conn:
                row = conn.execute(
                    "SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)
                ).fetchone()
                if row is None:
                    # 另一实例已清退 / 管理员已删除：不重删、不重复回收额度
                    return skip("already_removed")
                if row["state"] != "PUBLISHED":
                    return skip("not_published")
                # 核对接收身份：同号删除重建（哪怕声明完全相同，
                # created_at/updated_at 也会变）不能让旧清单误删新接收
                for field in _IDENTITY_FIELDS:
                    if row[field] != snap[field]:
                        return skip("identity_changed")
                # 核对当前期限：以事务内读到的最新策略为准
                policy = conn.execute(
                    "SELECT retain_days FROM retention_policies WHERE station_id = ?",
                    (row["station_id"],),
                ).fetchone()
                if policy is None:
                    return skip("policy_removed")
                cutoff = purge_date - dt.timedelta(days=int(policy["retain_days"]))
                if dt.date.fromisoformat(row["obs_date"]) >= cutoff:
                    return skip("retention_extended")

                sha = row["blob_sha256"]
                if sha:
                    conn.execute(
                        "UPDATE blobs SET ref_count = MAX(ref_count - 1, 0) "
                        "WHERE sha256 = ?",
                        (sha,),
                    )
                    blob = conn.execute(
                        "SELECT size, ref_count FROM blobs WHERE sha256 = ?", (sha,)
                    ).fetchone()
                    if blob is not None and blob["ref_count"] == 0:
                        # 最后一个引用：注销账本（额度随提交释放），
                        # 文件在提交后删除；仍被引用的共享原件不动
                        conn.execute(
                            "DELETE FROM blobs WHERE sha256 = ?", (sha,)
                        )
                        remove_path = blob_path(settings, sha)
                        freed = int(blob["size"])
                conn.execute(
                    "DELETE FROM receipts WHERE receipt_id = ?", (receipt_id,)
                )

            # 事务已提交、仍在全局锁内（发布/对账不会穿插）才删成品文件：
            # 若此处崩溃，磁盘只留无登记的孤儿文件，由对账清理，
            # 不会出现“成功记录指向缺件”。
            blob_file_removed = False
            if remove_path is not None:
                try:
                    blob_file_removed = safe_remove(remove_path)
                except OSError:
                    # 登记已回收，文件留作孤儿由对账清理；清退事实不变
                    blob_file_removed = False
            return {
                "purged": True,
                "receipt_id": receipt_id,
                "station_id": snap["station_id"],
                "obs_date": snap["obs_date"],
                "blob_sha256": sha,
                "blob_file_removed": blob_file_removed,
                "freed_bytes": freed,
            }
        finally:
            lock.release()


def purge_expired(
    settings: Settings,
    db: Database,
    *,
    station_id: str | None = None,
    today: dt.date | None = None,
) -> dict:
    """执行一轮清退，返回实际清退的接收号与腾出额度。

    基准日为清退当天的 UTC 日期（整轮共用同一天，跨零点也一致）。
    单件失败不中断整批：其事务已回滚，记录为 skipped，下轮可重试。
    """
    purge_date = today or _utc_today()
    candidates = _expired_candidates(db, purge_date, station_id)
    items: list[dict] = []
    skipped: list[dict] = []
    for snap in candidates:
        try:
            outcome = _purge_one(settings, db, snap, purge_date=purge_date)
        except Exception as exc:  # noqa: BLE001 - 单件失败不拖垮整批
            outcome = {
                "purged": False,
                "receipt_id": snap["receipt_id"],
                "reason": f"error:{exc}"[:200],
            }
        if outcome["purged"]:
            items.append(
                {
                    "receipt_id": outcome["receipt_id"],
                    "station_id": outcome["station_id"],
                    "obs_date": outcome["obs_date"],
                    "blob_sha256": outcome["blob_sha256"],
                    "blob_file_removed": outcome["blob_file_removed"],
                    "freed_bytes": outcome["freed_bytes"],
                }
            )
        else:
            skipped.append(
                {"receipt_id": outcome["receipt_id"], "reason": outcome["reason"]}
            )
    return {
        "purge_date": purge_date.isoformat(),
        "purged_count": len(items),
        "purged_receipts": [i["receipt_id"] for i in items],
        "freed_bytes_total": sum(i["freed_bytes"] for i in items),
        "items": items,
        "skipped": skipped,
    }

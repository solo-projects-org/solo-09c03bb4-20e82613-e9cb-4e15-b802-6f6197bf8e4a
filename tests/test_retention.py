"""保留期策略与过期清退测试。

覆盖：策略 CRUD、清退口径（UTC 基准日 / 严格早于 / 仅已发布 / 未配置站点）、
实际清退接收号返回、共享原件保护、期限延长与同号重建防护、双实例并发清退
不重复回收、中断重试、清点期间其他站点可入湖、不留缺件成功记录。
"""

from __future__ import annotations

import datetime as dt
import pathlib
import threading
import time

import pytest

from conftest import declare_payload, make_parquet
from app import main as main_mod
from app import retention as retention_mod
from app.db import Database
from app.storage import blob_path, reconcile


def _today() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def _publish(client, tmp_path, rid, station, obs_date, salt=0.0, data=None):
    """声明并上传一个合法包，返回包字节。"""
    if data is None:
        data = make_parquet(
            tmp_path / f"{rid}.parquet", station_id=station, date=obs_date, salt=salt
        )
    r = client.put(
        f"/receipts/{rid}/declaration",
        json=declare_payload(
            rid, data, station_id=station, obs_date=obs_date.isoformat()
        ),
    )
    assert r.status_code == 200, r.text
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 200, r.text
    assert r.json()["state"] == "PUBLISHED"
    return data


def _snapshot(db, rid, today):
    snaps = [
        s
        for s in retention_mod._expired_candidates(db, today, None)
        if s["receipt_id"] == rid
    ]
    assert len(snaps) == 1
    return snaps[0]


# ---------------------------------------------------------------- 策略 CRUD


def test_retention_policy_crud(client):
    assert client.get("/retention/BUOY-X").status_code == 404

    r = client.put("/retention/BUOY-X", json={"retain_days": 30})
    assert r.status_code == 200 and r.json()["retain_days"] == 30
    # 同站重复设置：upsert 覆盖
    r = client.put("/retention/BUOY-X", json={"retain_days": 15})
    assert r.status_code == 200 and r.json()["retain_days"] == 15
    assert client.get("/retention/BUOY-X").json()["retain_days"] == 15

    assert client.put("/retention/BUOY-Y", json={"retain_days": 0}).status_code == 200
    rows = client.get("/retention").json()
    assert [r["station_id"] for r in rows] == ["BUOY-X", "BUOY-Y"]

    # 非法保留天数
    assert client.put("/retention/BUOY-X", json={"retain_days": -1}).status_code == 422

    assert client.delete("/retention/BUOY-X").status_code == 204
    assert client.get("/retention/BUOY-X").status_code == 404
    assert client.delete("/retention/BUOY-X").status_code == 404


# ---------------------------------------------------------------- 清退口径


def test_purge_only_expired_published_and_configured(client, tmp_path):
    today = _today()
    old = today - dt.timedelta(days=10)
    client.put("/retention/BUOY-A", json={"retain_days": 7})

    _publish(client, tmp_path, "RCV-A-OLD", "BUOY-A", old, salt=1.0)          # 过期 → 清退
    _publish(client, tmp_path, "RCV-A-EDGE", "BUOY-A", today - dt.timedelta(days=7), salt=2.0)  # 边界 → 保留
    _publish(client, tmp_path, "RCV-A-NEW", "BUOY-A", today, salt=3.0)        # 当天 → 保留
    _publish(client, tmp_path, "RCV-B-OLD", "BUOY-B", old, salt=4.0)          # 未配置站点 → 保留

    # 在途（未发布）的旧观测日接收：不在清退范围
    dangling = make_parquet(tmp_path / "dangling.parquet", station_id="BUOY-A",
                            date=old, salt=5.0)
    client.put("/receipts/RCV-A-UP/declaration",
               json=declare_payload("RCV-A-UP", dangling, station_id="BUOY-A",
                                    obs_date=old.isoformat()))

    r = client.post("/admin/purge")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["purge_date"] == today.isoformat()
    assert body["purged_receipts"] == ["RCV-A-OLD"]
    assert body["purged_count"] == 1
    assert body["skipped"] == []

    # 被清退的接收登记已删除（404），其余完好
    assert client.get("/receipts/RCV-A-OLD").status_code == 404
    for rid in ("RCV-A-EDGE", "RCV-A-NEW", "RCV-B-OLD"):
        assert client.get(f"/receipts/{rid}").json()["state"] == "PUBLISHED"
        assert client.get(f"/receipts/{rid}/download").status_code == 200
    assert client.get("/receipts/RCV-A-UP").json()["state"] == "UPLOADING"


def test_purge_boundary_date(client, tmp_path):
    today = _today()
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    # 观测日 == 当天-保留天数 → 不早于截止日，保留
    _publish(client, tmp_path, "RCV-KEEP", "BUOY-A",
             today - dt.timedelta(days=7), salt=1.0)
    # 观测日 == 当天-保留天数-1 → 严格早于，清退
    _publish(client, tmp_path, "RCV-DROP", "BUOY-A",
             today - dt.timedelta(days=8), salt=2.0)

    body = client.post("/admin/purge").json()
    assert body["purged_receipts"] == ["RCV-DROP"]
    assert client.get("/receipts/RCV-KEEP/download").status_code == 200


def test_purge_without_any_policy_purges_nothing(client, tmp_path):
    today = _today()
    _publish(client, tmp_path, "RCV-NOP", "BUOY-NOP",
             today - dt.timedelta(days=365), salt=1.0)
    body = client.post("/admin/purge").json()
    assert body["purged_count"] == 0
    assert client.get("/receipts/RCV-NOP/download").status_code == 200


def test_purge_station_filter(client, tmp_path):
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    client.put("/retention/BUOY-B", json={"retain_days": 7})
    _publish(client, tmp_path, "RCV-FA", "BUOY-A", old, salt=1.0)
    _publish(client, tmp_path, "RCV-FB", "BUOY-B", old, salt=2.0)

    body = client.post("/admin/purge", params={"station_id": "BUOY-A"}).json()
    assert body["purged_receipts"] == ["RCV-FA"]
    assert client.get("/receipts/RCV-FB/download").status_code == 200

    body = client.post("/admin/purge").json()
    assert body["purged_receipts"] == ["RCV-FB"]


def test_purge_frees_quota_exactly_once_and_retry_idempotent(client, tmp_path):
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    d1 = _publish(client, tmp_path, "RCV-Q1", "BUOY-A", old, salt=1.0)
    d2 = _publish(client, tmp_path, "RCV-Q2", "BUOY-A", old, salt=2.0)

    before = client.get("/quota").json()
    assert before["used_bytes"] == len(d1) + len(d2)

    body = client.post("/admin/purge").json()
    assert body["freed_bytes_total"] == len(d1) + len(d2)
    assert {i["receipt_id"] for i in body["items"]} == {"RCV-Q1", "RCV-Q2"}
    assert all(i["blob_file_removed"] for i in body["items"])

    after = client.get("/quota").json()
    assert after["used_bytes"] == 0
    assert before["used_bytes"] - after["used_bytes"] == body["freed_bytes_total"]

    # 中断后重试 / 重复执行：不再清退、不重复回收额度
    body2 = client.post("/admin/purge").json()
    assert body2["purged_count"] == 0 and body2["freed_bytes_total"] == 0
    assert client.get("/quota").json()["used_bytes"] == 0


# ---------------------------------------------------------------- 共享原件


def test_shared_blob_survives_until_last_reference_purged(
    client, settings, tmp_path, helpers
):
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    data = make_parquet(tmp_path / "shared.parquet", station_id="BUOY-A",
                        date=old, salt=1.0)
    _publish(client, tmp_path, "RCV-S1", "BUOY-A", old, data=data)
    _publish(client, tmp_path, "RCV-S2", "BUOY-A", old, data=data)
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(blob_path(settings, sha))
    assert blob.exists()

    db = main_mod.db
    snap1 = _snapshot(db, "RCV-S1", today)
    snap2 = _snapshot(db, "RCV-S2", today)

    # 清退第一件：共享原件仍被 RCV-S2 引用，文件保留、额度未释放
    out1 = retention_mod._purge_one(settings, db, snap1, purge_date=today)
    assert out1["purged"] and out1["freed_bytes"] == 0 and not out1["blob_file_removed"]
    assert blob.exists()
    r = client.get("/receipts/RCV-S2/download")
    assert r.status_code == 200 and r.content == data

    # 清退第二件（最后一个引用）：文件删除、额度释放且只释放一次
    out2 = retention_mod._purge_one(settings, db, snap2, purge_date=today)
    assert out2["purged"] and out2["freed_bytes"] == len(data)
    assert out2["blob_file_removed"]
    assert not blob.exists()
    assert client.get("/quota").json()["used_bytes"] == 0


# ---------------------------------------------------------------- 快照失效防护


def test_retention_extended_after_snapshot_protects_receipt(client, settings, tmp_path):
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    _publish(client, tmp_path, "RCV-EXT", "BUOY-A", old, salt=1.0)
    db = main_mod.db
    snap = _snapshot(db, "RCV-EXT", today)

    # 清单生成后保留期被延长：旧清单不得再清退该接收
    client.put("/retention/BUOY-A", json={"retain_days": 3650})
    out = retention_mod._purge_one(settings, db, snap, purge_date=today)
    assert out == {"purged": False, "receipt_id": "RCV-EXT", "reason": "retention_extended"}
    assert client.get("/receipts/RCV-EXT/download").status_code == 200

    # 期限调回后，同一快照又可正常清退（跳过没有副作用）
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    out = retention_mod._purge_one(settings, db, snap, purge_date=today)
    assert out["purged"]
    assert client.get("/receipts/RCV-EXT").status_code == 404


def test_policy_removed_after_snapshot_skips(client, settings, tmp_path):
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    _publish(client, tmp_path, "RCV-POL", "BUOY-A", old, salt=1.0)
    db = main_mod.db
    snap = _snapshot(db, "RCV-POL", today)

    assert client.delete("/retention/BUOY-A").status_code == 204
    out = retention_mod._purge_one(settings, db, snap, purge_date=today)
    assert out == {"purged": False, "receipt_id": "RCV-POL", "reason": "policy_removed"}
    assert client.get("/receipts/RCV-POL/download").status_code == 200


def test_delete_and_recreate_same_id_not_purged_by_stale_list(
    client, settings, tmp_path
):
    """同号删除重建：旧清单不得误删新接收（无论新观测日是否仍过期）。"""
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    _publish(client, tmp_path, "RCV-RE", "BUOY-A", old, salt=1.0)
    db = main_mod.db
    stale = _snapshot(db, "RCV-RE", today)

    # 变体 A：重建为较新观测日
    assert client.delete("/receipts/RCV-RE").status_code == 204
    _publish(client, tmp_path, "RCV-RE", "BUOY-A", today, salt=2.0)
    out = retention_mod._purge_one(settings, db, stale, purge_date=today)
    assert out["purged"] is False and out["reason"] == "identity_changed"
    assert client.get("/receipts/RCV-RE").json()["state"] == "PUBLISHED"

    # 变体 B：重建为与旧接收完全相同的声明与内容（身份靠创建时间区分）
    assert client.delete("/receipts/RCV-RE").status_code == 204
    data = make_parquet(tmp_path / "same.parquet", station_id="BUOY-A",
                        date=old, salt=1.0)
    _publish(client, tmp_path, "RCV-RE", "BUOY-A", old, data=data)
    out = retention_mod._purge_one(settings, db, stale, purge_date=today)
    assert out["purged"] is False and out["reason"] == "identity_changed"
    assert client.get("/receipts/RCV-RE/download").status_code == 200


def test_purge_skips_receipt_reverted_to_uploading(client, settings, tmp_path, helpers):
    """快照后成品丢失被对账回退为 UPLOADING：清退跳过而非误删。"""
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    data = _publish(client, tmp_path, "RCV-REV", "BUOY-A", old, salt=1.0)
    db = main_mod.db
    snap = _snapshot(db, "RCV-REV", today)

    pathlib.Path(blob_path(settings, helpers.sha256_of(data))).unlink()
    reconcile(settings, db)  # 缺件 → 回退 UPLOADING

    out = retention_mod._purge_one(settings, db, snap, purge_date=today)
    assert out == {"purged": False, "receipt_id": "RCV-REV", "reason": "not_published"}
    assert client.get("/receipts/RCV-REV").json()["state"] == "UPLOADING"


# ---------------------------------------------------------------- 并发与中断


def test_concurrent_purge_two_instances_no_double_reclaim(
    client, settings, tmp_path, helpers
):
    """两个实例（两个 Database 连接）同时清退：每件只清退一次、额度不重复回收。"""
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})

    contents = [make_parquet(tmp_path / f"c{i}.parquet", station_id="BUOY-A",
                             date=old, salt=float(i)) for i in range(4)]
    shared = make_parquet(tmp_path / "shared.parquet", station_id="BUOY-A",
                          date=old, salt=99.0)
    for i, d in enumerate(contents):
        _publish(client, tmp_path, f"RCV-I{i}", "BUOY-A", old, data=d)
    # 共享原件的两个接收：引用计数 2，物理一份
    _publish(client, tmp_path, "RCV-IS1", "BUOY-A", old, data=shared)
    _publish(client, tmp_path, "RCV-IS2", "BUOY-A", old, data=shared)
    expected_freed = sum(len(d) for d in contents) + len(shared)
    all_ids = {f"RCV-I{i}" for i in range(4)} | {"RCV-IS1", "RCV-IS2"}

    db_a = Database(settings.db_path)
    db_b = Database(settings.db_path)
    results: list[dict] = []
    errors: list[Exception] = []

    def run(db):
        try:
            results.append(retention_mod.purge_expired(settings, db))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(d,)) for d in (db_a, db_b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    db_a.close()
    db_b.close()

    assert not errors
    purged_a, purged_b = (set(r["purged_receipts"]) for r in results)
    assert purged_a.isdisjoint(purged_b)          # 每件只被清退一次
    assert purged_a | purged_b == all_ids
    assert sum(r["freed_bytes_total"] for r in results) == expected_freed

    assert client.get("/quota").json()["used_bytes"] == 0
    for rid in all_ids:
        assert client.get(f"/receipts/{rid}").status_code == 404
    # 清退后账目与磁盘一致：对账无事可做
    assert reconcile(settings, main_mod.db) == {
        "removed_staging_files": 0,
        "removed_orphan_blobs": 0,
        "reset_receipts": 0,
    }
    assert not list(pathlib.Path(settings.blob_dir).rglob("*")) or all(
        p.is_dir() for p in pathlib.Path(settings.blob_dir).rglob("*")
    )


def test_purge_crash_inside_transaction_rolls_back_and_retry_ok(
    client, settings, tmp_path, monkeypatch, helpers
):
    """清退事务中途崩溃：整体回滚，重试后正常清退且额度只回收一次。"""
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    data = _publish(client, tmp_path, "RCV-CR", "BUOY-A", old, salt=1.0)
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(blob_path(settings, sha))
    db = main_mod.db

    # 模拟事务内崩溃（blob 路径解析即抛错）
    def boom(*a, **k):
        raise RuntimeError("模拟清退中途崩溃")

    monkeypatch.setattr(retention_mod, "blob_path", boom)
    result = retention_mod.purge_expired(settings, db)
    assert result["purged_count"] == 0
    assert result["skipped"][0]["reason"].startswith("error:")

    # 回滚完整：接收仍已发布、账本与文件都在、额度未变
    assert client.get("/receipts/RCV-CR").json()["state"] == "PUBLISHED"
    assert blob.exists()
    assert db.query_one("SELECT ref_count FROM blobs WHERE sha256 = ?", (sha,))[
        "ref_count"
    ] == 1
    assert client.get("/quota").json()["used_bytes"] == len(data)

    monkeypatch.undo()
    result = retention_mod.purge_expired(settings, db)
    assert result["purged_receipts"] == ["RCV-CR"]
    assert result["freed_bytes_total"] == len(data)
    assert client.get("/quota").json()["used_bytes"] == 0


def test_purge_file_remove_failure_leaves_no_dangling_success_record(
    client, settings, tmp_path, monkeypatch, helpers
):
    """登记已回收、成品文件删除失败：只留无登记孤儿文件（对账清理），
    绝不留下「成功记录指向缺件」。"""
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    data = _publish(client, tmp_path, "RCV-OF", "BUOY-A", old, salt=1.0)
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(blob_path(settings, sha))
    db = main_mod.db

    def fail_remove(path):
        raise OSError("模拟文件删除失败")

    monkeypatch.setattr(retention_mod, "safe_remove", fail_remove)
    result = retention_mod.purge_expired(settings, db)
    # 清退事实成立（登记已删），文件留作孤儿
    assert result["purged_receipts"] == ["RCV-OF"]
    assert result["items"][0]["blob_file_removed"] is False
    assert client.get("/receipts/RCV-OF").status_code == 404
    assert blob.exists()  # 孤儿文件
    assert db.query_one("SELECT 1 FROM blobs WHERE sha256 = ?", (sha,)) is None

    monkeypatch.undo()
    # 对账清理孤儿文件，且不产生任何“缺件成功记录”回退
    out = reconcile(settings, db)
    assert out["removed_orphan_blobs"] == 1
    assert out["reset_receipts"] == 0
    assert not blob.exists()


def test_other_station_can_ingest_during_purge(
    client, settings, tmp_path, monkeypatch
):
    """清点期间（逐件短临界区之间）其他站点照常声明、上传、发布。"""
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    for i in range(5):
        _publish(client, tmp_path, f"RCV-PA{i}", "BUOY-A", old, salt=float(i))

    started = threading.Event()
    real_purge_one = retention_mod._purge_one

    def slow_purge_one(*args, **kwargs):
        started.set()
        time.sleep(0.15)  # 放大逐件间隔，模拟长清点
        return real_purge_one(*args, **kwargs)

    monkeypatch.setattr(retention_mod, "_purge_one", slow_purge_one)

    box: dict = {}

    def run_purge():
        box["result"] = retention_mod.purge_expired(settings, main_mod.db)

    t = threading.Thread(target=run_purge)
    t.start()
    assert started.wait(5)

    # 清点进行中：另一站点完整入湖
    new_data = make_parquet(tmp_path / "new.parquet", station_id="BUOY-B",
                            date=today, salt=7.0)
    r = client.put("/receipts/RCV-NEW/declaration",
                   json=declare_payload("RCV-NEW", new_data, station_id="BUOY-B",
                                        obs_date=today.isoformat()))
    assert r.status_code == 200
    r = client.post("/receipts/RCV-NEW/content", content=new_data)
    assert r.status_code == 200 and r.json()["state"] == "PUBLISHED"
    assert t.is_alive()  # 入湖完成时清点尚未结束 → 未被清点阻塞

    t.join(10)
    assert box["result"]["purged_count"] == 5
    assert client.get("/receipts/RCV-NEW/download").status_code == 200


def test_purge_leaves_consistent_state_for_reconcile(client, settings, tmp_path):
    """清退后对账应无事可做：无孤儿、无缺件成功记录。"""
    today = _today()
    old = today - dt.timedelta(days=30)
    client.put("/retention/BUOY-A", json={"retain_days": 7})
    _publish(client, tmp_path, "RCV-Z1", "BUOY-A", old, salt=1.0)
    _publish(client, tmp_path, "RCV-Z2", "BUOY-A", today, salt=2.0)

    client.post("/admin/purge")
    out = reconcile(settings, main_mod.db)
    assert out == {"removed_staging_files": 0, "removed_orphan_blobs": 0,
                   "reset_receipts": 0}
    assert client.get("/receipts/RCV-Z2/download").status_code == 200


# ---------------------------------------------------------------- schema 迁移


def test_schema_migration_from_v1_creates_retention_table(settings):
    db = Database(settings.db_path)
    with db.transaction() as conn:
        conn.execute("DROP TABLE retention_policies")
        conn.execute(
            "UPDATE schema_meta SET value = '1' WHERE key = 'schema_version'"
        )
    db.close()

    db2 = Database(settings.db_path)  # 重开即迁移
    row = db2.query_one("SELECT value FROM schema_meta WHERE key = 'schema_version'")
    assert row["value"] == "2"
    assert db2.query_all("SELECT * FROM retention_policies") == []
    db2.close()

"""请求/响应模型与字段校验。"""

from __future__ import annotations

import datetime as dt
import re

from pydantic import BaseModel, Field, field_validator

_RECEIPT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}$")
_SHA_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class DeclarationIn(BaseModel):
    """接收声明：站号、UTC 日期、原包长度与 SHA-256。"""

    receipt_id: str = Field(..., description="接收号，同号重传沿用同一记录")
    station_id: str = Field(..., min_length=1, max_length=64, description="浮标站号")
    obs_date: dt.date = Field(..., description="观测 UTC 日期，ISO 格式 YYYY-MM-DD")
    size: int = Field(..., ge=0, description="原包字节长度（Parquet 文件大小）")
    sha256: str = Field(..., min_length=64, max_length=64, description="原包 SHA-256 十六进制摘要")

    @field_validator("receipt_id")
    @classmethod
    def _check_receipt(cls, v: str) -> str:
        if not _RECEIPT_RE.match(v):
            raise ValueError("接收号仅允许字母数字及 ._- 且以字母数字开头，最长 64 位")
        return v

    @field_validator("sha256")
    @classmethod
    def _check_sha(cls, v: str) -> str:
        if not _SHA_RE.match(v):
            raise ValueError("SHA-256 必须是 64 位十六进制字符串")
        return v.lower()


class ReceiptOut(BaseModel):
    receipt_id: str
    station_id: str
    obs_date: str
    declared_size: int
    declared_sha256: str
    state: str
    bytes_received: int
    actual_sha256: str | None
    blob_sha256: str | None
    row_count: int | None
    last_error: str | None
    created_at: str
    updated_at: str


class UploadResult(BaseModel):
    receipt_id: str
    state: str
    bytes_received: int
    declared_size: int
    actual_sha256: str | None
    row_count: int | None
    message: str


class QuotaOut(BaseModel):
    quota_bytes: int
    used_bytes: int
    blob_bytes: int
    staging_bytes: int
    available_bytes: int


class ReconcileOut(BaseModel):
    removed_staging_files: int
    removed_orphan_blobs: int
    reset_receipts: int


class RetentionPolicyIn(BaseModel):
    """站点保留天数：0 表示只保留 UTC 当天，更早的已发布接收都可清退。"""

    retain_days: int = Field(..., ge=0, le=36500, description="保留天数（0..36500）")


class RetentionPolicyOut(BaseModel):
    station_id: str
    retain_days: int
    updated_at: str


class PurgeItemOut(BaseModel):
    """一件实际清退的接收。"""

    receipt_id: str
    station_id: str
    obs_date: str
    blob_sha256: str | None
    blob_file_removed: bool = Field(..., description="成品文件是否随之删除（仍被其他接收引用时为 false）")
    freed_bytes: int = Field(..., description="本件实际腾出的额度字节（成品不再被引用时为其大小，否则为 0）")


class PurgeSkippedOut(BaseModel):
    """候选清单中、复核后未清退的接收及原因。"""

    receipt_id: str
    reason: str = Field(
        ...,
        description="already_removed / not_published / identity_changed / "
                    "policy_removed / retention_extended / receipt_busy / error:...",
    )


class PurgeResultOut(BaseModel):
    purge_date: str = Field(..., description="清退基准日（UTC 当天）")
    purged_count: int
    purged_receipts: list[str] = Field(..., description="实际清退的接收号")
    freed_bytes_total: int = Field(..., description="本次实际腾出的额度字节合计")
    items: list[PurgeItemOut]
    skipped: list[PurgeSkippedOut]

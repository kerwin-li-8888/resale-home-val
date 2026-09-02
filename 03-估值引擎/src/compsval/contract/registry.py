"""ExampleCity source/dataset registry (config.py deferred item, WP3-D).

Registered sources follow 数据取得与更新条件-V0.1.md (WP3-B); registered
datasets follow 数据字典-V0.1.md and the evidence captured by WP3-A / WP2
samples. Non-tabular raw evidence (page screenshots, saved text) under
``01-数据/raw`` is registered as ``raw_snapshot`` records with live sha256
fingerprints so ``compsval catalog`` can list it without a parquet main chain.

Evidence is scanned from the actual files on every call: fingerprints are
computed, never copied, so a re-run is stable and always reflects the stored
bytes (验收标准② 指纹稳定).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from compsval import config
from compsval.contract.models import (
    RawSnapshot,
    SnapshotFormat,
    SnapshotParseStatus,
    SourceAccessCondition,
    SourceAcquisitionMethod,
    SourceGranularity,
    SourceRegistry,
    SourceRepeatability,
    SourceRole,
    SourceStatus,
    SourceUpdateFrequency,
)

REGISTERED_AT = datetime(2026, 8, 21, 0, 0, 0, tzinfo=UTC)

# EXTFP0-A: 外部链家成交数据包登记时间（独立来源 lianjia_ext，2026-08-24）
EXT_REGISTERED_AT = datetime(2026, 8, 24, 0, 0, 0, tzinfo=UTC)

# 来源目录名 → source_registry.source_id（快照登记映射）
SOURCE_ID_BY_DIR: dict[str, str] = {
    "fang_esf": "SRC-005",
    "centanet": "SRC-006",
    "lianjia": "SRC-007",
    "gov_zfcj": "SRC-002",
    "58": "SRC-008",
    "lianjia_ext": "SRC-011",
}

# 原始证据文件（相对 01-数据/raw 的路径）→ 查询/入口 URL（EVIDENCE 来源为索引记录）
EVIDENCE_QUERY: dict[str, str] = {
    "source=58/dataset=ban_kkuai_price/fetched_at=20260820/58_targetdistrict_ban_kkuai_20260820.png": (
        "https://gz.58.com/fangjia/1657/"
    ),
    "source=centanet/dataset=chengjiao/fetched_at=20260820/centanet_xinghui_chengjiao_20260820.png": (  # noqa: E501
        "https://gz.centanet.com/xiaoqu/xq-0200027287/cj/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260820/fang_kangtai_chengjiao_20260820.png": (
        "https://esf.fang.com.example/loupan/2811007172/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260820/fang_kangtai_chengjiao_p2_20260820.png": (  # noqa: E501
        "https://esf.fang.com.example/loupan/2811007172/chengjiao/t11-a11-p12/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_guangda_chengjiao_20260821.png": (
        "https://esf.fang.com.example/loupan/2811052010/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_cuicheng_chengjiao_20260821.png": (
        "https://esf.fang.com.example/loupan/2811019201/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_zhonghaimingdu_chengjiao_20260821.png": (  # noqa: E501
        "https://esf.fang.com.example/loupan/2811021754/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_fuliqianxi_chengjiao_20260821.png": (  # noqa: E501
        "https://esf.fang.com.example/loupan/2811021775/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_yuexiuxinghui_chengjiao_20260821.png": (  # noqa: E501
        "https://esf.fang.com.example/loupan/2812279062/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_jinliju_chengjiao_20260821.png": (
        "https://esf.fang.com.example/loupan/2811327722/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_baolihongmian_chengjiao_20260821.png": (  # noqa: E501
        "https://esf.fang.com.example/loupan/2811006827/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_yijinghuayuan_chengjiao_20260821.png": (  # noqa: E501
        "https://esf.fang.com.example/loupan/2811086262/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_fenghuangxincun_chengjiao_20260821.png": (  # noqa: E501
        "https://esf.fang.com.example/loupan/2811342786/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_runnan_chengjiao_20260821.png": (
        "https://esf.fang.com.example/loupan/2811006902/chengjiao/"
    ),
    "source=fang_esf/dataset=chengjiao/fetched_at=20260821/fang_kangtai_chengjiao_20260821.png": (
        "https://esf.fang.com.example/loupan/2811007172/chengjiao/"
    ),
    "source=fang_esf/dataset=community_list/fetched_at=20260820/fang_dongxiaonan_community_list_20260820.png": (  # noqa: E501
        "https://esf.fang.com.example/housing/74_10076_1_39_0_0_1_0_0_0/"
    ),
    "source=gov_zfcj/dataset=surplus_house/fetched_at=20260820/zfcj_surplus_house_20260820.png": (
        "https://example-gov-source.example"
    ),
    "source=gov_zfcj/dataset=surplus_house/fetched_at=20260820/zfcj_surplus_house_targetdistrict_20260820.png": (  # noqa: E501
        "https://example-gov-source.example"
    ),
    "source=lianjia/dataset=chengjiao_list/fetched_at=20260821/lianjia_targetdistrict_chengjiao_list_20260821.txt": (  # noqa: E501
        "https://lianjia.com.example/chengjiao/targetdistrict/"
    ),
}


@dataclass(frozen=True)
class DatasetInfo:
    """示例城市数据集注册（compsval catalog 展示，WP3-D 目录注册）。"""

    dataset: str
    kind: str
    source_ids: tuple[str, ...]
    description: str


SOURCES: tuple[SourceRegistry, ...] = (
    SourceRegistry(
        source_id="SRC-001",
        name="示例城市住建局·存量房交易登记统计",
        publisher="示例城市住房和城乡建设局",
        role=SourceRole.P1,
        granularity=SourceGranularity.AGGREGATE_SALE,
        entry_url="https://example-gov-source.example",
        price_benchmark="登记统计",
        update_frequency=SourceUpdateFrequency.MONTHLY,
        access_condition=SourceAccessCondition.PUBLIC,
        acquisition_method=SourceAcquisitionMethod.DOWNLOAD,
        repeatability=SourceRepeatability.AUTOMATIC,
        status=SourceStatus.REGISTERED,
        registered_at=REGISTERED_AT,
    ),
    SourceRegistry(
        source_id="SRC-002",
        name="阳光家缘·存量房房源（示例城市住建局）",
        publisher="示例城市住房和城乡建设局",
        role=SourceRole.P1,
        granularity=SourceGranularity.LISTING,
        entry_url="https://example-gov-source.example",
        price_benchmark="官方挂牌",
        update_frequency=SourceUpdateFrequency.ON_LISTING,
        access_condition=SourceAccessCondition.PUBLIC,
        acquisition_method=SourceAcquisitionMethod.BROWSE,
        repeatability=SourceRepeatability.AUTOMATIC,
        status=SourceStatus.REGISTERED,
        registered_at=REGISTERED_AT,
    ),
    SourceRegistry(
        source_id="SRC-005",
        name="房天下·示例城市目标区·小区成交记录",
        publisher="房天下（fang.com）",
        role=SourceRole.P0,
        granularity=SourceGranularity.SALE_UNIT,
        entry_url="https://esf.fang.com.example/loupan/2811007172/chengjiao/",
        price_benchmark="平台披露",
        update_frequency=SourceUpdateFrequency.CONTINUOUS,
        access_condition=SourceAccessCondition.PUBLIC,
        acquisition_method=SourceAcquisitionMethod.BROWSE,
        repeatability=SourceRepeatability.AUTOMATIC,
        status=SourceStatus.ACTIVE,
        registered_at=REGISTERED_AT,
    ),
    SourceRegistry(
        source_id="SRC-006",
        name="中原地产·示例城市·小区成交",
        publisher="中原地产（centanet.com）",
        role=SourceRole.P0,
        granularity=SourceGranularity.SALE_UNIT,
        entry_url="https://gz.centanet.com/xiaoqu/xq-0200027287/cj/",
        price_benchmark="平台披露",
        update_frequency=SourceUpdateFrequency.CONTINUOUS,
        access_condition=SourceAccessCondition.PUBLIC,
        acquisition_method=SourceAcquisitionMethod.BROWSE,
        repeatability=SourceRepeatability.AUTOMATIC,
        status=SourceStatus.ACTIVE,
        registered_at=REGISTERED_AT,
    ),
    SourceRegistry(
        source_id="SRC-007",
        name="链家/贝壳·示例城市·成交与实体权威",
        publisher="链家（lianjia.com）/ 贝壳",
        role=SourceRole.P0,
        granularity=SourceGranularity.SALE_UNIT,
        entry_url="https://lianjia.com.example/chengjiao/targetdistrict/",
        price_benchmark="平台披露",
        update_frequency=SourceUpdateFrequency.CONTINUOUS,
        access_condition=SourceAccessCondition.CAPTCHA,
        acquisition_method=SourceAcquisitionMethod.MANUAL,
        repeatability=SourceRepeatability.CONDITIONAL,
        status=SourceStatus.ACTIVE,
        registered_at=REGISTERED_AT,
    ),
    SourceRegistry(
        source_id="SRC-008",
        name="58同城·示例城市目标区·板块均价",
        publisher="58同城（58.com）",
        role=SourceRole.P1,
        granularity=SourceGranularity.AGGREGATE_SALE,
        entry_url="https://gz.58.com/fangjia/1657/",
        price_benchmark="聚合",
        update_frequency=SourceUpdateFrequency.MONTHLY,
        access_condition=SourceAccessCondition.PUBLIC,
        acquisition_method=SourceAcquisitionMethod.BROWSE,
        repeatability=SourceRepeatability.AUTOMATIC,
        status=SourceStatus.REGISTERED,
        registered_at=REGISTERED_AT,
    ),
    SourceRegistry(
        source_id="SRC-009",
        name="安居客·示例城市·小区属性/挂牌",
        publisher="安居客（anjuke.com）",
        role=SourceRole.P1,
        granularity=SourceGranularity.LISTING,
        entry_url="https://mobile.anjuke.com/esf/city-cm/targetdistrict/",
        price_benchmark="平台挂牌",
        update_frequency=SourceUpdateFrequency.MONTHLY,
        access_condition=SourceAccessCondition.CAPTCHA,
        acquisition_method=SourceAcquisitionMethod.BROWSE,
        repeatability=SourceRepeatability.CONDITIONAL,
        status=SourceStatus.REGISTERED,
        registered_at=REGISTERED_AT,
    ),
    SourceRegistry(
        source_id="SRC-010",
        name="示例城市房地产中介协会·市场总量/板块活跃",
        publisher="示例城市房地产中介协会",
        role=SourceRole.P1,
        granularity=SourceGranularity.AGGREGATE_SALE,
        entry_url="UNKNOWN",
        price_benchmark="UNKNOWN",
        update_frequency=SourceUpdateFrequency.MONTHLY,
        access_condition=SourceAccessCondition.PUBLIC,
        acquisition_method=SourceAcquisitionMethod.BROWSE,
        repeatability=SourceRepeatability.AUTOMATIC,
        status=SourceStatus.REGISTERED,
        registered_at=REGISTERED_AT,
    ),
    SourceRegistry(
        source_id="SRC-011",
        name="外部链家/贝壳成交数据包（lianjia_ext）",
        publisher="链家/贝壳平台字段；来源取得渠道为用户提供本地文件",
        role=SourceRole.P0,
        granularity=SourceGranularity.SALE_UNIT,
        entry_url="UNKNOWN",  # 本地文件，无门户入口
        price_benchmark="平台披露",  # 技术方案 §5.1：不写成官方登记成交
        update_frequency=SourceUpdateFrequency.CONTINUOUS,
        access_condition=SourceAccessCondition.UNKNOWN,  # 用户提供文件，非在线访问
        acquisition_method=SourceAcquisitionMethod.DOWNLOAD,  # 用户提供本地文件
        repeatability=SourceRepeatability.CONDITIONAL,  # 依赖用户提供更新
        usage_terms="用户个人研究",  # 技术方案 §5.1 使用范围
        status=SourceStatus.REGISTERED,
        registered_at=EXT_REGISTERED_AT,
    ),
)


DATASETS: tuple[DatasetInfo, ...] = (
    DatasetInfo("chengjiao", "成交", ("SRC-005", "SRC-006"), "房天下/中原 成交事件原始页"),
    DatasetInfo("chengjiao_list", "成交列表", ("SRC-007",), "链家目标区成交列表原始文本"),
    DatasetInfo("community_list", "小区列表", ("SRC-005",), "房天下 板块小区列表"),
    DatasetInfo("surplus_house", "挂牌", ("SRC-002",), "官方存量房房源（阳光家缘）"),
    DatasetInfo("ban_kkuai_price", "市场序列", ("SRC-008",), "58同城 板块均价"),
    DatasetInfo("listing", "挂牌", ("SRC-002", "SRC-007", "SRC-009"), "挂牌事件（WP4 导入）"),
    # EXTFP0-A: 外部链家成交数据包数据集（技术方案 §5/§13）
    DatasetInfo(
        "chengjiao_xlsx",
        "成交",
        ("SRC-011",),
        "外部链家成交数据包逐行解析（用户提供 XLSX）",
    ),
    DatasetInfo(
        "floorplan_image",
        "户型图",
        ("SRC-011",),
        "外部链家成交记录户型图原始图片资产",
    ),
)


def registered_sources() -> tuple[SourceRegistry, ...]:
    return SOURCES


def registered_datasets() -> tuple[DatasetInfo, ...]:
    return DATASETS


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _partition_value(name: str) -> str:
    return name.split("=", 1)[1]


def _format_from_suffix(path: Path) -> SnapshotFormat:
    suffix = path.suffix.lower()
    if suffix == ".png":
        return SnapshotFormat.PNG
    if suffix == ".txt":
        return SnapshotFormat.TXT
    if suffix == ".html":
        return SnapshotFormat.HTML
    if suffix == ".json":
        return SnapshotFormat.JSON
    if suffix == ".parquet":
        return SnapshotFormat.PARQUET
    return SnapshotFormat.OTHER


def _snapshot_id(source: str, dataset: str, fetched_at: str, stem: str) -> str:
    return f"{source}-{dataset}-{fetched_at}-{stem}"


def list_evidence_snapshots(root: Path | None = None) -> list[RawSnapshot]:
    """Register every raw evidence file under an evidence root as a snapshot.

    One ``RawSnapshot`` record per evidence file (a page snapshot), with the
    live sha256 of the stored bytes. Layout mirrors the lake:
    ``source=<s>/dataset=<d>/fetched_at=<%Y%m%d>/<file>``.
    """
    evidence_root = root if root is not None else config.evidence_dir()
    if not evidence_root.is_dir():
        return []
    snapshots: list[RawSnapshot] = []
    for path in sorted(evidence_root.rglob("*")):
        if not path.is_file():
            continue
        parts = path.relative_to(evidence_root).parts
        if len(parts) != 4:
            continue
        source_dir, dataset_dir, fetched_dir, _filename = parts
        source = _partition_value(source_dir)
        dataset = _partition_value(dataset_dir)
        fetched_at = _partition_value(fetched_dir)
        source_id = SOURCE_ID_BY_DIR.get(source, "UNKNOWN")
        rel = str(path.relative_to(evidence_root)).replace("\\", "/")
        snapshots.append(
            RawSnapshot(
                snapshot_id=_snapshot_id(source, dataset, fetched_at, path.stem),
                source_id=source_id,
                dataset=dataset,
                fetched_at=datetime.strptime(fetched_at, "%Y%m%d").replace(tzinfo=UTC),
                query=EVIDENCE_QUERY.get(rel, "UNKNOWN"),
                content_hash=_sha256(path),
                file_count=1,
                record_count=0,
                format=_format_from_suffix(path),
                parse_status=SnapshotParseStatus.NOT_PARSED,
            )
        )
    return snapshots

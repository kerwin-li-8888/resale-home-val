"""DATA-005 census 别名补录批次（community-census-v1-2 归并复核裁决落表）。

追加式构建：读取既有 ``community_alias.parquet``（14 行名录批次），追加本
批次 73 行（57 一致 + 16 待定），再应用 2026-08-31 用户裁决 overrides
（5 行 promote/retarget、1 行 remove → 终态 72 行）；既有 ``A-`` 行逐字节
保留，同输入重跑幂等（先剔除已有 ``AC-`` 行再追加）。匹配消费语义不变：
仅 ``conflict_status=一致`` 的别名参与自动小区映射，待定行进 blocked
（不静默合并）。

裁决依据冻结于本 change ``review/alias-review-verdicts.csv``（57 一致 +
16 待定 + 4 拒绝；拒绝项不落表）与 ``_CENSUS_PENDING_OVERRIDES``（用户
裁决），由 ``tests/test_alias_census.py`` 保障对拍。

最终裁决（data005-alias-final-resolution）：``_ALIAS_REGISTRY_OVERRIDES``
把 8 条既有名录批次（`A-` 行）待定裁决改一致（3 promote + 5 retarget 至
同名标准小区）；泰沙路/工业大道南/工业大道 10 行维持待定。终态 86 行
（一致 72 / 待定 10 / 冲突 4）。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from compsval.contract.models import AliasConflictStatus, CommunityAlias
from compsval.entities.alias import (
    ALIAS_FILENAME,
    alias_table,
    write_alias_entity,
)
from compsval.entities.candidates import candidates_all
from compsval.entities.community import (
    COMMUNITY_FILENAME,
    ENTITIES_LAYER,
    community_id_of,
)
from compsval.ingest.manifests import InputRef

CENSUS_SOURCE_ID = "SRC-007"
CENSUS_BATCH_PREFIX = "AC-"
CENSUS_VERDICTS_REF = "data005-alias-backfill review/alias-review-verdicts.csv"


@dataclass(frozen=True)
class CensusAliasMapping:
    """一条 census 归并复核裁决的别名映射（community_alias 行）。"""

    source_name: str
    community_id: str
    status: AliasConflictStatus
    reason: str


_CENSUS_ALIAS_MAPPINGS: tuple[CensusAliasMapping, ...] = (
    CensusAliasMapping(
        source_name='万寿路',
        community_id='C-XXXX0132',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='东晓路',
        community_id='C-XXXX0167',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='仲恺路',
        community_id='C-XXXX0165',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区172宝通街',
        community_id='C-XXXX0098',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区132澜庭锦榕湾',
        community_id='C-XXXX0069',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区132和榕风景',
        community_id='C-XXXX0069',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区132榕城尚品公寓',
        community_id='C-XXXX0069',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区132水岸榕城',
        community_id='C-XXXX0069',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区089',
        community_id='C-XXXX0125',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='南华东路',
        community_id='C-XXXX0148',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='南华中路',
        community_id='C-XXXX0156',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='南华西路',
        community_id='C-XXXX0164',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='南村路',
        community_id='C-XXXX0134',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区169AB区',
        community_id='C-XXXX0020',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区169C区',
        community_id='C-XXXX0020',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区169东区',
        community_id='C-XXXX0020',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='南田路',
        community_id='C-XXXX0172',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='桐福中路',
        community_id='C-XXXX0137',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='桐福西路',
        community_id='C-XXXX0154',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='宝岗大道',
        community_id='C-XXXX0139',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区130A区',
        community_id='C-XXXX0063',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区130B区',
        community_id='C-XXXX0063',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='小港路',
        community_id='C-XXXX0169',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='工业大道中',
        community_id='C-XXXX0140',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='工业大道北',
        community_id='C-XXXX0135',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区232(目标区)',
        community_id='C-XXXX0040',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='新汀西路',
        community_id='C-XXXX0153',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='昌岗东路',
        community_id='C-XXXX0142',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='昌岗中路',
        community_id='C-XXXX0186',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区188晓园东',
        community_id='C-XXXX0099',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区188晓园北',
        community_id='C-XXXX0099',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区188晓园南',
        community_id='C-XXXX0099',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区188晓园新',
        community_id='C-XXXX0099',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='晓泊中马路',
        community_id='C-XXXX0159',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='晓泊西马路',
        community_id='C-XXXX0158',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区220一期',
        community_id='C-XXXX0038',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区220二期',
        community_id='C-XXXX0038',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区181(目标区)',
        community_id='C-XXXX0014',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区186102号大院',
        community_id='C-XXXX0145',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区139(目标区)',
        community_id='C-XXXX0111',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='江南大道中',
        community_id='C-XXXX0152',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='江南大道北',
        community_id='C-XXXX0131',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='江南西路',
        community_id='C-XXXX0150',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='江燕路',
        community_id='C-XXXX0161',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区135窝趣公寓',
        community_id='C-XXXX0104',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='海鹰路',
        community_id='C-XXXX0147',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='滨江中路',
        community_id='C-XXXX0160',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='滨江西路',
        community_id='C-XXXX0136',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='石岗路',
        community_id='C-XXXX0129',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='细岗路',
        community_id='C-XXXX0138',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区164',
        community_id='C-XXXX0181',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='荔福路',
        community_id='C-XXXX0130',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='远安路',
        community_id='C-XXXX0187',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区148(目标区)',
        community_id='C-XXXX0026',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区195(目标区)',
        community_id='C-XXXX0171',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='革新路',
        community_id='C-XXXX0133',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区165四期',
        community_id='C-XXXX0066',
        status=AliasConflictStatus.CONSISTENT,
        reason='结构相似唯一候选，无复合名冲突、无跨区括注',
    ),
    CensusAliasMapping(
        source_name='示例小区132榕岸',
        community_id='C-XXXX0069',
        status=AliasConflictStatus.PENDING,
        reason='双强候选：示例小区132 与 示例小区132榕岸华庭(E区)',
    ),
    CensusAliasMapping(
        source_name='示例小区132榕岸',
        community_id='C-XXXX0184',
        status=AliasConflictStatus.PENDING,
        reason='双强候选：示例小区132 与 示例小区132榕岸华庭(E区)',
    ),
    CensusAliasMapping(
        source_name='示例小区167二期示例小区244',
        community_id='C-XXXX0018',
        status=AliasConflictStatus.PENDING,
        reason='复合名含两个不同标准小区（示例小区167/示例小区244）',
    ),
    CensusAliasMapping(
        source_name='示例小区143示例小区245',
        community_id='C-XXXX0029',
        status=AliasConflictStatus.PENDING,
        reason='复合名含两个不同标准小区（示例小区143/示例小区245，跨板块），无法按名拆分',
    ),
    CensusAliasMapping(
        source_name='示例小区136拾光里',
        community_id='C-XXXX0033',
        status=AliasConflictStatus.PENDING,
        reason='复合名含两个不同标准小区（示例小区136/拾光里），开发关系待核',
    ),
    CensusAliasMapping(
        source_name='工业大道',
        community_id='C-XXXX0135',
        status=AliasConflictStatus.PENDING,
        reason='多道路门牌大院目标，无法唯一',
    ),
    CensusAliasMapping(
        source_name='工业大道',
        community_id='C-XXXX0140',
        status=AliasConflictStatus.PENDING,
        reason='多道路门牌大院目标，无法唯一',
    ),
    CensusAliasMapping(
        source_name='工业大道',
        community_id='C-XXXX0141',
        status=AliasConflictStatus.PENDING,
        reason='多道路门牌大院目标，无法唯一',
    ),
    CensusAliasMapping(
        source_name='工业大道',
        community_id='C-XXXX0168',
        status=AliasConflictStatus.PENDING,
        reason='多道路门牌大院目标，无法唯一',
    ),
    CensusAliasMapping(
        source_name='工业大道',
        community_id='C-XXXX0185',
        status=AliasConflictStatus.PENDING,
        reason='多道路门牌大院目标，无法唯一',
    ),
    CensusAliasMapping(
        source_name='工业大道南',
        community_id='C-XXXX0141',
        status=AliasConflictStatus.PENDING,
        reason='多道路门牌大院目标，无法唯一',
    ),
    CensusAliasMapping(
        source_name='工业大道南',
        community_id='C-XXXX0168',
        status=AliasConflictStatus.PENDING,
        reason='多道路门牌大院目标，无法唯一',
    ),
    CensusAliasMapping(
        source_name='工业大道南',
        community_id='C-XXXX0185',
        status=AliasConflictStatus.PENDING,
        reason='多道路门牌大院目标，无法唯一',
    ),
    CensusAliasMapping(
        source_name='示例小区186远洋宿舍',
        community_id='C-XXXX0145',
        status=AliasConflictStatus.PENDING,
        reason='疑似独立宿舍实体（另有标准名 示例小区024），归属待核',
    ),
    CensusAliasMapping(
        source_name='泰沙路',
        community_id='C-XXXX0089',
        status=AliasConflictStatus.PENDING,
        reason='双门牌标准名（示例小区006/示例小区013）无法唯一',
    ),
    CensusAliasMapping(
        source_name='泰沙路',
        community_id='C-XXXX0090',
        status=AliasConflictStatus.PENDING,
        reason='双门牌标准名（示例小区006/示例小区013）无法唯一',
    ),
)


@dataclass(frozen=True)
class CensusAliasOverride:
    """一条用户裁决 override：promote（待定→一致）/ retarget（改指目标并一致）/ remove。"""

    source_name: str
    action: str
    community_id: str | None
    reason: str


_CENSUS_PENDING_OVERRIDES: tuple[CensusAliasOverride, ...] = (
    CensusAliasOverride(
        source_name="示例小区132榕岸",
        action="remove",
        community_id="C-XXXX0069",
        reason="裁决指向榕岸华庭(E区)，本冗余待定行移除",
    ),
    CensusAliasOverride(
        source_name="示例小区132榕岸",
        action="retarget",
        community_id="C-XXXX0184",
        reason="名称直接对应榕岸华庭(E区)且其零样本，源名价格高于母小区17%符合新分期",
    ),
    CensusAliasOverride(
        source_name="示例小区186远洋宿舍",
        action="promote",
        community_id="C-XXXX0145",
        reason="新村内宿舍结构明确、无竞争候选",
    ),
    CensusAliasOverride(
        source_name="示例小区143示例小区245",
        action="retarget",
        community_id="C-XXXX0049",
        reason="用户确认示例小区143与示例小区245为两个小区，价差27%支持拆分",
    ),
    CensusAliasOverride(
        source_name="示例小区136拾光里",
        action="retarget",
        community_id="C-XXXX0051",
        reason="用户确认示例小区136与拾光里为两个小区，价差17%支持拆分",
    ),
    CensusAliasOverride(
        source_name="示例小区167二期示例小区244",
        action="retarget",
        community_id="C-XXXX0170",
        reason="用户确认示例小区167与示例小区244为两个小区，价差23%支持拆分",
    ),
)

_ADJUDICATION_REF = "2026-08-31 用户裁决（data005-alias-pending-resolution）"

_REGISTRY_ADJUDICATION_REF = "2026-08-31 用户裁决（data005-alias-final-resolution）"

#: 最终裁决（data005-alias-final-resolution）：作用于既有名录批次（`A-` 行）的
#: 8 条用户裁决（3 promote + 5 retarget，无 remove）。泰沙路 2 行、工业大道南
#: 3 行、工业大道 5 行经用户裁决维持待定（不入本表，继续 blocked）。
_ALIAS_REGISTRY_OVERRIDES: tuple[CensusAliasOverride, ...] = (
    CensusAliasOverride(
        source_name="示例小区202",
        action="promote",
        community_id="C-XXXX0052",
        reason="别名与标准名同名，145行/中位49417；板块口径差不影响同一性",
    ),
    CensusAliasOverride(
        source_name="春晖花苑(目标区)",
        action="promote",
        community_id="C-XXXX0027",
        reason="同区括注+花苑/花园变体，27行/中位22225；区内唯一春晖候选",
    ),
    CensusAliasOverride(
        source_name="示例小区242",
        action="promote",
        community_id="C-XXXX0188",
        reason="区内唯一星汇系住宅，123行/中位47175；超高层+年代+价格带吻合；板块字段差异记档",
    ),
    CensusAliasOverride(
        source_name="示例小区008",
        action="retarget",
        community_id="C-XXXX0151",
        reason="目录已有同名标准小区；二期/一期价差6%支持独立实体，原指向一期属名录误挂",
    ),
    CensusAliasOverride(
        source_name="示例小区039",
        action="retarget",
        community_id="C-XXXX0067",
        reason="目录已有同名标准小区，744行/中位39668；原指向示例小区047(7行)属名录误挂",
    ),
    CensusAliasOverride(
        source_name="示例小区053",
        action="retarget",
        community_id="C-XXXX0097",
        reason="目录已有同名标准小区，零样本",
    ),
    CensusAliasOverride(
        source_name="示例小区132榕岸华庭(E区)",
        action="retarget",
        community_id="C-XXXX0184",
        reason="目录已有同名标准小区；与上轮榕岸→榕岸华庭(E区)裁决同向",
    ),
    CensusAliasOverride(
        source_name="示例小区132榕景四季(D区)",
        action="retarget",
        community_id="C-XXXX0128",
        reason="目录已有同名标准小区，零样本",
    ),
)


def _apply_registry_overrides(rows: list[CommunityAlias]) -> list[CommunityAlias]:
    """把名录批次裁决 overrides 应用到既有行（promote/retarget，幂等护栏）。

    幂等护栏：`source_ref` 已含本轮裁决溯源的行视为已应用，跳过重复追加；
    promote/retarget 目标外键由构建入口统一校验。
    """
    overrides = {
        o.source_name: o
        for o in _ALIAS_REGISTRY_OVERRIDES
        if o.action in ("promote", "retarget")
    }
    out: list[CommunityAlias] = []
    for r in rows:
        o = overrides.get(r.source_alias)
        if o is None or _REGISTRY_ADJUDICATION_REF in r.source_ref:
            out.append(r)
            continue
        updates: dict[str, object] = {
            "conflict_status": AliasConflictStatus.CONSISTENT,
            "source_ref": f"{r.source_ref}；{_REGISTRY_ADJUDICATION_REF}：{o.reason}",
        }
        if o.community_id is not None and o.community_id != r.community_id:
            updates["community_id"] = o.community_id
        out.append(r.model_copy(update=updates))
    matched = {r.source_alias for r in out if r.source_alias in overrides}
    if len(matched) != len(overrides):
        missing = sorted(set(overrides) - matched)
        raise AssertionError(f"名录裁决 override 未命中任何既有行：{missing}")
    return out


def _apply_overrides(rows: list[CommunityAlias]) -> list[CommunityAlias]:
    removals = {
        (o.source_name, o.community_id)
        for o in _CENSUS_PENDING_OVERRIDES
        if o.action == "remove"
    }
    promotions = {
        o.source_name
        for o in _CENSUS_PENDING_OVERRIDES
        if o.action in ("promote", "retarget")
    }
    retargets = {
        o.source_name: o
        for o in _CENSUS_PENDING_OVERRIDES
        if o.action == "retarget"
    }
    out: list[CommunityAlias] = []
    for r in rows:
        if (r.source_alias, r.community_id) in removals:
            continue
        if r.source_alias in promotions:
            target = (
                retargets[r.source_alias].community_id
                if r.source_alias in retargets
                else r.community_id
            )
            reason = next(
                o.reason
                for o in _CENSUS_PENDING_OVERRIDES
                if o.source_name == r.source_alias and o.action in ("promote", "retarget")
            )
            r = r.model_copy(
                update={
                    "community_id": target,
                    "conflict_status": AliasConflictStatus.CONSISTENT,
                    "source_ref": f"{r.source_ref}；{_ADJUDICATION_REF}：{reason}",
                }
            )
        out.append(r)
    return out


def census_alias_rows() -> tuple[CommunityAlias, ...]:
    """把冻结裁决列表解析为 CommunityAlias 实体行（alias_id = AC-<seq>）。"""
    rows: list[CommunityAlias] = []
    for seq, m in enumerate(_CENSUS_ALIAS_MAPPINGS, start=1):
        rows.append(
            CommunityAlias(
                alias_id=f"{CENSUS_BATCH_PREFIX}{seq}",
                community_id=m.community_id,
                source_alias=m.source_name,
                source_id=CENSUS_SOURCE_ID,
                source_ref=(
                    "community-census-v1-2 merge_candidates + "
                    f"{CENSUS_VERDICTS_REF}：{m.source_name}（{m.reason}）"
                ),
                conflict_status=m.status,
            )
        )
    return tuple(rows)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _known_community_ids(data_dir: Path) -> set[str]:
    community_path = data_dir / ENTITIES_LAYER / COMMUNITY_FILENAME
    if community_path.is_file():
        return set(
            pq.read_table(community_path).column("community_id").to_pylist()
        )
    return {community_id_of(c.source_key) for c in candidates_all()}


def _read_existing_aliases(alias_path: Path) -> list[CommunityAlias]:
    table = pq.read_table(alias_path)
    kept: list[CommunityAlias] = []
    for aid, cid, name, sid, ref, status in zip(
        table.column("alias_id").to_pylist(),
        table.column("community_id").to_pylist(),
        table.column("source_alias").to_pylist(),
        table.column("source_id").to_pylist(),
        table.column("source_ref").to_pylist(),
        table.column("conflict_status").to_pylist(),
        strict=True,
    ):
        if aid.startswith(CENSUS_BATCH_PREFIX):
            continue
        kept.append(
            CommunityAlias(
                alias_id=aid,
                community_id=cid,
                source_alias=name,
                source_id=sid,
                source_ref=ref,
                conflict_status=AliasConflictStatus(status),
            )
        )
    return kept


def build_alias_census_backfill(
    *,
    data_dir: Path,
    verdicts_csv: Path,
    notes: str | None = None,
) -> Path:
    """追加 census 别名批次并原子重写 ``community_alias.parquet`` + manifest。

    幂等：已有 ``AC-`` 行先剔除再追加，既有非 AC 行原序保留；同输入重跑
    产出逐字节一致的表文件。
    """
    new_rows = census_alias_rows()
    known_ids = _known_community_ids(data_dir)
    for row in new_rows:
        if row.community_id not in known_ids:
            raise AssertionError(
                f"census 别名 {row.alias_id} 外键悬空：{row.community_id}"
            )
    for o in _ALIAS_REGISTRY_OVERRIDES:
        if o.community_id is not None and o.community_id not in known_ids:
            raise AssertionError(
                f"名录裁决 override 外键悬空：{o.source_name}→{o.community_id}"
            )

    entities_dir = data_dir / ENTITIES_LAYER
    alias_path = entities_dir / ALIAS_FILENAME
    kept = _read_existing_aliases(alias_path) if alias_path.is_file() else []

    merged = _apply_registry_overrides(kept) + _apply_overrides(list(new_rows))
    ids = [r.alias_id for r in merged]
    if len(ids) != len(set(ids)):
        raise AssertionError("alias_id 重复")

    inputs = [
        InputRef(
            dataset="census_alias_review_verdicts",
            fetched_at="2026-08-31",
            content_hash=(
                _sha256_file(verdicts_csv) if verdicts_csv.is_file() else None
            ),
        )
    ]
    if alias_path.is_file():
        inputs.append(
            InputRef(
                dataset="community_alias_previous",
                fetched_at="2026-08-31",
                content_hash=_sha256_file(alias_path),
            )
        )
    return write_alias_entity(
        alias_table(merged),
        data_dir=data_dir,
        inputs=inputs,
        notes=notes
        or (
            "DATA-005 census 别名补录批次+两轮用户裁决：终态 86 行"
            "（一致 72 / 待定 10 / 冲突 4）"
        ),
    )


__all__ = [
    "CENSUS_BATCH_PREFIX",
    "CENSUS_SOURCE_ID",
    "CensusAliasMapping",
    "_ALIAS_REGISTRY_OVERRIDES",
    "_CENSUS_ALIAS_MAPPINGS",
    "_CENSUS_PENDING_OVERRIDES",
    "build_alias_census_backfill",
    "census_alias_rows",
]

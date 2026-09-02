"""候选小区名录（WP5-A 骨架数据）：结构化转录自 候选小区名录-V0.1.md。

来源：01-数据/sources/候选小区名录-V0.1.md（DATA-001-C 交付物，边界已由用户确认
2026-08-21；来源=房天下 fang.com 目标区西部各板块小区列表第 1 页 + 58 板块均价 +
DATA-001-B 样本）。本模块把名录逐行转录为可编程数据，供 WP5-A 构建 community
小区实体权威表骨架。

转录纪律（每行均可追溯到名录节号+行号，验收②）：
- 仅转录**具来源ID（房天下 loupan ID）**的候选行；名录中"ID 待补"行（东泊南
  5 个：晓园花苑/晓泊中电信宿舍/万翠苑/鸿福大厦/晓泊中路130号）无稳定主键，
  不进入实体权威表，待扩充名录后回填。
- address：仅新港西（§2.12）名录给出地址；其余板块无地址 → "UNKNOWN"（不得用 0）。
- 不虚构坐标/地址：名录无任何坐标，故 latitude/longitude 一律 None。
- notes：原样转录名录备注（"—"→None）；boundary 判定规则见
  :func:`boundary_status_of` 与模块底部说明。

boundary_status 判定（机器可执行，逐行核对名录后固化）：
- 名录明确"需排除/非住宅物业/公寓不纳入普通住宅估值" → OUT_OF_SCOPE（正式范围外）
- 名录明确"需识别/待核验/待坐标/边界待定/非具体小区/板块级/道路级命名" → BOUNDARY_PENDING（边界待定）
- 其余（所在板块已确认纳入） → MACHINE_CONFIRMED（机器确认）
"""

# ruff: noqa: E501 — 名录转录数据行保持单行，便于与 候选小区名录-V0.1.md 逐行
# 核对溯源；超长源于完整中文备注，不改写为折行。

from __future__ import annotations

from dataclasses import dataclass

from compsval.contract.models import BoundaryStatus

# 板块 → (名录节号, 房天下板块ID)。实际纳入 12 个板块（名录 §1.1 注①）。
BLOCKS: tuple[tuple[str, str, str], ...] = (
    ("工业大道北", "2.1", "74_1227"),
    ("工业大道南", "2.2", "74_1228"),
    ("江南西", "2.3", "74_655"),
    ("宝岗", "2.4", "74_651"),
    ("昌岗路", "2.5", "74_5478"),
    ("南洲", "2.6", "74_650"),
    ("江燕路", "2.7", "74_1224"),
    ("前进路", "2.8", "74_1593"),
    ("滨江西", "2.9", "74_1229"),
    ("滨江中", "2.10", "74_14154"),
    ("东泊南", "2.11", "74_10076"),
    ("新港西", "2.12", "74_649"),
)

_MACHINE = BoundaryStatus.MACHINE_CONFIRMED
_PENDING = BoundaryStatus.BOUNDARY_PENDING
_OUT = BoundaryStatus.OUT_OF_SCOPE

# 每行：(标准名, 房天下loupanID, 地址(UNKNOWN=名录无), 边界状态, 名录备注)
_RAW: dict[str, tuple[tuple[str, str, str, BoundaryStatus, str | None], ...]] = {
    # ---- §2.1 工业大道北 ----
    "工业大道北": (
        ("示例小区132", "2811052010", "UNKNOWN", _MACHINE, "含分期（§3#7）"),
        ("示例小区001", "2811212296", "UNKNOWN", _MACHINE, None),
        ("示例小区042", "2811068722", "UNKNOWN", _MACHINE, None),
        ("示例小区021", "2812248650", "UNKNOWN", _MACHINE, None),
        ("示例小区046", "2811406066", "UNKNOWN", _MACHINE, "汐园片区归属本板块"),
        ("示例小区111", "2811200866", "UNKNOWN", _MACHINE, None),
        ("示例小区132榕岸华庭(E区)", "2812205736", "UNKNOWN", _MACHINE, "示例小区132分期（§3#7）"),
        ("示例小区017", "2811537394", "UNKNOWN", _PENDING, "板块级命名，非具体小区（§3#10 排除或单列）"),
        ("示例小区218", "2811065442", "UNKNOWN", _MACHINE, None),
        ("示例小区071", "2811934018", "UNKNOWN", _MACHINE, None),
        ("示例小区132榕景四季(D区)", "2811514782", "UNKNOWN", _MACHINE, "示例小区132分期（§3#7）"),
        ("示例小区065", "2811670902", "UNKNOWN", _MACHINE, None),
        ("示例小区128", "2811007340", "UNKNOWN", _MACHINE, None),
        ("示例小区148", "2811007121", "UNKNOWN", _MACHINE, None),
        ("示例小区025", "2811175338", "UNKNOWN", _OUT, "公寓，与示例小区026同项目拆分（§3#1：公寓不纳入普通住宅估值）"),
        ("示例小区211", "2811032061", "UNKNOWN", _MACHINE, None),
        ("示例小区095", "2811772938", "UNKNOWN", _MACHINE, None),
        ("示例小区086", "2811535340", "UNKNOWN", _MACHINE, None),
        ("示例小区199", "2812037352", "UNKNOWN", _MACHINE, None),
        ("示例小区196", "2811955074", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.2 工业大道南 ----
    "工业大道南": (
        ("示例小区026", "2812279062", "UNKNOWN", _MACHINE, "与'示例小区025'同项目拆分（§3#1）"),
        ("示例小区043", "2811017700", "UNKNOWN", _MACHINE, None),
        ("示例小区039", "2811049414", "UNKNOWN", _MACHINE, "金汐花园分期（§3#6）"),
        ("示例小区014", "2811172728", "UNKNOWN", _MACHINE, None),
        ("示例小区179", "2811177014", "UNKNOWN", _MACHINE, None),
        ("示例小区053", "2811284242", "UNKNOWN", _MACHINE, "金汐花园分期（§3#6）"),
        ("示例小区108", "2811200930", "UNKNOWN", _MACHINE, None),
        ("示例小区165", "2811042496", "UNKNOWN", _MACHINE, None),
        ("示例小区168", "2811007060", "UNKNOWN", _MACHINE, None),
        ("示例小区098", "2811535312", "UNKNOWN", _MACHINE, None),
        ("示例小区004", "2811557624", "UNKNOWN", _MACHINE, "门牌命名"),
        ("示例小区143", "2811007261", "UNKNOWN", _MACHINE, None),
        ("示例小区234", "2811328008", "UNKNOWN", _MACHINE, None),
        ("示例小区047", "2811175216", "UNKNOWN", _MACHINE, "金汐花园分期（§3#6）"),
        ("示例小区124", "2812198206", "UNKNOWN", _MACHINE, None),
        ("示例小区003", "2812206456", "UNKNOWN", _MACHINE, "门牌命名"),
        ("示例小区201", "2811599424", "UNKNOWN", _MACHINE, None),
        ("示例小区002", "2811863904", "UNKNOWN", _MACHINE, "门牌命名"),
        ("示例小区226", "2811608430", "UNKNOWN", _PENDING, "村集体物业，需识别使用性质"),
        ("示例小区123", "2811732910", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.3 江南西 ----
    "江南西": (
        ("示例小区156", "2811889210", "UNKNOWN", _MACHINE, None),
        ("示例小区157", "2811019080", "UNKNOWN", _MACHINE, None),
        ("示例小区077", "2811557636", "UNKNOWN", _MACHINE, None),
        ("示例小区145", "2812179192", "UNKNOWN", _MACHINE, "与示例小区144/示例小区217相似命名（§3#3，需核验地址）"),
        ("示例小区051", "2811623822", "UNKNOWN", _PENDING, "板块级命名（§3#10 排除或单列）"),
        ("示例小区094", "2811535322", "UNKNOWN", _MACHINE, None),
        ("示例小区176", "2811032195", "UNKNOWN", _MACHINE, None),
        ("示例小区223", "2811328006", "UNKNOWN", _MACHINE, None),
        ("示例小区052", "2811007034", "UNKNOWN", _MACHINE, None),
        ("示例小区190", "2811284414", "UNKNOWN", _MACHINE, None),
        ("示例小区063", "2811662194", "UNKNOWN", _MACHINE, None),
        ("示例小区049", "2811659468", "UNKNOWN", _PENDING, "道路级命名（§3#10 排除或单列）"),
        ("示例小区078", "2811705018", "UNKNOWN", _MACHINE, None),
        ("示例小区204", "2811659506", "UNKNOWN", _MACHINE, None),
        ("示例小区061", "2811648942", "UNKNOWN", _MACHINE, None),
        ("示例小区224", "2811328022", "UNKNOWN", _MACHINE, None),
        ("示例小区079", "2811639542", "UNKNOWN", _MACHINE, None),
        ("示例小区076", "2811695782", "UNKNOWN", _MACHINE, None),
        ("示例小区070", "2812044778", "UNKNOWN", _MACHINE, None),
        ("示例小区062", "2811618350", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.4 宝岗 ----
    "宝岗": (
        ("拾光里", "2811021647", "UNKNOWN", _MACHINE, "补数新增（2026-08-22 搜索定位，板块=宝岗，候选名录 §2.4 行1）"),
        ("示例小区045", "2811342786", "UNKNOWN", _MACHINE, None),
        ("示例小区217", "2811007084", "UNKNOWN", _MACHINE, "与示例小区144/示例小区145相似命名（§3#3，需核验地址）"),
        ("示例小区160", "2811135064", "UNKNOWN", _MACHINE, None),
        ("示例小区066", "2811536868", "UNKNOWN", _MACHINE, None),
        ("示例小区136", "2811007476", "UNKNOWN", _MACHINE, None),
        ("示例小区080", "2811006826", "UNKNOWN", _MACHINE, None),
        ("示例小区144", "2811341104", "UNKNOWN", _MACHINE, "与示例小区145/示例小区217相似命名（§3#3，需核验地址）"),
        ("示例小区028", "2811537352", "UNKNOWN", _PENDING, "道路级命名（§3#10 排除或单列）"),
        ("示例小区155", "2811321824", "UNKNOWN", _MACHINE, None),
        ("示例小区166", "2811405748", "UNKNOWN", _MACHINE, None),
        ("示例小区096", "2811536512", "UNKNOWN", _MACHINE, None),
        ("示例小区027", "2812128880", "UNKNOWN", _MACHINE, None),
        ("示例小区097", "2811888108", "UNKNOWN", _MACHINE, None),
        ("示例小区120", "2811284120", "UNKNOWN", _MACHINE, None),
        ("示例小区038", "2811714538", "UNKNOWN", _MACHINE, None),
        ("示例小区151", "2811405474", "UNKNOWN", _MACHINE, None),
        ("示例小区117", "2811328910", "UNKNOWN", _MACHINE, None),
        ("示例小区137", "2811328144", "UNKNOWN", _MACHINE, None),
        ("示例小区221", "2811006977", "UNKNOWN", _MACHINE, None),
        ("示例小区232", "2811007585", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.5 昌岗路 ----
    "昌岗路": (
        ("示例小区121", "2811019201", "UNKNOWN", _MACHINE, None),
        ("示例小区116", "2811017053", "UNKNOWN", _MACHINE, None),
        ("示例小区075", "2811204174", "UNKNOWN", _MACHINE, None),
        ("示例小区072", "2811200044", "UNKNOWN", _MACHINE, None),
        ("示例小区134", "2811006786", "UNKNOWN", _MACHINE, None),
        ("示例小区139", "2811328270", "UNKNOWN", _MACHINE, None),
        ("示例小区189", "2811284408", "UNKNOWN", _MACHINE, None),
        ("示例小区037", "2812210576", "UNKNOWN", _PENDING, "道路级命名（§3#10 排除或单列）"),
        ("示例小区167", "2811007047", "UNKNOWN", _MACHINE, None),
        ("示例小区016", "2811536710", "UNKNOWN", _PENDING, "跨板块命名（与工业大道北重叠，§3#10 排除或单列）"),
        ("示例小区244", "2811883848", "UNKNOWN", _MACHINE, None),
        ("示例小区203", "2811007655", "UNKNOWN", _MACHINE, None),
        ("示例小区147", "2811007092", "UNKNOWN", _MACHINE, None),
        ("示例小区067", "2811284382", "UNKNOWN", _MACHINE, None),
        ("示例小区140", "2811328292", "UNKNOWN", _MACHINE, None),
        ("示例小区158", "2811468262", "UNKNOWN", _MACHINE, None),
        ("示例小区186", "2811602828", "UNKNOWN", _MACHINE, None),
        ("示例小区222", "2811007045", "UNKNOWN", _MACHINE, None),
        ("示例小区036", "2811557630", "UNKNOWN", _MACHINE, None),
        ("示例小区207", "2811007282", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.6 南洲 ----
    "南洲": (
        ("示例小区193", "2811086262", "UNKNOWN", _MACHINE, None),
        ("示例小区198", "2811022425", "UNKNOWN", _MACHINE, None),
        ("示例小区119", "2811477542", "UNKNOWN", _MACHINE, None),
        ("示例小区229", "2811007513", "UNKNOWN", _MACHINE, None),
        ("示例小区172", "2811284316", "UNKNOWN", _MACHINE, None),
        ("示例小区169", "2811007074", "UNKNOWN", _MACHINE, "与示例小区170相似命名（§3#4，需核验）"),
        ("示例小区109", "2811200092", "UNKNOWN", _MACHINE, None),
        ("示例小区015", "2811212234", "UNKNOWN", _PENDING, "项目地块名，非小区（待识别）"),
        ("示例小区194", "2811068638", "UNKNOWN", _MACHINE, None),
        ("示例小区209", "2811148422", "UNKNOWN", _MACHINE, None),
        ("示例小区245", "2811019206", "UNKNOWN", _MACHINE, None),
        ("示例小区170", "2811007075", "UNKNOWN", _MACHINE, "与示例小区169相似命名（§3#4，需核验）"),
        ("示例小区175", "2811006897", "UNKNOWN", _MACHINE, None),
        ("示例小区114", "2811077061", "UNKNOWN", _MACHINE, None),
        ("示例小区184", "2811284080", "UNKNOWN", _MACHINE, None),
        ("示例小区197", "2811007604", "UNKNOWN", _MACHINE, None),
        ("示例小区187", "2811599032", "UNKNOWN", _MACHINE, None),
        ("示例小区164龙禧", "2812174704", "UNKNOWN", _MACHINE, "示例小区164分期"),
        ("示例小区055", "2811219896", "UNKNOWN", _MACHINE, None),
        ("示例小区005", "2811172768", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.7 江燕路 ----
    "江燕路": (
        ("示例小区031", "2811006827", "UNKNOWN", _MACHINE, None),
        ("示例小区029", "2811040556", "UNKNOWN", _MACHINE, "含二期（§3#5）"),
        ("示例小区113", "2811019076", "UNKNOWN", _MACHINE, None),
        ("示例小区171", "2811342782", "UNKNOWN", _MACHINE, None),
        ("示例小区030", "2811072340", "UNKNOWN", _MACHINE, None),
        ("示例小区230", "2811007517", "UNKNOWN", _MACHINE, None),
        ("示例小区227", "2811054173", "UNKNOWN", _MACHINE, None),
        ("示例小区105", "2811284420", "UNKNOWN", _MACHINE, None),
        ("示例小区127", "2811032155", "UNKNOWN", _MACHINE, None),
        ("示例小区032", "2811213902", "UNKNOWN", _MACHINE, None),
        ("示例小区073", "2811173596", "UNKNOWN", _PENDING, "疑似高端/特殊产品，需识别"),
        ("示例小区092", "2811723988", "UNKNOWN", _PENDING, "道路级命名（§3#10 排除或单列）"),
        ("示例小区008", "2811638662", "UNKNOWN", _MACHINE, "示例小区029分期（§3#5）"),
        ("示例小区191", "2811007558", "UNKNOWN", _MACHINE, None),
        ("示例小区149", "2811328684", "UNKNOWN", _MACHINE, None),
        ("示例小区214", "2811032157", "UNKNOWN", _MACHINE, None),
        ("示例小区174", "2811328010", "UNKNOWN", _MACHINE, None),
        ("示例小区210", "2811054059", "UNKNOWN", _MACHINE, None),
        ("示例小区040", "2811557622", "UNKNOWN", _MACHINE, None),
        ("示例小区087", "2811611904", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.8 前进路 ----
    "前进路": (
        ("示例小区216", "2812125888", "UNKNOWN", _MACHINE, None),
        ("示例小区162", "2812197890", "UNKNOWN", _PENDING, "公寓，需识别使用性质"),
        ("示例小区182", "2811406102", "UNKNOWN", _MACHINE, None),
        ("示例小区135", "2811327588", "UNKNOWN", _MACHINE, None),
        ("示例小区126", "2811032154", "UNKNOWN", _MACHINE, None),
        ("示例小区099", "2811670904", "UNKNOWN", _MACHINE, "单位宿舍"),
        ("示例小区141", "2811328296", "UNKNOWN", _MACHINE, None),
        ("示例小区100", "2811535338", "UNKNOWN", _MACHINE, None),
        ("示例小区125", "2812081244", "UNKNOWN", _MACHINE, None),
        ("示例小区181", "2811006914", "UNKNOWN", _MACHINE, None),
        ("示例小区185", "2811018907", "UNKNOWN", _MACHINE, None),
        ("示例小区054", "2811698548", "UNKNOWN", _MACHINE, None),
        ("示例小区161", "2811032798", "UNKNOWN", _MACHINE, None),
        ("示例小区059", "2812195384", "UNKNOWN", _MACHINE, None),
        ("示例小区115", "2811784372", "UNKNOWN", _MACHINE, None),
        ("示例小区153", "2811895542", "UNKNOWN", _MACHINE, None),
        ("示例小区159", "2811019078", "UNKNOWN", _MACHINE, None),
        ("示例小区228", "2811406146", "UNKNOWN", _MACHINE, None),
        ("示例小区060", "2811772498", "UNKNOWN", _MACHINE, None),
        ("示例小区183", "2811006917", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.9 滨江西 ----
    "滨江西": (
        ("示例小区130", "2811034445", "UNKNOWN", _MACHINE, "补数新增（2026-08-22 搜索定位，板块=滨江西，候选名录 §2.9 行1）"),
        ("示例小区202", "2811021754", "UNKNOWN", _MACHINE, "板块归属分歧：房天下=滨江西，安居客=滨江中（§3#8）"),
        ("示例小区033", "2811182462", "UNKNOWN", _MACHINE, None),
        ("示例小区034", "2811536716", "UNKNOWN", _PENDING, "道路级命名（§3#10 排除或单列）"),
        ("示例小区220", "2811007573", "UNKNOWN", _MACHINE, None),
        ("示例小区083", "2811614704", "UNKNOWN", _MACHINE, None),
        ("示例小区058", "2811673944", "UNKNOWN", _MACHINE, None),
        ("示例小区020", "2811535336", "UNKNOWN", _MACHINE, None),
        ("示例小区102", "2811863934", "UNKNOWN", _MACHINE, None),
        ("示例小区233", "2811071928", "UNKNOWN", _MACHINE, None),
        ("示例小区138", "2811284198", "UNKNOWN", _MACHINE, None),
        ("示例小区152", "2811895540", "UNKNOWN", _MACHINE, None),
        ("示例小区056", "2811619786", "UNKNOWN", _MACHINE, None),
        ("示例小区057", "2811738788", "UNKNOWN", _MACHINE, None),
        ("示例小区231", "2811007570", "UNKNOWN", _MACHINE, None),
        ("示例小区104", "2811341966", "UNKNOWN", _MACHINE, None),
        ("示例小区007", "2811219940", "UNKNOWN", _MACHINE, None),
        ("示例小区208", "2811329086", "UNKNOWN", _MACHINE, None),
        ("示例小区081", "2812212098", "UNKNOWN", _MACHINE, None),
        ("示例小区018", "2811899780", "UNKNOWN", _MACHINE, "单位宿舍"),
        ("示例小区206", "2811006836", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.10 滨江中 ----
    "滨江中": (
        ("示例小区146", "2811007086", "UNKNOWN", _MACHINE, None),
        ("示例小区090", "2811051623", "UNKNOWN", _MACHINE, None),
        ("华标品峰", "2811453502", "UNKNOWN", _MACHINE, None),
        ("示例小区133", "2811284180", "UNKNOWN", _MACHINE, None),
        ("示例小区205", "2811040495", "UNKNOWN", _MACHINE, None),
        ("示例小区163", "2811781542", "UNKNOWN", _MACHINE, None),
        ("示例小区069", "2811619770", "UNKNOWN", _MACHINE, None),
        ("示例小区118", "2811006834", "UNKNOWN", _MACHINE, None),
        ("示例小区106", "2811201118", "UNKNOWN", _MACHINE, None),
        ("示例小区150", "2811621590", "UNKNOWN", _OUT, "非住宅物业，需排除"),
        ("示例小区019", "2811536522", "UNKNOWN", _MACHINE, None),
        ("示例小区048", "2811625184", "UNKNOWN", _MACHINE, None),
        ("示例小区064", "2811720180", "UNKNOWN", _MACHINE, None),
        ("示例小区035", "2811722586", "UNKNOWN", _PENDING, "道路级命名（§3#10 排除或单列）"),
        ("示例小区107", "2812224336", "UNKNOWN", _MACHINE, None),
        ("示例小区213", "2811327566", "UNKNOWN", _MACHINE, None),
        ("示例小区091", "2811537146", "UNKNOWN", _MACHINE, None),
        ("示例小区112", "2811754288", "UNKNOWN", _MACHINE, None),
        ("示例小区044", "2811006794", "UNKNOWN", _MACHINE, None),
        ("示例小区212", "2811284182", "UNKNOWN", _MACHINE, None),
    ),
    # ---- §2.11 东泊南（名录 20 行中 5 行 ID 待补，不转录） ----
    "东泊南": (
        ("示例小区219", "2811327722", "UNKNOWN", _MACHINE, None),
        ("示例小区178", "2811170688", "UNKNOWN", _MACHINE, None),
        ("示例小区129", "2811342584", "UNKNOWN", _MACHINE, None),
        ("示例小区188", "2811284398", "UNKNOWN", _MACHINE, None),
        ("示例小区022", "2811694658", "UNKNOWN", _MACHINE, None),
        ("示例小区023", "2811694660", "UNKNOWN", _MACHINE, None),
        ("示例小区200", "2811032814", "UNKNOWN", _MACHINE, None),
        ("示例小区122", "2811284126", "UNKNOWN", _MACHINE, None),
        ("示例小区101", "2811537150", "UNKNOWN", _MACHINE, None),
        ("示例小区074", "2811177514", "UNKNOWN", _MACHINE, None),
        ("示例小区142", "2811680954", "UNKNOWN", _MACHINE, None),
        ("示例小区110", "2811200870", "UNKNOWN", _MACHINE, "建成年代缺失（DATA-001-B 发现）"),
        ("示例小区177", "2811006902", "UNKNOWN", _MACHINE, None),
        ("示例小区084", "2812261630", "UNKNOWN", _MACHINE, "建成年代缺失（DATA-001-B 发现）"),
        ("示例小区154", "2811007172", "UNKNOWN", _MACHINE, "DATA-001-B 成交样本 28 条；春晖花苑别名（§3#2）"),
    ),
    # ---- §2.12 新港西（西段判定见名录，address 有值） ----
    "新港西": (
        ("示例小区041", "2811021775", "新汀西路17号", _MACHINE, "西段（门牌<135）；D 采样 9 页高流动性"),
        ("示例小区131", "2811007363", "新汀西路68号", _MACHINE, "西段（门牌<135）"),
        ("示例小区180", "2811021607", "新汀西路立新东街1号", _MACHINE, "西段（中大西侧路网）"),
        ("示例小区225", "2811032696", "怡乐路70号", _MACHINE, "西段（中大西侧路网）"),
        ("示例小区068", "2811666618", "新汀西路（无门牌）", _PENDING, "边界待定（无门牌，待坐标）"),
        ("示例小区088", "2811006958", "怡乐路怡乐十巷1-3号", _MACHINE, "西段"),
        ("示例小区085", "2811097781", "怡乐路二巷5号", _MACHINE, "西段"),
        ("示例小区024", "2811514906", "新汀西路35号", _MACHINE, "西段（门牌<135）；单位宿舍"),
        ("示例小区050", "2811007080", "新汀西路82号", _MACHINE, "西段（门牌<135）"),
        ("示例小区009", "2812253600", "东泊南路与新汀西路交汇处", _PENDING, "边界待定（交汇处，待坐标；与一期同项目倾向西段）"),
        ("示例小区093", "2811659490", "新汀西路立新街7号", _MACHINE, "西段（中大西侧路网）"),
        ("示例小区192", "2811007581", "怡乐路17号", _MACHINE, "西段"),
        ("示例小区082", "2811794008", "目标区东晓路", _PENDING, "边界待定（东晓路非新汀西路门牌序列）"),
        ("示例小区010", "2811974218", "目标区新汀西路82号", _MACHINE, "西段（与一期同址82号）"),
        ("示例小区195", "2811888062", "新汀西路7号", _MACHINE, "西段（门牌<135）"),
        ("示例小区215", "2811007248", "怡乐路四巷22号", _MACHINE, "西段"),
        ("示例小区173", "2811724786", "新汀西路114号", _MACHINE, "西段（门牌<135）"),
        ("示例小区006", "2811218108", "泰沙路111号", _MACHINE, "西段（中大西侧路网）；门牌命名"),
        ("示例小区013", "2811218372", "泰沙路14号", _MACHINE, "西段；门牌命名"),
        ("示例小区103", "2811536864", "晓阳街22号903号", _MACHINE, "西段"),
    ),
}

# 名录中 ID 待补、无法构建实体行的候选（保留说明，见模块 docstring）
ID_PENDING_EXCLUDED: tuple[str, ...] = (
    "晓园花苑",
    "晓泊中电信宿舍",
    "万翠苑",
    "鸿福大厦",
    "晓泊中路130号",
)


@dataclass(frozen=True)
class CandidateCommunity:
    """候选小区名录一行（可编程表示）。"""

    standard_name: str
    source_key: str
    block: str
    address: str
    boundary: BoundaryStatus
    notes: str | None
    source_ref: str


def candidates_all() -> list[CandidateCommunity]:
    """按名录板块顺序转录全部具来源ID的候选行（235 个）。"""
    out: list[CandidateCommunity] = []
    for block, section, board_id in BLOCKS:
        for row_no, (name, key, address, boundary, notes) in enumerate(_RAW[block], start=1):
            out.append(
                CandidateCommunity(
                    standard_name=name,
                    source_key=key,
                    block=block,
                    address=address,
                    boundary=boundary,
                    notes=notes,
                    source_ref=(
                        f"候选小区名录-V0.1.md §{section} {block}({board_id}) 行{row_no}"
                    ),
                )
            )
    return out

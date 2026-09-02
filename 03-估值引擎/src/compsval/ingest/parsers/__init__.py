"""Source adapters that parse raw evidence into normalized, field-standard record rows.

Each adapter owns one source's layout: it turns the raw evidence (the immutable
raw snapshot imported by WP4-A) into typed, unit-standardized records that the
cleaning stage (WP4-C) maps onto sale_event / listing_event contracts. The
standardization rules live here; none of them mutate the raw snapshot.
"""

from compsval.ingest.parsers.fang_esf import (
    FANG_COMMUNITY_REGISTRY,
    FangEsfRecord,
    parse_fang_esf_csv,
    resolve_fang_community,
)
from compsval.ingest.parsers.fang_esf import (
    PARSER_VERSION as FANG_ESF_PARSER_VERSION,
)
from compsval.ingest.parsers.lianjia import (
    PARSER_VERSION as LIANJIA_PARSER_VERSION,
)
from compsval.ingest.parsers.lianjia import (
    LianjiaRecord,
    parse_lianjia_txt,
)
from compsval.ingest.parsers.lianjia_html import (
    PARSER_VERSION as LIANJIA_HTML_PARSER_VERSION,
)
from compsval.ingest.parsers.lianjia_html import (
    crosscheck_html_vs_log,
    extract_li_blocks,
    parse_lianjia_csv_table,
    parse_lianjia_html,
    write_lianjia_csv,
)

__all__ = [
    "FANG_COMMUNITY_REGISTRY",
    "FangEsfRecord",
    "FANG_ESF_PARSER_VERSION",
    "LianjiaRecord",
    "LIANJIA_PARSER_VERSION",
    "LIANJIA_HTML_PARSER_VERSION",
    "crosscheck_html_vs_log",
    "extract_li_blocks",
    "parse_fang_esf_csv",
    "parse_lianjia_csv_table",
    "parse_lianjia_html",
    "parse_lianjia_txt",
    "resolve_fang_community",
    "write_lianjia_csv",
]
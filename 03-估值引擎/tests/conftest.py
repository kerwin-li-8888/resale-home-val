"""Test-suite adjustments for the open-source distribution.

The open-source release replaces real community ids with unique synthetic ids
(C-XXXXnnnn, see README data-compliance section and NOTICE). A small set of
tests exercise community-id spaces that are constructed at runtime from real
catalog numeric ids (e.g. "C-" + source numeric id); synthetic ids can never
satisfy those runtime constructions, so they are skipped here.

If you fork this project with your own real catalog data, review this list:
those tests become meaningful again once real id spaces exist.
"""

from __future__ import annotations

import pytest

_SKIPPED_NODEIDS = {
    "test_entities.py::test_build_community_entity_roundtrip",
    "test_alias_census.py::test_build_appends_batch_applies_overrides_and_preserves_existing",
    "test_alias_census.py::test_adjudicated_rows_final_state",
    "test_alias_census.py::test_backfill_idempotent_byte_identical",
    "test_alias_census.py::test_adjudicated_aliases_resolve_and_pending_blocked",
    "test_alias_census.py::test_all_written_rows_pass_contract_model",
    "test_community_family.py::test_build_writes_expected_rows",
    "test_community_family.py::test_new_entities_c29_segment_and_family",
    "test_community_family.py::test_renames_applied_and_old_names_resolvable",
    "test_community_family.py::test_merges_keep_rows_with_redirect",
    "test_community_family.py::test_alias_targets_to_merged_entities_forward_consistently",
    "test_community_family.py::test_family_assignment_and_hengda_excluded",
    "test_community_family.py::test_rebuild_idempotent_byte_identical",
    "test_community_family.py::test_existing_community_rows_preserved_except_target_columns",
    "test_community_family.py::test_all_family_main_entities_exist_or_unknown",
}

_REASON = (
    "exercises community-id spaces constructed at runtime from real catalog "
    "numeric ids; real ids are excluded from the open-source distribution"
)


def pytest_collection_modifyitems(config, items):  # noqa: ANN001, ANN201
    for item in items:
        tail = "::".join(item.nodeid.split("::")[-2:])
        if tail in _SKIPPED_NODEIDS:
            item.add_marker(pytest.mark.skip(reason=_REASON))

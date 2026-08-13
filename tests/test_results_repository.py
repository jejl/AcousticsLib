"""Tests for the cross-observation read methods on ResultsRepository.

These are the only methods in the repository that query across more than one
observation, and the only ones that pick a column name at runtime — so the
table-name allowlist and the Probability/Score asymmetry both need guarding.

No database is required: ``get_session`` is replaced with a MagicMock context
manager and the emitted SQL is inspected directly.
"""
from unittest.mock import MagicMock, patch

import pytest

from calltrackerslib.database.repositories.results import (
    ResultsRepository,
    _score_column,
)
from calltrackerslib.exceptions import DatabaseError

_MOD = "calltrackerslib.database.repositories.results"
_ALLOWLIST = frozenset({"ResultsBats", "ResultsBitterns", "ResultsCurlews"})


def _patch_session(rows=()):
    """Patch get_session to a context manager yielding a mock session."""
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = list(rows)
    cm = MagicMock()
    cm.__enter__.return_value = session
    cm.__exit__.return_value = False
    return patch(f"{_MOD}.get_session", return_value=cm), session


def _sql_of(session):
    """Return the SQL text of the single execute() call."""
    return str(session.execute.call_args[0][0])


def _params_of(session):
    return session.execute.call_args[0][1]


@pytest.fixture(autouse=True)
def _allowlist():
    with patch(f"{_MOD}._get_table_allowlist", return_value=_ALLOWLIST):
        yield


# ---------------------------------------------------------------------------
# _score_column
# ---------------------------------------------------------------------------

class TestScoreColumn:
    def test_bats_use_probability(self):
        assert _score_column("ResultsBats") == "Probability"

    def test_everything_else_uses_score(self):
        assert _score_column("ResultsBitterns") == "Score"
        assert _score_column("ResultsCurlews") == "Score"


# ---------------------------------------------------------------------------
# get_observation_ids_with_detections
# ---------------------------------------------------------------------------

class TestGetObservationIdsWithDetections:
    def test_rejects_table_not_in_allowlist_without_querying(self):
        p, session = _patch_session()
        with p:
            with pytest.raises(DatabaseError, match="not in the allowed list"):
                ResultsRepository.get_observation_ids_with_detections("Users")
        session.execute.assert_not_called()

    def test_bats_query_uses_probability_not_score(self):
        p, session = _patch_session()
        with p:
            ResultsRepository.get_observation_ids_with_detections(
                "ResultsBats", min_score=0.9
            )
        sql = _sql_of(session)
        assert "`Probability` >= :min_score" in sql
        assert "Score" not in sql

    def test_bitterns_query_uses_score_not_probability(self):
        p, session = _patch_session()
        with p:
            ResultsRepository.get_observation_ids_with_detections(
                "ResultsBitterns", min_score=0.9
            )
        sql = _sql_of(session)
        assert "`Score` >= :min_score" in sql
        assert "Probability" not in sql

    def test_species_is_bound_never_interpolated(self):
        """A species value must never reach the SQL string itself."""
        nasty = "'; DROP TABLE calltrackers.LocationLog; --"
        p, session = _patch_session()
        with p:
            ResultsRepository.get_observation_ids_with_detections(
                "ResultsBats", species=nasty
            )
        sql = _sql_of(session)
        assert ":species" in sql
        assert "DROP TABLE" not in sql
        assert _params_of(session)["species"] == nasty

    def test_min_score_clause_omitted_when_none(self):
        p, session = _patch_session()
        with p:
            ResultsRepository.get_observation_ids_with_detections("ResultsBats")
        assert ">=" not in _sql_of(session)
        assert "min_score" not in _params_of(session)

    def test_no_id_excluded_by_default(self):
        p, session = _patch_session()
        with p:
            ResultsRepository.get_observation_ids_with_detections("ResultsBats")
        assert "<> :no_id" in _sql_of(session)
        assert _params_of(session)["no_id"] == "No ID"

    def test_blank_english_name_also_excluded(self):
        """ResultsBats writes Species='No ID' but leaves English_Name empty.

        Filtering on the sentinel alone lets ~242k unidentified bat rows
        through as a species named ''.
        """
        p, session = _patch_session()
        with p:
            ResultsRepository.get_observation_ids_with_detections("ResultsBats")
        sql = _sql_of(session)
        assert "TRIM(r.`English_Name`) <> ''" in sql
        assert "r.`Species` <> :no_id" in sql

    def test_explicit_species_replaces_the_no_id_clause(self):
        p, session = _patch_session()
        with p:
            ResultsRepository.get_observation_ids_with_detections(
                "ResultsBats", species="Gould's Wattled Bat"
            )
        params = _params_of(session)
        assert "no_id" not in params
        assert params["species"] == "Gould's Wattled Bat"

    def test_exclude_no_id_false_drops_the_clause(self):
        p, session = _patch_session()
        with p:
            ResultsRepository.get_observation_ids_with_detections(
                "ResultsBats", exclude_no_id=False
            )
        sql = _sql_of(session)
        assert "no_id" not in sql
        assert "WHERE" not in sql

    def test_groups_by_observation_id(self):
        p, session = _patch_session()
        with p:
            ResultsRepository.get_observation_ids_with_detections("ResultsBats")
        assert "GROUP BY r.observation_id" in _sql_of(session)

    def test_returns_plain_dicts(self):
        p, session = _patch_session([{"observation_id": 12, "n": 4}])
        with p:
            out = ResultsRepository.get_observation_ids_with_detections("ResultsBats")
        assert out == [{"observation_id": 12, "n": 4}]
        assert isinstance(out[0], dict)


# ---------------------------------------------------------------------------
# get_distinct_species
# ---------------------------------------------------------------------------

class TestGetDistinctSpecies:
    def test_rejects_table_not_in_allowlist_without_querying(self):
        p, session = _patch_session()
        with p:
            with pytest.raises(DatabaseError, match="not in the allowed list"):
                ResultsRepository.get_distinct_species("Recorder")
        session.execute.assert_not_called()

    def test_excludes_unidentified_and_orders_by_frequency(self):
        p, session = _patch_session()
        with p:
            ResultsRepository.get_distinct_species("ResultsBats")
        sql = _sql_of(session)
        assert "<> :no_id" in sql
        assert "TRIM(r.`English_Name`) <> ''" in sql
        assert "ORDER BY n DESC" in sql
        assert _params_of(session)["no_id"] == "No ID"

    def test_returns_plain_dicts(self):
        p, session = _patch_session(
            [{"species": "Gould's Wattled Bat", "n": 23854}]
        )
        with p:
            out = ResultsRepository.get_distinct_species("ResultsBats")
        assert out == [{"species": "Gould's Wattled Bat", "n": 23854}]

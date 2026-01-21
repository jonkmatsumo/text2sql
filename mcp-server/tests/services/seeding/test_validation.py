"""Unit tests for query-target startup validation."""

from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.services.seeding.validation import (
    ValidationResult,
    check_seed_artifacts,
    log_validation_summary,
    run_startup_validation,
    validate_query_target,
)


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_default_values(self):
        """Test ValidationResult has sensible defaults."""
        result = ValidationResult()
        assert result.db_reachable is True
        assert result.table_count == 0
        assert result.column_count == 0
        assert result.fk_count == 0
        assert result.queries_present is False
        assert result.tables_json_present is False
        assert result.errors == []
        assert result.warnings == []


class TestValidateQueryTarget:
    """Tests for validate_query_target function."""

    @pytest.mark.asyncio
    async def test_successful_validation(self):
        """Test validation with healthy database."""
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            [{"cnt": 15}],  # tables
            [{"cnt": 120}],  # columns
            [{"cnt": 10}],  # foreign keys
        ]

        result = await validate_query_target(mock_conn)

        assert result.db_reachable is True
        assert result.table_count == 15
        assert result.column_count == 120
        assert result.fk_count == 10
        assert result.errors == []

    @pytest.mark.asyncio
    async def test_database_error(self):
        """Test validation handles database errors gracefully."""
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = Exception("Connection refused")

        result = await validate_query_target(mock_conn)

        assert result.db_reachable is False
        assert len(result.errors) == 1
        assert "Connection refused" in result.errors[0]

    @pytest.mark.asyncio
    async def test_empty_database(self):
        """Test validation with zero tables."""
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            [{"cnt": 0}],  # tables
            [{"cnt": 0}],  # columns
            [{"cnt": 0}],  # foreign keys
        ]

        result = await validate_query_target(mock_conn)

        assert result.db_reachable is True
        assert result.table_count == 0


class TestCheckSeedArtifacts:
    """Tests for check_seed_artifacts function."""

    def test_artifacts_present(self, tmp_path):
        """Test when all seed artifacts are present."""
        # Create artifacts
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "test.json").write_text("[]")
        (tmp_path / "tables.json").write_text("[]")

        result = ValidationResult()
        check_seed_artifacts(tmp_path, result)

        assert result.queries_present is True
        assert result.tables_json_present is True
        assert result.warnings == []

    def test_artifacts_missing(self, tmp_path):
        """Test when seed artifacts are missing."""
        result = ValidationResult()
        check_seed_artifacts(tmp_path, result)

        assert result.queries_present is False
        assert result.tables_json_present is False
        assert len(result.warnings) == 2


class TestLogValidationSummary:
    """Tests for log_validation_summary function."""

    def test_logs_healthy_summary(self, caplog):
        """Test logging for healthy database."""
        result = ValidationResult(
            db_reachable=True,
            table_count=15,
            column_count=120,
            fk_count=10,
        )

        with caplog.at_level("INFO"):
            log_validation_summary(result)

        assert "Query-Target Database Contract Summary" in caplog.text
        assert "Tables:           15" in caplog.text
        assert "Contract satisfied" in caplog.text

    def test_logs_unreachable_error(self, caplog):
        """Test logging for unreachable database."""
        result = ValidationResult(
            db_reachable=False,
            errors=["Connection refused"],
        )

        with caplog.at_level("ERROR"):
            log_validation_summary(result)

        assert "Database unreachable" in caplog.text

    def test_logs_zero_tables_error(self, caplog):
        """Test logging for empty database."""
        result = ValidationResult(
            db_reachable=True,
            table_count=0,
        )

        with caplog.at_level("ERROR"):
            log_validation_summary(result)

        assert "No tables found" in caplog.text


class TestRunStartupValidation:
    """Tests for run_startup_validation function."""

    @pytest.mark.asyncio
    async def test_healthy_db_returns_true(self):
        """Test validation passes for healthy database."""
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            [{"cnt": 15}],
            [{"cnt": 120}],
            [{"cnt": 10}],
        ]

        result = await run_startup_validation(mock_conn, fail_fast=False)

        assert result is True

    @pytest.mark.asyncio
    async def test_empty_db_returns_false(self):
        """Test validation fails for empty database."""
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            [{"cnt": 0}],
            [{"cnt": 0}],
            [{"cnt": 0}],
        ]

        result = await run_startup_validation(mock_conn, fail_fast=False)

        assert result is False

    @pytest.mark.asyncio
    async def test_fail_fast_exits(self):
        """Test fail_fast=True exits on critical failure."""
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            [{"cnt": 0}],
            [{"cnt": 0}],
            [{"cnt": 0}],
        ]

        with pytest.raises(SystemExit) as exc_info:
            await run_startup_validation(mock_conn, fail_fast=True)

        assert exc_info.value.code == 1

    @pytest.mark.asyncio
    async def test_fail_fast_from_env(self):
        """Test fail_fast defaults from environment variable."""
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            [{"cnt": 0}],
            [{"cnt": 0}],
            [{"cnt": 0}],
        ]

        with patch.dict("os.environ", {"SEEDING_FAIL_FAST": "true"}):
            with pytest.raises(SystemExit):
                await run_startup_validation(mock_conn)


class TestRunMcpStartupValidation:
    """Tests for run_mcp_startup_validation function."""

    @pytest.mark.asyncio
    async def test_exits_on_empty_db(self):
        """Test MCP validation exits when no tables found."""
        from mcp_server.services.seeding.validation import run_mcp_startup_validation

        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            [{"cnt": 0}],  # tables
            [{"cnt": 0}],  # columns
            [{"cnt": 0}],  # foreign keys
        ]

        with pytest.raises(SystemExit) as exc_info:
            await run_mcp_startup_validation(mock_conn)

        assert exc_info.value.code == 1

    @pytest.mark.asyncio
    async def test_exits_on_db_unreachable(self):
        """Test MCP validation exits when DB is unreachable."""
        from mcp_server.services.seeding.validation import run_mcp_startup_validation

        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = Exception("Connection refused")

        with pytest.raises(SystemExit) as exc_info:
            await run_mcp_startup_validation(mock_conn)

        assert exc_info.value.code == 1

    @pytest.mark.asyncio
    async def test_passes_with_tables(self):
        """Test MCP validation passes with healthy DB."""
        from mcp_server.services.seeding.validation import run_mcp_startup_validation

        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            [{"cnt": 15}],
            [{"cnt": 120}],
            [{"cnt": 10}],
        ]

        # Should not raise
        await run_mcp_startup_validation(mock_conn)


class TestFkWarning:
    """Tests for FK constraint warning."""

    def test_fk_warning_logged_when_zero(self, caplog):
        """Test that FK warning is logged when fk_count is 0."""
        result = ValidationResult(
            db_reachable=True,
            table_count=15,
            column_count=120,
            fk_count=0,
        )

        with caplog.at_level("WARNING"):
            log_validation_summary(result)

        assert "fk_constraints_missing_or_undetected" in caplog.text
        assert "fk_edges=0" in caplog.text

    def test_no_fk_warning_when_fks_present(self, caplog):
        """Test that no FK warning is logged when FKs are present."""
        result = ValidationResult(
            db_reachable=True,
            table_count=15,
            column_count=120,
            fk_count=10,
        )

        with caplog.at_level("WARNING"):
            log_validation_summary(result)

        assert "fk_constraints_missing_or_undetected" not in caplog.text


class TestWarnIfQualityFilesMissing:
    """Tests for warn_if_quality_files_missing function."""

    def test_warns_when_tables_json_missing(self, tmp_path, caplog):
        """Test warning is logged when tables.json is missing."""
        import mcp_server.services.seeding.validation as validation_module
        from mcp_server.services.seeding.validation import warn_if_quality_files_missing

        # Reset the guard
        validation_module._tables_json_warning_emitted = False

        with caplog.at_level("WARNING"):
            warn_if_quality_files_missing(tmp_path)

        assert "tables_json_missing" in caplog.text

    def test_no_warn_when_tables_json_present(self, tmp_path, caplog):
        """Test no warning when tables.json exists."""
        import mcp_server.services.seeding.validation as validation_module
        from mcp_server.services.seeding.validation import warn_if_quality_files_missing

        # Create tables.json
        (tmp_path / "tables.json").write_text("[]")

        # Reset the guard
        validation_module._tables_json_warning_emitted = False

        with caplog.at_level("WARNING"):
            warn_if_quality_files_missing(tmp_path)

        assert "tables_json_missing" not in caplog.text

    def test_warns_only_once(self, tmp_path, caplog):
        """Test that warning is emitted only once."""
        import mcp_server.services.seeding.validation as validation_module
        from mcp_server.services.seeding.validation import warn_if_quality_files_missing

        # Reset the guard
        validation_module._tables_json_warning_emitted = False

        with caplog.at_level("WARNING"):
            warn_if_quality_files_missing(tmp_path)
            warn_if_quality_files_missing(tmp_path)

        # Should only appear once
        assert caplog.text.count("tables_json_missing") == 1

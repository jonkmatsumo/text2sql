"""Tests for retry utility."""

from unittest.mock import AsyncMock, patch

import pytest

from agent.utils.retry import is_transient_error, retry_with_backoff


class TestIsTransientError:
    """Tests for transient error detection."""

    def test_connection_error_is_transient(self):
        """Connection errors should be detected as transient."""
        err = ConnectionError("Connection refused")
        assert is_transient_error(err) is True

    def test_timeout_error_is_transient(self):
        """Timeout errors should be detected as transient."""
        err = TimeoutError("Connection timeout")
        assert is_transient_error(err) is True

    def test_connection_in_message_is_transient(self):
        """Error message containing 'connection' should be transient."""
        err = Exception("Lost connection to database")
        assert is_transient_error(err) is True

    def test_deadlock_is_transient(self):
        """Deadlock errors should be transient."""
        err = Exception("Deadlock detected")
        assert is_transient_error(err) is True

    def test_normal_value_error_is_not_transient(self):
        """Normal ValueError should not be transient."""
        err = ValueError("Invalid input")
        assert is_transient_error(err) is False

    def test_key_error_is_not_transient(self):
        """Verify KeyError should not be transient."""
        err = KeyError("missing_key")
        assert is_transient_error(err) is False


class TestRetryWithBackoff:
    """Tests for retry_with_backoff function."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self):
        """Operation succeeds on first attempt - no retry needed."""
        mock_op = AsyncMock(return_value="success")
        result = await retry_with_backoff(mock_op, "test_op")
        assert result == "success"
        assert mock_op.call_count == 1

    @pytest.mark.asyncio
    async def test_retry_on_transient_error_then_success(self):
        """Transient error followed by success triggers retry."""
        call_count = 0

        async def flaky_op():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection refused")
            return "success"

        with patch("agent.utils.retry.asyncio.sleep", new=AsyncMock()):
            result = await retry_with_backoff(flaky_op, "test_op")

        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_no_retry_on_non_transient_error(self):
        """Non-transient error should not trigger retry."""
        mock_op = AsyncMock(side_effect=ValueError("Invalid input"))

        with pytest.raises(ValueError):
            await retry_with_backoff(mock_op, "test_op")

        assert mock_op.call_count == 1

    @pytest.mark.asyncio
    async def test_max_attempts_exhausted(self):
        """All attempts exhausted should raise last exception."""
        mock_op = AsyncMock(side_effect=ConnectionError("Connection refused"))

        with patch("agent.utils.retry.asyncio.sleep", new=AsyncMock()):
            with pytest.raises(ConnectionError):
                await retry_with_backoff(mock_op, "test_op", max_attempts=3)

        assert mock_op.call_count == 3

    @pytest.mark.asyncio
    async def test_logs_each_retry_attempt(self):
        """Each retry attempt should be logged."""
        call_count = 0

        async def flaky_op():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Connection refused")
            return "success"

        with (
            patch("agent.utils.retry.asyncio.sleep", new=AsyncMock()),
            patch("agent.utils.retry.logger") as mock_logger,
        ):
            await retry_with_backoff(flaky_op, "test_op")

        # Should have logged warning for retry
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args
        assert "Transient error" in call_args[0][0]
        assert call_args[1]["extra"]["attempt"] == 1

    @pytest.mark.asyncio
    async def test_logs_final_failure(self):
        """Final failure after all attempts should log error."""
        mock_op = AsyncMock(side_effect=ConnectionError("Connection refused"))

        with (
            patch("agent.utils.retry.asyncio.sleep", new=AsyncMock()),
            patch("agent.utils.retry.logger") as mock_logger,
        ):
            with pytest.raises(ConnectionError):
                await retry_with_backoff(mock_op, "test_op", max_attempts=2)

        # Should have logged error for exhausted attempts
        mock_logger.error.assert_called()
        call_args = mock_logger.error.call_args
        assert "exhausted" in call_args[0][0]

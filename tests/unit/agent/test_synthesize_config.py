"""Tests for synthesize temperature configuration."""

from agent.config import get_synthesize_temperature


def test_synthesize_temperature_default(monkeypatch):
    """Default temperature remains unchanged when env is unset."""
    monkeypatch.delenv("AGENT_SYNTHESIZE_TEMPERATURE", raising=False)
    monkeypatch.delenv("AGENT_SYNTHESIZE_MODE", raising=False)
    assert get_synthesize_temperature() == 0.7


def test_synthesize_temperature_deterministic_mode(monkeypatch):
    """Deterministic mode forces temperature to zero."""
    monkeypatch.setenv("AGENT_SYNTHESIZE_MODE", "deterministic")
    monkeypatch.setenv("AGENT_SYNTHESIZE_TEMPERATURE", "0.9")
    assert get_synthesize_temperature() == 0.0


def test_synthesize_temperature_zero(monkeypatch):
    """Explicit zero temperature is respected."""
    monkeypatch.delenv("AGENT_SYNTHESIZE_MODE", raising=False)
    monkeypatch.setenv("AGENT_SYNTHESIZE_TEMPERATURE", "0")
    assert get_synthesize_temperature() == 0.0


def test_synthesize_temperature_invalid_env_logs_warning(monkeypatch):
    """Invalid env value falls back to default with a warning."""
    monkeypatch.delenv("AGENT_SYNTHESIZE_MODE", raising=False)
    monkeypatch.setenv("AGENT_SYNTHESIZE_TEMPERATURE", "bad")

    from agent import config as config_mod

    called = {}

    def _warn(msg, *args, **kwargs):
        called["warned"] = True
        return msg

    monkeypatch.setattr(config_mod.logger, "warning", _warn)
    assert get_synthesize_temperature() == 0.7
    assert called.get("warned") is True

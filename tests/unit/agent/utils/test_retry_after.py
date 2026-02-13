from agent.utils.retry_after import compute_retry_delay


def test_compute_retry_delay_basics():
    """Verify basic boundary conditions (0 or negative input)."""
    assert compute_retry_delay(0) == 0.0
    assert compute_retry_delay(-1) == 0.0


def test_compute_retry_delay_jitter():
    """Verify jitter is applied within expected range."""
    base = 10.0
    jitter = 0.2
    for _ in range(100):
        val = compute_retry_delay(base, jitter_ratio=jitter)
        # Jitter is added to base, so range is base +/- base*jitter?
        # Implementation: jitter_range = base * jitter_ratio; return base + jitter
        # So it's [base - range, base + range]
        assert base * (1 - jitter) <= val <= base * (1 + jitter)


def test_compute_retry_delay_cap():
    """Verify max delay cap is applied before jitter."""
    # max_delay is applied to base. Then jitter is applied.
    # jitter is calculated on the capped base.
    # So capped base = 10. jitter range = 2. max result = 12.
    val = compute_retry_delay(100, max_delay=10.0, jitter_ratio=0.2)
    assert val <= 12.001
    assert val >= 8.0

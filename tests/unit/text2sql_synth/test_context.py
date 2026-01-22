"""Tests for GenerationContext."""

from datetime import date

import numpy as np
import pandas as pd

from text2sql_synth.context import GenerationContext, TimeWindowConfig


def make_context(
    seed: int = 42,
    schema: str = "CREATE TABLE users (id INT);",
    start: date = date(2024, 1, 1),
    end: date = date(2024, 1, 31),
) -> GenerationContext:
    """Create a GenerationContext with defaults."""
    time_window = TimeWindowConfig(start_date=start, end_date=end)
    return GenerationContext(seed=seed, schema_definition=schema, time_window=time_window)


class TestTimeWindowConfig:
    """Tests for TimeWindowConfig."""

    def test_date_range_inclusive(self) -> None:
        """Date range includes both start and end dates."""
        config = TimeWindowConfig(start_date=date(2024, 1, 1), end_date=date(2024, 1, 3))
        dates = config.date_range()
        assert len(dates) == 3
        assert dates[0] == date(2024, 1, 1)
        assert dates[-1] == date(2024, 1, 3)

    def test_single_day_range(self) -> None:
        """Single day range returns one date."""
        config = TimeWindowConfig(start_date=date(2024, 1, 1), end_date=date(2024, 1, 1))
        dates = config.date_range()
        assert len(dates) == 1
        assert dates[0] == date(2024, 1, 1)


class TestGenerationContextDeterminism:
    """Tests verifying deterministic behavior of GenerationContext."""

    def test_same_seed_same_schema_snapshot_id(self) -> None:
        """Same seed + schema produces identical schema_snapshot_id."""
        ctx1 = make_context(seed=42, schema="CREATE TABLE users (id INT);")
        ctx2 = make_context(seed=42, schema="CREATE TABLE users (id INT);")
        assert ctx1.schema_snapshot_id == ctx2.schema_snapshot_id

    def test_different_schema_different_snapshot_id(self) -> None:
        """Different schema produces different schema_snapshot_id."""
        ctx1 = make_context(schema="CREATE TABLE users (id INT);")
        ctx2 = make_context(schema="CREATE TABLE orders (id INT);")
        assert ctx1.schema_snapshot_id != ctx2.schema_snapshot_id

    def test_same_seed_same_stable_ids(self) -> None:
        """Same seed produces identical sequence of stable IDs."""
        ctx1 = make_context(seed=42)
        ctx2 = make_context(seed=42)

        ids1 = [ctx1.stable_id("cust") for _ in range(5)]
        ids2 = [ctx2.stable_id("cust") for _ in range(5)]

        assert ids1 == ids2
        assert ids1 == ["cust_000001", "cust_000002", "cust_000003", "cust_000004", "cust_000005"]

    def test_same_seed_same_rng_sequence(self) -> None:
        """Same seed produces identical RNG sequences per table."""
        ctx1 = make_context(seed=42)
        ctx2 = make_context(seed=42)

        rng1 = ctx1.rng_for("users")
        rng2 = ctx2.rng_for("users")

        # Generate same sequence from both
        vals1 = [rng1.integers(0, 100) for _ in range(10)]
        vals2 = [rng2.integers(0, 100) for _ in range(10)]

        assert vals1 == vals2

    def test_different_tables_different_rng_sequences(self) -> None:
        """Different table names produce different RNG sequences."""
        ctx = make_context(seed=42)

        rng_users = ctx.rng_for("users")
        rng_orders = ctx.rng_for("orders")

        vals_users = [rng_users.integers(0, 1000) for _ in range(5)]
        vals_orders = [rng_orders.integers(0, 1000) for _ in range(5)]

        assert vals_users != vals_orders

    def test_different_seeds_different_rng_sequences(self) -> None:
        """Different seeds produce different RNG sequences."""
        ctx1 = make_context(seed=42)
        ctx2 = make_context(seed=123)

        rng1 = ctx1.rng_for("users")
        rng2 = ctx2.rng_for("users")

        vals1 = [rng1.integers(0, 1000) for _ in range(5)]
        vals2 = [rng2.integers(0, 1000) for _ in range(5)]

        assert vals1 != vals2


class TestStableIdGeneration:
    """Tests for stable ID generation."""

    def test_stable_id_format(self) -> None:
        """Stable IDs have correct format."""
        ctx = make_context()
        id1 = ctx.stable_id("order")
        assert id1 == "order_000001"

    def test_stable_id_increments(self) -> None:
        """Stable IDs increment correctly."""
        ctx = make_context()
        ids = [ctx.stable_id("cust") for _ in range(3)]
        assert ids == ["cust_000001", "cust_000002", "cust_000003"]

    def test_stable_id_batch(self) -> None:
        """Batch ID generation works correctly."""
        ctx = make_context()
        ids = ctx.stable_id("item", n=3)
        assert ids == ["item_000001", "item_000002", "item_000003"]

    def test_stable_int_id(self) -> None:
        """Integer IDs are generated correctly."""
        ctx = make_context()
        ids = ctx.stable_int_id("user", n=3)
        assert ids == [1, 2, 3]

    def test_prefixes_independent(self) -> None:
        """Different prefixes have independent counters."""
        ctx = make_context()
        ctx.stable_id("cust")  # cust_000001
        ctx.stable_id("cust")  # cust_000002
        order_id = ctx.stable_id("order")  # should be order_000001, not order_000003
        assert order_id == "order_000001"


class TestSamplingMethods:
    """Tests for sampling helper methods."""

    def test_sample_categorical_deterministic(self) -> None:
        """Categorical sampling is deterministic."""
        ctx1 = make_context(seed=42)
        ctx2 = make_context(seed=42)

        rng1 = ctx1.rng_for("test")
        rng2 = ctx2.rng_for("test")

        categories = ["A", "B", "C"]
        samples1 = ctx1.sample_categorical(rng1, categories, size=10)
        samples2 = ctx2.sample_categorical(rng2, categories, size=10)

        assert samples1 == samples2

    def test_sample_categorical_with_weights(self) -> None:
        """Weighted categorical sampling respects weights."""
        ctx = make_context(seed=42)
        rng = ctx.rng_for("test")

        # Heavy weight on "A" should produce mostly "A"
        categories = ["A", "B"]
        weights = [0.99, 0.01]
        samples = ctx.sample_categorical(rng, categories, weights=weights, size=100)

        a_count = sum(1 for s in samples if s == "A")
        assert a_count > 90  # Should be close to 99

    def test_sample_zipf_deterministic(self) -> None:
        """Zipf sampling is deterministic."""
        ctx1 = make_context(seed=42)
        ctx2 = make_context(seed=42)

        rng1 = ctx1.rng_for("test")
        rng2 = ctx2.rng_for("test")

        samples1 = ctx1.sample_zipf(rng1, a=2.0, size=10)
        samples2 = ctx2.sample_zipf(rng2, a=2.0, size=10)

        np.testing.assert_array_equal(samples1, samples2)

    def test_sample_pareto_deterministic(self) -> None:
        """Pareto sampling is deterministic."""
        ctx1 = make_context(seed=42)
        ctx2 = make_context(seed=42)

        rng1 = ctx1.rng_for("test")
        rng2 = ctx2.rng_for("test")

        samples1 = ctx1.sample_pareto(rng1, a=2.0, size=10)
        samples2 = ctx2.sample_pareto(rng2, a=2.0, size=10)

        np.testing.assert_array_equal(samples1, samples2)


class TestTableRegistry:
    """Tests for table registration."""

    def test_register_and_get_table(self) -> None:
        """Tables can be registered and retrieved."""
        ctx = make_context()
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        ctx.register_table("users", df)
        retrieved = ctx.get_table("users")

        assert retrieved is not None
        pd.testing.assert_frame_equal(df, retrieved)

    def test_get_nonexistent_table(self) -> None:
        """Getting nonexistent table returns None."""
        ctx = make_context()
        assert ctx.get_table("nonexistent") is None


class TestResetFunctionality:
    """Tests for reset methods."""

    def test_reset_all_restores_state(self) -> None:
        """reset_all restores context to initial state."""
        ctx = make_context(seed=42)

        # Generate some state
        ctx.stable_id("cust")
        ctx.rng_for("users").integers(0, 100)
        ctx.register_table("test", pd.DataFrame({"a": [1]}))

        # Reset
        ctx.reset_all()

        # Should start fresh
        assert ctx.stable_id("cust") == "cust_000001"
        assert ctx.tables == {}

    def test_reset_id_counter(self) -> None:
        """Reset specific ID counter."""
        ctx = make_context()
        ctx.stable_id("cust")  # cust_000001
        ctx.stable_id("cust")  # cust_000002

        ctx.reset_id_counter("cust")

        assert ctx.stable_id("cust") == "cust_000001"


class TestRowHash:
    """Tests for row hashing."""

    def test_row_hash_deterministic(self) -> None:
        """Row hashing is deterministic."""
        ctx = make_context()
        row = {"id": 1, "name": "test"}

        hash1 = ctx.row_hash(row)
        hash2 = ctx.row_hash(row)

        assert hash1 == hash2

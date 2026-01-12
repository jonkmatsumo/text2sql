"""Generation context for managing seeds, state, and deterministic data generation."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from numpy.random import Generator, PCG64
from pydantic import BaseModel, Field

from text2sql_synth import __version__
from text2sql_synth.util.hashing import seed_from_str, stable_hash_str, stable_row_hash


class TimeWindowConfig(BaseModel):
    """Configuration for the generation time window."""

    start_date: date
    end_date: date

    def date_range(self) -> list[date]:
        """Generate list of all dates in the window (inclusive)."""
        days = (self.end_date - self.start_date).days + 1
        return [self.start_date + timedelta(days=i) for i in range(days)]


class GenerationContext:
    """Context for deterministic synthetic data generation.

    Manages seeded RNG, schema versioning, time windows, and generated table registry.
    All randomness flows through this context to ensure reproducibility.

    Attributes:
        seed: Base seed for all random number generation.
        schema_definition: String representation of the schema being generated.
        time_window: Configuration for date range of generated data.
        schema_snapshot_id: Stable hash identifying schema + package version.
    """

    def __init__(
        self,
        seed: int,
        schema_definition: str,
        time_window: TimeWindowConfig,
    ) -> None:
        """Initialize generation context.

        Args:
            seed: Base seed for reproducible generation.
            schema_definition: String representation of the target schema.
            time_window: Date range configuration for temporal data.
        """
        self._seed = seed
        self._schema_definition = schema_definition
        self._time_window = time_window

        # Compute stable schema snapshot ID
        snapshot_input = f"{schema_definition}|{__version__}"
        self._schema_snapshot_id = stable_hash_str(snapshot_input)

        # Initialize base RNG with PCG64 for high-quality randomness
        self._base_rng = Generator(PCG64(seed))

        # Registry of generated tables (table_name -> DataFrame)
        self._tables: dict[str, pd.DataFrame] = {}

        # Cache of table-specific RNGs
        self._table_rngs: dict[str, Generator] = {}

        # Counter for stable ID generation per prefix
        self._id_counters: dict[str, int] = {}

    @property
    def seed(self) -> int:
        """Base seed for generation."""
        return self._seed

    @property
    def schema_definition(self) -> str:
        """Schema definition string."""
        return self._schema_definition

    @property
    def time_window(self) -> TimeWindowConfig:
        """Time window configuration."""
        return self._time_window

    @property
    def schema_snapshot_id(self) -> str:
        """Stable hash of schema + package version."""
        return self._schema_snapshot_id

    @property
    def dates(self) -> list[date]:
        """List of all dates in the time window."""
        return self._time_window.date_range()

    @property
    def tables(self) -> dict[str, pd.DataFrame]:
        """Registry of generated tables."""
        return self._tables

    def rng_for(self, table_name: str) -> Generator:
        """Get a deterministic sub-RNG for a specific table.

        Creates a new RNG seeded by mixing the base seed with a hash
        of the table name. Cached for consistent access within generation.

        Args:
            table_name: Name of the table to get RNG for.

        Returns:
            Numpy Generator with deterministic seed for this table.
        """
        if table_name not in self._table_rngs:
            # Mix base seed with table name hash for unique but deterministic seed
            table_seed = (self._seed + seed_from_str(table_name)) % (2**32)
            self._table_rngs[table_name] = Generator(PCG64(table_seed))
        return self._table_rngs[table_name]

    def stable_id(self, prefix: str, n: int = 1) -> str | list[str]:
        """Generate deterministic string IDs with a prefix.

        IDs are formatted as "{prefix}_{counter:06d}" and are guaranteed
        to be unique and reproducible given the same sequence of calls.

        Args:
            prefix: Prefix for the ID (e.g., "cust", "order").
            n: Number of IDs to generate. Defaults to 1.

        Returns:
            Single ID string if n=1, otherwise list of ID strings.
        """
        if prefix not in self._id_counters:
            self._id_counters[prefix] = 0

        ids = []
        for _ in range(n):
            self._id_counters[prefix] += 1
            ids.append(f"{prefix}_{self._id_counters[prefix]:06d}")

        return ids[0] if n == 1 else ids

    def stable_int_id(self, prefix: str, n: int = 1) -> int | list[int]:
        """Generate deterministic integer IDs.

        IDs are sequential integers starting from 1, unique per prefix.

        Args:
            prefix: Prefix/namespace for the ID sequence.
            n: Number of IDs to generate. Defaults to 1.

        Returns:
            Single int if n=1, otherwise list of ints.
        """
        if prefix not in self._id_counters:
            self._id_counters[prefix] = 0

        ids = []
        for _ in range(n):
            self._id_counters[prefix] += 1
            ids.append(self._id_counters[prefix])

        return ids[0] if n == 1 else ids

    def sample_categorical(
        self,
        rng: Generator,
        categories: list[Any],
        weights: list[float] | None = None,
        size: int = 1,
    ) -> Any | list[Any]:
        """Sample from categorical distribution with optional weights.

        Args:
            rng: Numpy Generator to use for sampling.
            categories: List of category values to sample from.
            weights: Optional probability weights (will be normalized).
            size: Number of samples to draw.

        Returns:
            Single value if size=1, otherwise list of values.
        """
        if weights is not None:
            # Normalize weights to probabilities
            weights_arr = np.array(weights, dtype=np.float64)
            probs = weights_arr / weights_arr.sum()
        else:
            probs = None

        indices = rng.choice(len(categories), size=size, p=probs)

        if size == 1:
            return categories[indices[0]]
        return [categories[i] for i in indices]

    def sample_zipf(
        self,
        rng: Generator,
        a: float,
        size: int = 1,
        min_val: int = 1,
        max_val: int | None = None,
    ) -> int | np.ndarray:
        """Sample from Zipf (power-law) distribution.

        Useful for generating long-tail distributions like popularity,
        frequency counts, etc.

        Args:
            rng: Numpy Generator to use for sampling.
            a: Shape parameter (must be > 1). Higher values = steeper distribution.
            size: Number of samples to draw.
            min_val: Minimum value (default 1).
            max_val: Optional maximum value (samples are clipped).

        Returns:
            Single int if size=1, otherwise numpy array of ints.
        """
        samples = rng.zipf(a, size=size)
        samples = samples + (min_val - 1)  # Shift minimum

        if max_val is not None:
            samples = np.clip(samples, min_val, max_val)

        return int(samples[0]) if size == 1 else samples

    def sample_pareto(
        self,
        rng: Generator,
        a: float,
        size: int = 1,
        scale: float = 1.0,
    ) -> float | np.ndarray:
        """Sample from Pareto distribution.

        Useful for generating long-tail continuous distributions like
        transaction amounts, durations, etc.

        Args:
            rng: Numpy Generator to use for sampling.
            a: Shape parameter (must be > 0). Higher values = lighter tail.
            size: Number of samples to draw.
            scale: Scale parameter (minimum value).

        Returns:
            Single float if size=1, otherwise numpy array of floats.
        """
        # Numpy's pareto returns values >= 0, we scale to get values >= scale
        samples = (rng.pareto(a, size=size) + 1) * scale

        return float(samples[0]) if size == 1 else samples

    def row_hash(self, row_data: dict) -> str:
        """Compute stable hash for a row of data.

        Args:
            row_data: Dictionary of column name -> value.

        Returns:
            Hex string hash of the row.
        """
        return stable_row_hash(row_data)

    def register_table(self, name: str, df: pd.DataFrame) -> None:
        """Register a generated table in the context.

        Args:
            name: Table name.
            df: Generated DataFrame.
        """
        self._tables[name] = df

    def get_table(self, name: str) -> pd.DataFrame | None:
        """Get a registered table by name.

        Args:
            name: Table name to retrieve.

        Returns:
            DataFrame if found, None otherwise.
        """
        return self._tables.get(name)

    def reset_id_counter(self, prefix: str) -> None:
        """Reset the ID counter for a specific prefix.

        Args:
            prefix: The prefix to reset.
        """
        if prefix in self._id_counters:
            del self._id_counters[prefix]

    def reset_all(self) -> None:
        """Reset all state for fresh generation.

        Resets RNG, tables, and ID counters to initial state.
        """
        self._base_rng = Generator(PCG64(self._seed))
        self._tables.clear()
        self._table_rngs.clear()
        self._id_counters.clear()

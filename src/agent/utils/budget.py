"""Token budget utility for request-level cost control."""

from typing import Optional


class TokenBudget:
    """Tracks and enforces a token budget for a single request."""

    def __init__(self, max_tokens: int, consumed_tokens: int = 0):
        """Initialize the budget with a limit and optional initial consumption."""
        self.max_tokens = max_tokens
        self.consumed_tokens = consumed_tokens

    def consume(self, n: int) -> None:
        """Record consumption of n tokens."""
        self.consumed_tokens += n

    def remaining(self) -> int:
        """Return the number of tokens left in the budget."""
        return max(0, self.max_tokens - self.consumed_tokens)

    def is_exhausted(self) -> bool:
        """Check if the budget has been exceeded."""
        return self.consumed_tokens >= self.max_tokens

    def to_dict(self) -> dict:
        """Convert to dictionary for state persistence."""
        return {
            "max_tokens": self.max_tokens,
            "consumed_tokens": self.consumed_tokens,
        }

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> Optional["TokenBudget"]:
        """Restore from state dictionary."""
        if not data:
            return None
        return cls(
            max_tokens=data.get("max_tokens", 0),
            consumed_tokens=data.get("consumed_tokens", 0),
        )

"""Utilities for managing bounded agent context."""

from typing import List, Union


def append_bounded(
    context: Union[List[str], str],
    addition: str,
    max_items: int = 5,
    max_chars: int = 2000,
) -> Union[List[str], str]:
    """Append to a list or string context while keeping it within bounds.

    If context is a list:
    - Appends 'addition' to the end.
    - Truncates from the START if max_items is exceeded.
    - Truncates from the START if cumulative character count exceeds max_chars.

    If context is a string:
    - Appends 'addition' with a newline.
    - Truncates from the START if total chars exceed max_chars.

    Args:
        context: The current context (list of strings or a single string).
        addition: The new string to add.
        max_items: Maximum number of items (only applies if context is a list).
        max_chars: Maximum cumulative character count.

    Returns:
        The updated and bounded context (same type as input).
    """
    if isinstance(context, list):
        # 1. Append new item
        new_context = list(context) + [addition]

        # 2. Bound by item count (FIFO)
        if len(new_context) > max_items:
            new_context = new_context[-max_items:]

        # 3. Bound by character count (FIFO)
        while new_context and sum(len(x) for x in new_context) > max_chars:
            if len(new_context) == 1:
                # If a single item is too large, truncate the item itself
                new_context[0] = new_context[0][-max_chars:]
                break
            new_context.pop(0)

        return new_context

    elif isinstance(context, str):
        # 1. Append with separator
        separator = "\n" if context else ""
        new_str = context + separator + addition

        # 2. Bound by character count
        if len(new_str) > max_chars:
            new_str = new_str[-max_chars:]

        return new_str

    return context

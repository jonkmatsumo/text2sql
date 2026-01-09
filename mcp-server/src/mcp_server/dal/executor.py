"""Context-aware executor for safe async context propagation.

This module provides a thread pool executor that captures the current context
(including contextvars like tenant_id) at submission time and propagates it
to the worker thread. This is critical for offloading synchronous DB operations
(like Neo4j driver calls) without losing the tenant context.
"""

import asyncio
import contextvars
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, TypeVar

from typing_extensions import ParamSpec

P = ParamSpec("P")
T = TypeVar("T")


class ContextAwareExecutor(ThreadPoolExecutor):
    """Thread pool executor that propagates contextvars to worker threads."""

    def submit(self, fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> asyncio.Future:
        """Submit a function to the executor, capturing the current context."""
        context = contextvars.copy_context()

        # Wrap the function to run within the captured context
        def _wrapper(*w_args, **w_kwargs):
            return context.run(fn, *w_args, **w_kwargs)

        return super().submit(_wrapper, *args, **kwargs)


async def run_in_executor_with_context(
    executor: ThreadPoolExecutor, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> T:
    """Run a function in an executor with the current context preserved.

    If executor is None, the default loop executor is used.
    Note: If using the default loop executor, ensure it is context-aware or use
    this helper which manually handles context propagation if the executor doesn't.

    Actually, standard asyncio.get_running_loop().run_in_executor() takes
    (executor, func, *args). It doesn't take kwargs easily without functools.partial.

    And simply running it won't propagate context unless the executor does it
    or we wrap the function here.
    """
    loop = asyncio.get_running_loop()
    context = contextvars.copy_context()

    # Wrap function to run in captured context
    func_with_context = functools.partial(context.run, func, *args, **kwargs)

    return await loop.run_in_executor(executor, func_with_context)

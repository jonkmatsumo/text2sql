from contextvars import ContextVar
from typing import Optional

run_id_var: ContextVar[Optional[str]] = ContextVar("run_id", default=None)
request_id_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)

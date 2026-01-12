"""Utility for manual token tracking in MLflow spans."""


def log_token_usage(span, input_tokens: int, output_tokens: int, model: str = None):
    """
    Log token usage to telemetry span.

    Args:
        span: TelemetrySpan object
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens
        model: Model name (optional)
    """
    if span:
        span.set_attribute("llm.token_usage.input_tokens", input_tokens)
        span.set_attribute("llm.token_usage.output_tokens", output_tokens)
        span.set_attribute("llm.token_usage.total_tokens", input_tokens + output_tokens)
        if model:
            span.set_attribute("llm.model", model)

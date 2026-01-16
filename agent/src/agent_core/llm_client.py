"""LLM Client factory for multi-provider support.

Supports OpenAI, Anthropic (Claude), and Google (Gemini) with runtime model selection.
"""

from typing import Any, Optional

from dotenv import load_dotenv
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.runnables import RunnableLambda

from common.config.env import get_env_str

load_dotenv()


def _sanitize_llm_input(input_data: Any) -> Any:
    """Recursively sanitize all string content in LLM inputs."""
    from common.sanitization import sanitize_text

    if isinstance(input_data, str):
        # We allow longer inputs for LLM prompts (e.g. DDLs)
        # The default max is 64, which is too small for prompts.
        # However, the user said "exactly once per request/invocation".
        # And "common.sanitization" has defaults.
        # If I use defaults, I might truncate the whole prompt!

        # Wait, if I'm sanitizing the WHOLE prompt, I need a much larger max_len.
        res = sanitize_text(input_data, max_len=100000)
        return res.sanitized or ""

    if isinstance(input_data, list):
        return [_sanitize_llm_input(x) for x in input_data]

    if isinstance(input_data, dict):
        return {k: _sanitize_llm_input(v) for k, v in input_data.items()}

    if hasattr(input_data, "content"):
        # It's a LangChain message. We want to sanitize its content.
        # We should not mutate it if we want to be safe, but creating a new one is better.
        # However, we don't necessarily know the constructor args.
        # Most LangChain messages allow content as first arg.
        content = _sanitize_llm_input(input_data.content)
        # Using copy and update if possible, or just mutation if we are certain it's fresh.
        # In a Runnable chain, messages are usually freshly created by the prompt.
        input_data.content = content
        return input_data

    # Handle PromptValue (has to_messages() and to_string())
    if hasattr(input_data, "to_messages"):
        messages = input_data.to_messages()
        sanitized_messages = [_sanitize_llm_input(m) for m in messages]
        # Return as list of messages, which LangChain LLMs accept.
        return sanitized_messages

    return input_data


# Default configuration
DEFAULT_PROVIDER = "openai"
DEFAULT_MODEL = "gpt-5.2"

# Supported models by provider
SUPPORTED_MODELS = {
    "openai": ["gpt-5.2", "gpt-5.1", "gpt-5", "gpt-4o", "gpt-4o-mini"],
    "anthropic": ["claude-sonnet-4-20250514", "claude-3-5-sonnet-20241022"],
    "google": ["gemini-2.5-flash-preview-05-20", "gemini-2.5-pro-preview-05-06"],
}


def get_llm_client(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    temperature: float = 0,
    use_light_model: bool = False,
) -> BaseChatModel:
    """Get an LLM client for the specified provider.

    Args:
        provider: LLM provider ('openai', 'anthropic', 'google').
                  Defaults to LLM_PROVIDER env var or 'openai'.
        model: Model name. Defaults to LLM_MODEL env var or provider default.
        temperature: Temperature for generation. Defaults to 0 (deterministic).
        use_light_model: If True, uses LLM_MODEL_LIGHT env var if available.

    Returns:
        BaseChatModel: LangChain chat model instance.

    Raises:
        ValueError: If provider is not supported.
    """
    # Resolve provider from env or default
    resolved_provider = provider or get_env_str("LLM_PROVIDER", DEFAULT_PROVIDER)
    resolved_provider = resolved_provider.lower()

    # Validate provider
    if resolved_provider not in SUPPORTED_MODELS:
        raise ValueError(
            f"Unsupported provider: {resolved_provider}. "
            f"Supported: {list(SUPPORTED_MODELS.keys())}"
        )

    # Resolve model from env or default
    if model:
        resolved_model = model
    elif use_light_model:
        # Default for light model is gpt-4o-mini if not set
        resolved_model = get_env_str("LLM_MODEL_LIGHT", "gpt-4o-mini")
    else:
        resolved_model = get_env_str("LLM_MODEL", DEFAULT_MODEL)

    # Create client based on provider
    if resolved_provider == "openai":
        from langchain_openai import ChatOpenAI

        # Validate API key before instantiation to fail fast
        key = get_env_str("OPENAI_API_KEY")
        placeholders = {"<REPLACE_ME>", "changeme", "your_api_key_here"}
        if not key or key.strip() in placeholders or key.startswith("<"):
            raise ValueError(
                "OPENAI_API_KEY is missing or set to a placeholder value (<REPLACE_ME>). "
                "Please update your .env file with a valid OpenAI API key."
            )

        return ChatOpenAI(model=resolved_model, temperature=temperature)

    elif resolved_provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=resolved_model, temperature=temperature)

    elif resolved_provider == "google":
        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(model=resolved_model, temperature=temperature)

    # Should not reach here due to validation above
    raise ValueError(f"Unsupported provider: {resolved_provider}")


def get_available_models(provider: str) -> list[str]:
    """
    Get list of available models for a provider.

    Args:
        provider: LLM provider name.

    Returns:
        List of model names, or empty list if provider not found.
    """
    return SUPPORTED_MODELS.get(provider.lower(), [])


def get_available_providers() -> list[str]:
    """
    Get list of available providers.

    Returns:
        List of provider names.
    """
    return list(SUPPORTED_MODELS.keys())


# Private cache for lazy-loaded LLM clients
# keys: tuple(provider, model, temperature, use_light_model)
_LLM_CACHE = {}


def get_llm(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    temperature: float = 0,
    use_light_model: bool = False,
) -> BaseChatModel:
    """
    Lazy accessor for LLM clients.

    Uses a simple cache to avoid re-instantiating clients with the same configuration.
    This is preferred over calling get_llm_client() directly in module-level code,
    as it delays validation and API key checks until runtime.

    Args:
        provider: LLM provider
        model: Model name
        temperature: Temperature
        use_light_model: Use light model flag

    Returns:
        BaseChatModel: The configured chat model
    """
    key = (provider, model, temperature, use_light_model)

    if key not in _LLM_CACHE:
        llm = get_llm_client(
            provider=provider,
            model=model,
            temperature=temperature,
            use_light_model=use_light_model,
        )
        # Apply strict telemetry parity wrapper
        telemetric_llm = _wrap_llm(llm)

        # Apply sanitization wrapper exactly once at invocation boundary
        _LLM_CACHE[key] = RunnableLambda(_sanitize_llm_input) | telemetric_llm

    return _LLM_CACHE[key]


def _extract_prompts(input_data: Any) -> tuple[Optional[str], Optional[str]]:
    """Extract system and user prompts from various input types."""
    system_prompt = None
    user_prompt = None

    messages = []
    if hasattr(input_data, "to_messages"):
        messages = input_data.to_messages()
    elif isinstance(input_data, list):
        messages = input_data
    elif isinstance(input_data, str):
        user_prompt = input_data
        return None, user_prompt

    for msg in messages:
        if msg.type == "system":
            system_prompt = msg.content
        elif msg.type == "human":
            user_prompt = msg.content

    return system_prompt, user_prompt


def _wrap_llm(llm: BaseChatModel):
    """Wrap an LLM with strict telemetry parity."""
    from agent_core.telemetry import telemetry
    from agent_core.telemetry_schema import SpanKind, TelemetryKeys, truncate_json

    # Attempt to resolve model name
    model_name = getattr(llm, "model_name", getattr(llm, "model", "unknown"))

    def telemetric_invoke(input, config=None, **kwargs):
        sys_prompt, user_prompt = _extract_prompts(input)

        with telemetry.start_span(name="llm.call", span_type=SpanKind.LLM_CALL) as span:
            span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.LLM_CALL)
            span.set_attribute(TelemetryKeys.EVENT_NAME, "llm_generation")
            span.set_attribute(TelemetryKeys.LLM_MODEL, model_name)

            if sys_prompt:
                sys_trunc, _, _, _ = truncate_json(sys_prompt)
                span.set_attribute(TelemetryKeys.LLM_PROMPT_SYSTEM, sys_trunc)
            if user_prompt:
                usr_trunc, _, _, _ = truncate_json(user_prompt)
                span.set_attribute(TelemetryKeys.LLM_PROMPT_USER, usr_trunc)

            try:
                response = llm.invoke(input, config, **kwargs)

                # Capture Output
                if hasattr(response, "content"):
                    out_trunc, truncated, size, _ = truncate_json(response.content)
                    span.set_attribute(TelemetryKeys.LLM_RESPONSE_TEXT, out_trunc)
                    if truncated:
                        span.set_attribute(TelemetryKeys.PAYLOAD_TRUNCATED, True)

                # Capture Tokens
                usage = extract_token_usage(response)
                if usage:
                    span.set_attributes(usage)

                return response
            except Exception as e:
                error_info = {"error": str(e), "type": type(e).__name__}
                span.set_attribute(TelemetryKeys.ERROR, truncate_json(error_info)[0])
                raise e

    async def telemetric_ainvoke(input, config=None, **kwargs):
        sys_prompt, user_prompt = _extract_prompts(input)

        with telemetry.start_span(name="llm.call", span_type=SpanKind.LLM_CALL) as span:
            span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.LLM_CALL)
            span.set_attribute(TelemetryKeys.EVENT_NAME, "llm_generation")
            span.set_attribute(TelemetryKeys.LLM_MODEL, model_name)

            if sys_prompt:
                sys_trunc, _, _, _ = truncate_json(sys_prompt)
                span.set_attribute(TelemetryKeys.LLM_PROMPT_SYSTEM, sys_trunc)
            if user_prompt:
                usr_trunc, _, _, _ = truncate_json(user_prompt)
                span.set_attribute(TelemetryKeys.LLM_PROMPT_USER, usr_trunc)

            try:
                response = await llm.ainvoke(input, config, **kwargs)

                # Capture Output
                if hasattr(response, "content"):
                    out_trunc, truncated, size, _ = truncate_json(response.content)
                    span.set_attribute(TelemetryKeys.LLM_RESPONSE_TEXT, out_trunc)
                    if truncated:
                        span.set_attribute(TelemetryKeys.PAYLOAD_TRUNCATED, True)

                # Capture Tokens
                usage = extract_token_usage(response)
                if usage:
                    span.set_attributes(usage)

                return response
            except Exception as e:
                error_info = {"error": str(e), "type": type(e).__name__}
                span.set_attribute(TelemetryKeys.ERROR, truncate_json(error_info)[0])
                raise e

    return RunnableLambda(telemetric_invoke, telemetric_ainvoke)


def extract_token_usage(response: Any) -> dict[str, int]:
    """
    Extract token usage from LLM response metadata.

    Handles different provider formats (OpenAI, Anthropic, Google).
    Returns standard OTEL-compatible keys:
    - llm.token_usage.input_tokens
    - llm.token_usage.output_tokens
    - llm.token_usage.total_tokens

    Args:
        response: LangChain AIMessage response.

    Returns:
        Dict with usage metrics (integers).
    """
    usage = {}
    if not hasattr(response, "response_metadata"):
        return usage

    meta = response.response_metadata

    # Keys for input/output/total
    k_input = "llm.token_usage.input_tokens"
    k_output = "llm.token_usage.output_tokens"
    k_total = "llm.token_usage.total_tokens"

    # OpenAI format:
    # 'token_usage': {'completion_tokens': 15, 'prompt_tokens': 561, 'total_tokens': 576}
    if "token_usage" in meta:
        tu = meta["token_usage"]
        if "prompt_tokens" in tu:
            usage[k_input] = int(tu["prompt_tokens"])
        if "completion_tokens" in tu:
            usage[k_output] = int(tu["completion_tokens"])
        if "total_tokens" in tu:
            usage[k_total] = int(tu["total_tokens"])

    # Anthropic/Google format: 'usage': {'input_tokens': 20, 'output_tokens': 20, ...}
    elif "usage" in meta:
        u = meta["usage"]
        if hasattr(u, "input_tokens"):  # Some are objects
            usage[k_input] = int(u.input_tokens)
            usage[k_output] = int(u.output_tokens)
            # Total tokens might be inferred or present
            if hasattr(u, "total_tokens"):
                usage[k_total] = int(u.total_tokens)
            else:
                usage[k_total] = int(u.input_tokens) + int(u.output_tokens)
        elif isinstance(u, dict):
            # Input
            if "input_tokens" in u:
                usage[k_input] = int(u["input_tokens"])
            elif "prompt_token_count" in u:  # Google
                usage[k_input] = int(u["prompt_token_count"])

            # Output
            if "output_tokens" in u:
                usage[k_output] = int(u["output_tokens"])
            elif "candidates_token_count" in u:  # Google
                usage[k_output] = int(u["candidates_token_count"])

            # Total
            if "total_tokens" in u:
                usage[k_total] = int(u["total_tokens"])
            elif "total_token_count" in u:  # Google
                usage[k_total] = int(u["total_token_count"])

    return usage

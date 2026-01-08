import json
import logging

logger = logging.getLogger(__name__)


def parse_tool_output(tool_output):
    """
    Robustly parsing utility for MCP tool outputs.

    Handles tool outputs that may be wrapped in LangChain Message lists,
    stringified, or double-encoded.
    """
    aggregated_results = []

    # Ensure input is a list to unify processing logic
    inputs = tool_output if isinstance(tool_output, list) else [tool_output]

    for item in inputs:
        raw_payload = None

        # 1. Extract the raw string payload
        if isinstance(item, dict):
            raw_payload = item.get("text") or item.get("content")
            # If no message wrapper keys found, treat the dict itself as a data chunk
            if raw_payload is None:
                aggregated_results.append(item)
                continue
        elif hasattr(item, "text"):
            raw_payload = item.text
        elif hasattr(item, "content"):
            raw_payload = item.content
        elif isinstance(item, str):
            raw_payload = item

        if not raw_payload:
            continue

        # 2. Parse the JSON string
        try:
            parsed_chunk = json.loads(raw_payload)

            # 3. Handle Double-Encoding (common in MCP/LangChain bridges)
            if isinstance(parsed_chunk, str):
                try:
                    parsed_chunk = json.loads(parsed_chunk)
                except json.JSONDecodeError:
                    # If second parse fails, it might just be a string result (e.g. error message)
                    # But usually we expect structured data.
                    # If we want to capture simple strings, we could append parsed_chunk here.
                    # For now, let's allow strings if they are not json.
                    pass

            # 4. Aggregate Results
            if isinstance(parsed_chunk, list):
                aggregated_results.extend(parsed_chunk)
            elif isinstance(parsed_chunk, dict):
                aggregated_results.append(parsed_chunk)
            elif isinstance(parsed_chunk, str):
                # If the payload was a json string "foo", parsed_chunk is "foo".
                aggregated_results.append(parsed_chunk)
            else:
                aggregated_results.append(parsed_chunk)

        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse tool output chunk: {str(e)[:100]}...")
            continue

    return aggregated_results

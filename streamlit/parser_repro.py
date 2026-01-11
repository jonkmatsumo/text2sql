import json

from agent_core.utils.parsing import parse_tool_output

# Mocking the large JSON string from the user trace
# Note: I'm simplifying the inner content but keeping the structure and escaping
raw_text = json.dumps(
    {
        "nodes": [
            {
                "id": "72",
                "name": "film",
                "type": "Table",
                "sample_data": '[{"film_id": 1, "title": "ACADEMY DINOSAUR"}]',
            }
        ],
        "relationships": [],
    }
)

tool_output = [{"type": "text", "text": raw_text, "id": "lc_5524af26-5179-4db0-ba31-bb4e0fe7a9ed"}]

print(f"Input text length: {len(raw_text)}")

try:
    parsed = parse_tool_output(tool_output)
    print(f"Parsed Type: {type(parsed)}")
    print(f"Parsed Len: {len(parsed) if isinstance(parsed, list) else 'N/A'}")

    if isinstance(parsed, list) and len(parsed) > 0:
        graph_data = parsed[0]
        print(f"Graph Data Type: {type(graph_data)}")
        if isinstance(graph_data, dict):
            nodes = graph_data.get("nodes", [])
            print(f"Nodes count: {len(nodes)}")
            print(f"First node match: {nodes[0].get('name') == 'film'}")
        else:
            print("Graph data is not a dict")
    else:
        print("Parsed result is empty or not a list")

except Exception as e:
    print(f"Error: {e}")

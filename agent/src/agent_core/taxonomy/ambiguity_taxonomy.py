"""Taxonomy for query ambiguities and clarification templates."""

AMBIGUITY_TAXONOMY = {
    "UNCLEAR_SCHEMA_REFERENCE": {
        "description": "Query mentions an entity or attribute that exists in multiple tables.",
        "example": "Which 'region' do you mean: customer region or store region?",
        "question_template": "I found multiple matches for '{mention}'. Do you mean {options}?",
    },
    "UNCLEAR_VALUE_REFERENCE": {
        "description": "Query mentions a value that could correspond to multiple attributes.",
        "example": "Does 'Springfield' refer to a city or a customer name?",
        "question_template": "Does '{mention}' refer to a {attribute1} or a {attribute2}?",
    },
    "MISSING_TEMPORAL_CONSTRAINT": {
        "description": "Query asks for a metric but doesn't specify a time range.",
        "example": "Would you like sales for the current year, or the last 30 days?",
        "question_template": "Which time period should I use for this calculation?",
    },
    "LOGICAL_METRIC_CONFLICT": {
        "description": (
            "Query combines metrics or filters in a way that is ambiguous or contradictory."
        ),
        "example": "Do you want the average of total sales, or the total of average sales?",
        "question_template": "Could you clarify how you'd like to aggregate those metrics?",
    },
    "MISSING_FILTER_CRITERIA": {
        "description": (
            "Query is too broad and needs additional filters to be performant or meaningful."
        ),
        "example": "Which product category should I filter by?",
        "question_template": (
            "To give you an accurate result, should I filter by a specific {entity}?"
        ),
    },
}

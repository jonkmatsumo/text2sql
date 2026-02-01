def translate_double_quotes_to_backticks(sql: str) -> str:
    """Translate double-quoted identifiers to MySQL backticks."""
    result = []
    in_single_quote = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'":
            if in_single_quote and i + 1 < len(sql) and sql[i + 1] == "'":
                result.append("''")
                i += 2
                continue
            in_single_quote = not in_single_quote
            result.append(ch)
            i += 1
            continue
        if ch == '"' and not in_single_quote:
            result.append("`")
            i += 1
            continue
        result.append(ch)
        i += 1
    return "".join(result)

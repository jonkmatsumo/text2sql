"""SQL comment stripping utilities."""

from __future__ import annotations


def strip_sql_comments(sql: str) -> str:
    """Strip SQL line/block comments while preserving quoted strings."""
    if not isinstance(sql, str) or not sql:
        return ""

    out: list[str] = []
    i = 0
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    block_comment_depth = 0

    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append("\n")
            i += 1
            continue

        if block_comment_depth > 0:
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                i += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                i += 2
                continue
            # Preserve line boundaries for parser diagnostics.
            out.append("\n" if ch == "\n" else " ")
            i += 1
            continue

        if in_single_quote:
            out.append(ch)
            if ch == "'":
                if nxt == "'":  # Escaped single quote
                    out.append(nxt)
                    i += 2
                    continue
                in_single_quote = False
            i += 1
            continue

        if in_double_quote:
            out.append(ch)
            if ch == '"':
                if nxt == '"':  # Escaped double quote
                    out.append(nxt)
                    i += 2
                    continue
                in_double_quote = False
            i += 1
            continue

        if ch == "'":
            in_single_quote = True
            out.append(ch)
            i += 1
            continue

        if ch == '"':
            in_double_quote = True
            out.append(ch)
            i += 1
            continue

        if ch == "-" and nxt == "-":
            in_line_comment = True
            i += 2
            continue

        if ch == "/" and nxt == "*":
            block_comment_depth = 1
            i += 2
            continue

        out.append(ch)
        i += 1

    return "".join(out)

"""Split a migration file on semicolons without cutting procedure bodies."""

from __future__ import annotations


def split_sql(raw: str) -> list[tuple[str, str]]:
    """Return ``(section, statement)`` pairs.

    A full-line comment ``-- @section core`` or ``-- @section event`` selects
    the section for the following statements. Semicolons inside ``$$`` bodies
    stay part of the statement.
    """
    statements: list[tuple[str, str]] = []
    buf: list[str] = []
    section = "core"
    index = 0
    length = len(raw)
    in_dollar = False
    in_string = False
    in_line_comment = False
    while index < length:
        if in_line_comment:
            if raw[index] == "\n":
                in_line_comment = False
                buf.append("\n")
            index += 1
            continue
        if in_dollar:
            if raw.startswith("$$", index):
                buf.append("$$")
                index += 2
                in_dollar = False
                continue
            buf.append(raw[index])
            index += 1
            continue
        if in_string:
            buf.append(raw[index])
            if raw[index] == "'":
                if index + 1 < length and raw[index + 1] == "'":
                    buf.append("'")
                    index += 2
                    continue
                in_string = False
            index += 1
            continue
        if raw[index] == "-" and index + 1 < length and raw[index + 1] == "-":
            line_end = raw.find("\n", index)
            if line_end < 0:
                line_end = length
            comment = raw[index:line_end].strip()
            if comment.startswith("-- @section "):
                section = comment.split()[-1]
                if buf and not "".join(buf).strip():
                    buf = []
            in_line_comment = True
            index += 2
            continue
        if raw[index] == "'":
            in_string = True
            buf.append(raw[index])
            index += 1
            continue
        if raw.startswith("$$", index):
            in_dollar = True
            buf.append("$$")
            index += 2
            continue
        if raw[index] == ";":
            text = _clean("".join(buf))
            if text:
                statements.append((section, text))
            buf = []
            index += 1
            continue
        buf.append(raw[index])
        index += 1
    tail = _clean("".join(buf))
    if tail:
        statements.append((section, tail))
    return statements


def _clean(text: str) -> str:
    lines = [line for line in text.splitlines() if not line.strip().startswith("--")]
    return "\n".join(lines).strip()

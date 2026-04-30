from __future__ import annotations

import re


def rtf_to_text(data: bytes) -> str:
    """
    Minimal RTF -> plain text converter for typical Windows RTF exports.

    Notes:
    - focuses on extracting text for search / regex extraction
    - handles \\'hh and \\uNNNN escapes
    """
    # detect codepage: \ansicpg1251 (fallback to 1251)
    m = re.search(br"\\ansicpg(\d+)", data)
    cpg = m.group(1).decode("ascii", errors="ignore") if m else "1251"

    # decode to a 1:1 mapping buffer first (latin1 keeps byte values)
    s = data.decode("latin1", errors="ignore")
    out_chars: list[str] = []

    i = 0
    n = len(s)
    while i < n:
        ch = s[i]

        # hex escaped byte: \'hh
        if i + 3 < n and ch == "\\" and s[i + 1] == "'" and re.match(r"[0-9A-Fa-f]{2}", s[i + 2 : i + 4]):
            out_chars.append(chr(int(s[i + 2 : i + 4], 16)))
            i += 4
            continue

        if ch == "\\":
            # escaped control chars like \{ \} \\
            if i + 1 < n and s[i + 1] in "{}\\":
                out_chars.append(s[i + 1])
                i += 2
                continue

            # control word: \word-123?
            j = i + 1
            while j < n and s[j].isalpha():
                j += 1
            word = s[i + 1 : j]

            while j < n and s[j] in "-0123456789":
                j += 1
            num = s[i + 1 + len(word) : j]

            if j < n and s[j] == " ":
                j += 1

            # unicode escape: \uNNNN? (optional fallback char after it)
            if word == "u" and num:
                try:
                    code = int(num)
                    if code < 0:
                        code = 65536 + code
                    out_chars.append(chr(code))
                    # RTF specifies a single fallback character after \uN
                    # consume it only if it looks like a normal character
                    if j < n and s[j] not in "\\{}":
                        j += 1
                except Exception:
                    pass

            i = j
            continue

        if ch in "{}":
            i += 1
            continue

        out_chars.append(ch)
        i += 1

    text = "".join(out_chars)
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    # normalize common non-breaking space artifacts
    text = text.replace("\u00a0", " ")
    return text


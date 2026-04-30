from __future__ import annotations

import argparse
import re
from pathlib import Path


def rtf_to_text(data: bytes) -> str:
    """
    Minimal RTF -> plain text converter for typical Windows RTF exports (cp1251).
    Good enough for search / extraction tasks.
    """
    # detect codepage: \ansicpg1251
    m = re.search(br"\\ansicpg(\d+)", data)
    cpg = m.group(1).decode("ascii", errors="ignore") if m else "1251"

    s = data.decode("latin1", errors="ignore")
    # We'll build as unicode to support \uNNNN escapes.
    out_chars: list[str] = []

    i = 0
    n = len(s)
    while i < n:
        ch = s[i]

        # hex escaped byte: \'hh
        if i + 3 < n and ch == "\\" and s[i + 1] == "'" and re.match(r"[0-9A-Fa-f]{2}", s[i + 2 : i + 4]):
            out.append(int(s[i + 2 : i + 4], 16))
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
                    # RTF specifies a single fallback character after \uN; consume it only if it's
                    # a real character (not the start of the next control sequence/group).
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

    # normalize whitespace a bit
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("path", type=Path)
    ap.add_argument("--find", default="", help="Substring to find (case-insensitive)")
    ap.add_argument("--context", type=int, default=400)
    args = ap.parse_args()

    data = args.path.read_bytes()
    txt = rtf_to_text(data)

    if args.find:
        needle = args.find.lower()
        idx = txt.lower().find(needle)
        if idx == -1:
            print("NOT_FOUND")
            return 2
        c = args.context
        print(txt[max(0, idx - c) : idx + c])
        return 0

    print(txt[:2000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


from __future__ import annotations

import json
import re
from pathlib import Path


ALLOWED_OTF = tuple("ABCDEFGHI")


def _clean(s: str) -> str:
    return " ".join((s or "").replace("\t", " ").split()).strip()


def _looks_like_role(line: str) -> bool:
    """
    Very lightweight heuristic for "role / должность" lines in extracted PS text.
    We intentionally keep it permissive and rely on dedup + cutoffs.
    """
    ln = _clean(line)
    if not ln:
        return False
    low = ln.lower()
    if len(ln) < 12:
        return False
    if any(x in low for x in ("разряд", "буриль", "помощник", "машинист", "оператор", "слесар", "инженер")):
        return True
    # generic: has a digit and looks like a title
    if re.search(r"\b\d{1,2}\b", ln) and not low.startswith(("таблица", "примечание", "характеристика")):
        return True
    return False


def parse_otf_roles(ps_text: str) -> dict[str, dict]:
    """
    Extract OTF code -> roles from the functional map section.
    Extraction quality depends on how docx->text was produced; this is a best-effort parser.
    """
    lines = [ln.rstrip("\n") for ln in (ps_text or "").splitlines()]
    # Normalize: strip but keep empty lines for stop conditions
    lines = [ln.strip() for ln in lines]

    out: dict[str, dict] = {}
    i = 0
    n = len(lines)
    while i < n:
        ln = _clean(lines[i])
        if ln in ALLOWED_OTF:
            code = ln
            roles: list[str] = []
            j = i + 1

            # scan until next OTF code or until too far
            scanned = 0
            while j < n and scanned < 350:
                cur = _clean(lines[j])
                if cur in ALLOWED_OTF:
                    break
                if _looks_like_role(cur):
                    # avoid extremely long "paragraph" captures
                    if len(cur) <= 220 and cur not in roles:
                        roles.append(cur)
                        if len(roles) >= 25:
                            break
                # stop early if section clearly ended
                if "особые условия" in cur.lower() or "необходимые знания" in cur.lower():
                    break
                j += 1
                scanned += 1

            out[code] = {
                "otf": code,
                "roles": roles,
                "education": "",
                "experience": "",
            }
            i = j
            continue
        i += 1

    # If we couldn't detect OTF codes at all, emit an empty template.
    if not out:
        out = {
            "A": {"otf": "A", "roles": [], "education": "", "experience": ""},
        }
    return out


def main() -> int:
    ps_text_path = Path("output/ps_extracted_text.txt")
    if not ps_text_path.exists():
        raise SystemExit("Missing output/ps_extracted_text.txt. Extract PS text first.")

    ps_text = ps_text_path.read_text(encoding="utf-8", errors="ignore")
    parsed = parse_otf_roles(ps_text)

    out_path = Path("output/ps_otf_parsed.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


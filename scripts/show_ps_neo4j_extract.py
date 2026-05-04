"""
Печать того, что сейчас лежит в Neo4j по ПС: длина raw_text, фрагменты, результат _extract_vpd_field_section_i.

Запуск из корня репозитория:
  python scripts/show_ps_neo4j_extract.py
Переменные: NEO4J_URI (по умолчанию bolt://localhost:7687), NEO4J_USER, NEO4J_PASSWORD.
"""
from __future__ import annotations

import os
import re
import runpy
import sys
from pathlib import Path

# Windows консоль: не падать на символах вроде U+2002 из docx
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from neo4j import GraphDatabase

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_exp = runpy.run_path(str(_ROOT / "scripts" / "export_mandatory_ps_table.py"), run_name="ps_show")
_extract_vpd_field_section_i = _exp["_extract_vpd_field_section_i"]
_slice_section_i = _exp["_slice_section_i"]
_looks_like_ps = _exp["_looks_like_ps"]


def _snippet(text: str, needle: str, radius: int = 220) -> str:
    t = text or ""
    i = t.lower().find(needle.lower())
    if i < 0:
        return ""
    a = max(0, i - radius)
    b = min(len(t), i + len(needle) + radius)
    frag = t[a:b].replace("\r", " ")
    frag = re.sub(r"\s+", " ", frag)
    return frag


def main() -> int:
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4j_password")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as s:
            rows = list(
                s.run(
                    """
                    MATCH (ps:Document {source:'profstandard'})
                    RETURN ps.id AS id, ps.path AS path, ps.raw_text AS raw_text, ps.ps_general_code AS ps_general_code
                    ORDER BY ps.updated_at DESC
                    """
                )
            )
        print(f"NEO4J_URI={uri!r}  documents profstandard: {len(rows)}\n")
        for r in rows:
            rid = r.get("id")
            path = r.get("path")
            raw = r.get("raw_text") or ""
            if not isinstance(raw, str):
                raw = str(raw or "")
            code = r.get("ps_general_code")
            n = len(raw)
            looks = _looks_like_ps(raw)
            vpd_raw, vpd_code = _extract_vpd_field_section_i(raw)
            si = _slice_section_i(raw)
            print("=" * 72)
            print(f"id={rid!r}")
            print(f"path={path!r}")
            print(f"ps_general_code={code!r}")
            print(f"raw_text chars={n}  _looks_like_ps={looks}")
            print(f"_slice_section_i chars={len(si)}")
            if si.strip():
                print(f"_slice_section_i preview: {si[:400]!r}")
            print(f"_extract_vpd_field_section_i: code={vpd_code!r}  raw={vpd_raw!r}")
            idx_vid = raw.lower().find("вид профессиональн")
            idx_os = re.search(r"общие\s+сведения", raw, re.IGNORECASE)
            idx_os_i = idx_os.start() if idx_os else -1
            print(f"index 'вид профессиональн' in full raw_text: {idx_vid}")
            print(f"index 'общие сведения' in full raw_text: {idx_os_i}")
            head = raw[:1800].replace("\n", "\\n")
            print(f"--- raw_text[:1800] (escaped newlines) ---\n{head}\n--- end head ---")
            for needle in ("вид профессиональн", "общие сведения", "профессиональный стандарт"):
                sn = _snippet(raw, needle)
                if sn:
                    print(f"--- around {needle!r} ---\n{sn}\n")
            if not looks:
                print("(document skipped by export _looks_like_ps)\n")
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

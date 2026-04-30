from __future__ import annotations

import os
import re
from pathlib import Path

from neo4j import GraphDatabase


def _get_env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v else default


def _extract_ps_meta(ps_text: str) -> dict[str, str]:
    # Name + code from the title block (best-effort, different PS templates exist).
    marker = "ПРОФЕССИОНАЛЬНЫЙ СТАНДАРТ"
    idx = ps_text.find(marker)
    code = ""
    name = ""
    order = ""
    if idx != -1:
        after = ps_text[idx + len(marker) : idx + len(marker) + 800]
        mcode = re.search(r"\b(\d{3,6})\b", after)
        if mcode:
            code = mcode.group(1)
            name = after[: mcode.start()].strip()
            # clean name from extra tokens/newlines
            name = " ".join(name.split())

    # Alternative: explicit "Регистрационный номер" lines
    if not code:
        m = re.search(r"регистрационн\w*\s+номер\w*\s*[:\-]?\s*(\d{3,6})\b", ps_text, flags=re.IGNORECASE)
        if m:
            code = m.group(1)

    # Alternative: "Рег. № 1426" / "№ 1426" near "профессиональный стандарт"
    if not code:
        m = re.search(r"профессиональн\w*\s+стандарт[^\n]{0,200}?№\s*(\d{3,6})\b", ps_text, flags=re.IGNORECASE)
        if m:
            code = m.group(1)

    if not name:
        # Try to capture a common variant: "Профессиональный стандарт <name>" in one line
        m = re.search(r"профессиональн\w*\s+стандарт\s+([^\n]{10,220})", ps_text, flags=re.IGNORECASE)
        if m:
            cand = " ".join(m.group(1).split())
            # strip trailing numbers/sections
            cand = re.sub(r"\s+\d{3,6}\b.*$", "", cand).strip()
            name = cand

    morder = re.search(r"от «(\d{1,2})»\s*([а-яё]+)\s*(\d{4}) г\.?\s*№\s*(\d+н|\d+)", ps_text, re.IGNORECASE)
    if morder:
        day, month, year, num = morder.groups()
        # Normalize month casing but keep original word
        order = f"от «{day}» {month} {year} г. № {num}"

    # Вид профессиональной деятельности: use first found "Эксплуатационное и разведочное бурение..." line
    # (Fallback to empty if not found)
    # Limit to the current line to avoid capturing the subsequent "19.071 ..." blocks.
    mscope = re.search(
        r"(Эксплуатационное и разведочное бурение[^\r\n]{0,180})",
        ps_text,
        flags=re.IGNORECASE,
    )
    view_activity = mscope.group(1).strip() if mscope else ""

    return {
        "code_ps": code,
        "name_ps": name,
        "order_mintrud": order,
        "view_activity": view_activity,
    }


def _pick_best_profession(candidates: list[str]) -> str:
    """
    Heuristic: prefer Cyrillic, then longer strings (usually more specific).
    """
    def score(s: str) -> tuple[int, int]:
        s = s or ""
        cyr = len(re.findall(r"[А-Яа-яЁё]", s))
        return (cyr, len(s))

    cleaned = [" ".join((c or "").split()) for c in candidates if c and str(c).strip()]
    if not cleaned:
        return ""
    cleaned.sort(key=score, reverse=True)
    return cleaned[0]


def _looks_like_ps(ps_text: str) -> bool:
    t = (ps_text or "").lower()
    # Explicitly exclude our internal reports accidentally ingested as PS docs
    if "отчёт (markdown)" in t or "что llm извлекла" in t:
        return False
    return any(
        m in t
        for m in (
            "профессиональный стандарт",
            "профессиональные стандарты",
            "отф",
            "обобщенная трудовая функция",
            "минтруд",
        )
    )


def main() -> int:
    # Query NPA in graph via "proper" matching edges Norm->OTF.
    neo4j_uri = _get_env("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = _get_env("NEO4J_USER", "neo4j")
    neo4j_password = _get_env("NEO4J_PASSWORD", "neo4j_password")
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def clean_cell(v: object) -> str:
        return " ".join(str(v or "").split())

    try:
        # Load all PS documents (multi-PS)
        with driver.session() as s:
            ps_docs = list(
                s.run(
                    """
                    MATCH (ps:Document {source:'profstandard'})
                    RETURN ps.id AS id, ps.path AS path, ps.raw_text AS raw_text
                    ORDER BY ps.updated_at DESC
                    """
                )
            )
            if not ps_docs:
                raise SystemExit("No profstandard documents in Neo4j. Run PS ingest first.")

            out_lines: list[str] = []
            out_lines.append("# Список обязательных профессиональных стандартов (черновик под текущие данные)")
            out_lines.append("")
            out_lines.append("| № | Наименование профессионального стандарта | Код ПС | Наименование профессии/должности (дочернего общества) | Нормативный правовой акт РФ (пункт) | Код ОТФ | Возможные наименования должностей/профессий | Вид профессиональной деятельности | Вид обязательности |")
            out_lines.append("|---|---|---|---|---|---|---|---|---|")

            # Query per-PS best matching norm (first row), prefer norm 200 when exists
            cypher_best = """
            MATCH (ps:Document {id:$ps_id, source:'profstandard'})-[:HAS_OTF]->(o:OTF)<-[:MATCHES_OTF]-(n:Norm)-[:MENTIONED_IN]->(npa:Document {source:'npa'})
            OPTIONAL MATCH (n)-[:SETS_REQUIREMENT]->(r:Requirement)
            WITH npa, n, collect(DISTINCT r.type) AS req_types, collect(DISTINCT o.code) AS otf_codes, collect(DISTINCT o.id) AS otf_ids
            WHERE size(req_types) > 0
            RETURN
              npa.title AS npa_title,
              n.number AS norm_number,
              n.text AS norm_text,
              otf_codes,
              otf_ids
            ORDER BY CASE WHEN n.number = '200' THEN 0 ELSE 1 END, n.number
            LIMIT 1
            """

            for idx, ps in enumerate(ps_docs, start=1):
                ps_id = ps["id"]
                ps_text = ps.get("raw_text") or ""
                if not isinstance(ps_text, str):
                    ps_text = str(ps_text or "")

                if not _looks_like_ps(ps_text):
                    continue
                ps_meta = _extract_ps_meta(ps_text)

                # Fallback: PS name from extracted professions in graph
                if not (ps_meta.get("name_ps") or "").strip():
                    profs = [
                        r["name"]
                        for r in s.run(
                            "MATCH (p:Profession)-[:MENTIONED_IN]->(ps:Document {id:$ps_id}) RETURN DISTINCT p.name AS name",
                            ps_id=ps_id,
                        )
                        if r.get("name")
                    ]
                    ps_meta["name_ps"] = _pick_best_profession([str(x) for x in profs])

                # Hard filter: keep only "real" PS documents (must have either Mintrud order or registration code)
                if not (ps_meta.get("order_mintrud") or "").strip() and not (ps_meta.get("code_ps") or "").strip():
                    continue

                best = s.run(cypher_best, ps_id=ps_id).single()

                npa_title = clean_cell(best.get("npa_title") if best else "")
                norm_number = clean_cell(best.get("norm_number") if best else "")
                norm_text = clean_cell(best.get("norm_text") if best else "")
                otf_codes_m = best.get("otf_codes") if best else []
                otf_ids_m = best.get("otf_ids") if best else []

                # All OTF codes attached to PS (fallback)
                all_otf_codes = [
                    r["code"]
                    for r in s.run(
                        "MATCH (ps:Document {id:$ps_id})-[:HAS_OTF]->(o:OTF) RETURN DISTINCT o.code AS code ORDER BY code",
                        ps_id=ps_id,
                    )
                    if r.get("code")
                ]
                if not all_otf_codes:
                    # Skip documents that did not produce OTF codes (likely not a real PS, or extraction failed).
                    continue
                matched_otf_codes = sorted(set([str(x) for x in (otf_codes_m or []) if x])) or all_otf_codes

                # Roles for matched OTFs (fallback to all roles for PS)
                role_names: list[str] = []
                ids = [str(x) for x in (otf_ids_m or []) if x]
                if ids:
                    rrec = s.run(
                        """
                        MATCH (o:OTF)-[:HAS_ROLE]->(r:Role)
                        WHERE o.id IN $ids
                        RETURN collect(DISTINCT r.name) AS roles
                        """,
                        ids=ids,
                    ).single()
                    if rrec and rrec.get("roles"):
                        role_names = [clean_cell(x) for x in (rrec.get("roles") or []) if clean_cell(x)]
                if not role_names:
                    rrec2 = s.run(
                        """
                        MATCH (ps:Document {id:$ps_id})-[:HAS_OTF]->(o:OTF)-[:HAS_ROLE]->(r:Role)
                        RETURN collect(DISTINCT r.name) AS roles
                        """,
                        ps_id=ps_id,
                    ).single()
                    if rrec2 and rrec2.get("roles"):
                        role_names = [clean_cell(x) for x in (rrec2.get("roles") or []) if clean_cell(x)]

                roles_cell = "<br>".join(role_names[:25]) + ("<br>..." if len(role_names) > 25 else "")

                name_ps_cell = clean_cell(ps_meta["name_ps"] or Path(ps.get("path") or "").stem)
                code_ps_cell = clean_cell(ps_meta["code_ps"])
                order_cell_s = clean_cell(ps_meta["order_mintrud"])
                view_activity_cell = clean_cell(ps_meta["view_activity"] or "—")

                applied_to_cell = (norm_text[:500] + ("..." if len(norm_text) > 500 else "")) if norm_text else "—"
                npa_cell = clean_cell(f"{npa_title} (пункт {norm_number})").strip() if npa_title or norm_number else "—"
                otf_cell = clean_cell(", ".join(matched_otf_codes)) if matched_otf_codes else "—"

                mandatory_cell = "обязателен" if (npa_title or norm_number) else "—"

                # De-dup: same PS can be ingested from multiple files/representations.
                key = (code_ps_cell or "").strip() or f"{name_ps_cell}::{order_cell_s}".strip()
                if not hasattr(main, "_seen_keys"):
                    setattr(main, "_seen_keys", set())
                seen: set[str] = getattr(main, "_seen_keys")
                if key in seen:
                    continue
                seen.add(key)

                out_lines.append(
                    f"| {len(seen)} | {name_ps_cell} <br><small>{order_cell_s}</small> | {code_ps_cell} | {applied_to_cell} | {npa_cell} | {otf_cell} | {roles_cell} | {view_activity_cell} | {mandatory_cell} |"
                )

        out_path = Path("output/list_mandatory_ps.md")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(out_lines), encoding="utf-8")
        print(f"Wrote {out_path}")
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


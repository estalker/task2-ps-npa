from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass

from neo4j import GraphDatabase


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _clean(s: str) -> str:
    return " ".join((s or "").split()).strip()


KEYWORDS = [
    "бурен",  # бурение/бурению
    "освоен",
    "ремонт",
    "реконструкц",
    "консервац",
    "ликвидац",
    "скважин",
    "геофиз",
    "добыч",
    "сбор",
    "подготовк",
    "пвр",  # ПВР
]


def _kw_set(text: str) -> set[str]:
    t = (text or "").lower()
    return {k for k in KEYWORDS if k in t}


@dataclass(frozen=True)
class OtfMeta:
    code: str
    name: str


def parse_otf_names(ps_text: str) -> dict[str, str]:
    """
    Extract OTF code -> OTF name from the functional map section.
    Works for our current PS export style where OTF code is on a separate line.
    """
    out: dict[str, str] = {}
    allowed = {chr(c) for c in range(ord("A"), ord("I") + 1)}
    lines = [ln.strip() for ln in ps_text.splitlines()]
    n = len(lines)
    i = 0
    while i < n:
        if lines[i] in allowed:
            code = lines[i]
            # next non-empty line is name
            j = i + 1
            while j < n and not lines[j]:
                j += 1
            if j < n:
                name = _clean(lines[j])
                if len(name) >= 10 and code not in out:
                    out[code] = name
            i = j + 1
            continue
        i += 1
    return out


ALLOWED_OTF = tuple("ABCDEFGHI")


def _looks_like_role(line: str) -> bool:
    ln = _clean(line)
    if not ln:
        return False
    low = ln.lower()
    if len(ln) < 12:
        return False
    if any(x in low for x in ("разряд", "буриль", "помощник", "машинист", "оператор", "слесар", "инженер")):
        return True
    if re.search(r"\b\d{1,2}\b", ln) and not low.startswith(("таблица", "примечание", "характеристика")):
        return True
    return False


def parse_otf_roles(ps_text: str) -> dict[str, dict]:
    """
    Best-effort parser for OTF code -> roles from extracted PS text.
    """
    lines = [ln.strip() for ln in (ps_text or "").splitlines()]
    n = len(lines)
    out: dict[str, dict] = {}

    i = 0
    while i < n:
        ln = _clean(lines[i])
        if ln in ALLOWED_OTF:
            code = ln
            roles: list[str] = []
            j = i + 1
            scanned = 0
            while j < n and scanned < 350:
                cur = _clean(lines[j])
                if cur in ALLOWED_OTF:
                    break
                if _looks_like_role(cur):
                    if len(cur) <= 220 and cur not in roles:
                        roles.append(cur)
                        if len(roles) >= 25:
                            break
                if "особые условия" in cur.lower() or "необходимые знания" in cur.lower():
                    break
                j += 1
                scanned += 1

            out[code] = {"otf": code, "roles": roles}
            i = j
            continue
        i += 1

    return out


def _looks_like_ps(ps_text: str) -> bool:
    t = (ps_text or "").lower()
    if "отчёт (markdown)" in t or "что llm извлекла" in t:
        return False
    # broad markers typical for professional standards
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
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687").strip() or "bolt://localhost:7687"
    neo4j_user = os.getenv("NEO4J_USER", "neo4j").strip() or "neo4j"
    neo4j_password = os.getenv("NEO4J_PASSWORD", "neo4j_password").strip() or "neo4j_password"

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    with driver.session() as s:
        # Constraints (safe with IF NOT EXISTS)
        for q in [
            "CREATE CONSTRAINT otf_id IF NOT EXISTS FOR (o:OTF) REQUIRE o.id IS UNIQUE",
            "CREATE CONSTRAINT role_id IF NOT EXISTS FOR (r:Role) REQUIRE r.id IS UNIQUE",
        ]:
            s.run(q)

        # Load all NPA WorkScopes (target universe for matching)
        ws: list[str] = []
        for r in s.run("MATCH (w:WorkScope) RETURN w.name AS name"):
            name_raw = r["name"]
            if isinstance(name_raw, str) and name_raw.strip():
                # IMPORTANT: keep raw name for exact MATCH later
                ws.append(name_raw)

        # Iterate ALL PS documents (multi-PS scenario)
        ps_docs = list(
            s.run(
                """
                MATCH (d:Document {source:'profstandard'})
                RETURN d.id AS id, d.path AS path, d.raw_text AS raw_text
                ORDER BY d.updated_at DESC
                """
            )
        )
        if not ps_docs:
            raise SystemExit("No profstandard Document found in Neo4j. Run PS ingest first.")

        for rec in ps_docs:
            ps_doc_id = rec["id"]
            ps_text = rec.get("raw_text") or ""
            if not isinstance(ps_text, str):
                ps_text = str(ps_text or "")

            if not _looks_like_ps(ps_text):
                # Skip accidental non-PS documents that were ingested under the same source label.
                continue

            otf_names = parse_otf_names(ps_text)
            otf_roles = parse_otf_roles(ps_text)
            all_codes = sorted(set(list(otf_names.keys()) + list(otf_roles.keys())))
            all_codes = [c for c in all_codes if c in ALLOWED_OTF]
            if not all_codes:
                # If we cannot detect any OTF codes, this is likely not a real PS text (or extraction is broken).
                continue

            # Clean previous OTF/Role graph for this PS to keep rebuild deterministic
            s.run(
                """
                MATCH (ps:Document {id:$ps_id})-[:HAS_OTF]->(o:OTF)
                OPTIONAL MATCH (o)-[:HAS_ROLE]->(r:Role)
                DETACH DELETE o, r
                """,
                ps_id=ps_doc_id,
            )

            # Upsert OTF/Role nodes and attach to PS doc
            for code in all_codes:
                code = str(code).strip()
                if code not in ALLOWED_OTF:
                    continue

                otf_name = _clean(otf_names.get(code) or "")
                if not otf_name:
                    otf_name = f"ОТФ {code}"

                otf_id = _sha1(f"{ps_doc_id}:{code}")
                s.run(
                    """
                    MATCH (ps:Document {id:$ps_id})
                    MERGE (o:OTF {id:$otf_id})
                    SET o.code=$code, o.name=$name
                    MERGE (ps)-[:HAS_OTF]->(o)
                    """,
                    ps_id=ps_doc_id,
                    otf_id=otf_id,
                    code=code,
                    name=otf_name,
                )

                roles: list[str] = (otf_roles.get(code) or {}).get("roles") or []
                roles = [_clean(r) for r in roles if _clean(r)]
                for role in roles:
                    role_id = _sha1(f"{otf_id}:{role}")
                    s.run(
                        """
                        MATCH (o:OTF {id:$otf_id})
                        MERGE (r:Role {id:$role_id})
                        SET r.name=$name
                        MERGE (o)-[:HAS_ROLE]->(r)
                        """,
                        otf_id=otf_id,
                        role_id=role_id,
                        name=role,
                    )

                # Build OTF -> WorkScope matching based on keyword overlap
                otf_kws = _kw_set(otf_name)
                for role in roles:
                    otf_kws |= _kw_set(role)

                best: list[tuple[str, int, list[str]]] = []
                for wname in ws:
                    w_kws = _kw_set(wname)
                    overlap = sorted(list(otf_kws & w_kws))
                    score = len(overlap)
                    if score >= 2:
                        best.append((wname, score, overlap))

                best.sort(key=lambda x: (-x[1], len(x[0])))
                best = best[:5]
                for wname, score, overlap in best:
                    s.run(
                        """
                        MATCH (o:OTF {id:$otf_id})
                        MATCH (w:WorkScope {name:$wname})
                        MERGE (o)-[rel:INVOLVES]->(w)
                        SET rel.score=$score, rel.keywords=$keywords
                        """,
                        otf_id=otf_id,
                        wname=wname,
                        score=score,
                        keywords=overlap,
                    )

        # Now connect Norm -> OTF via shared WorkScope
        s.run(
            """
            MATCH (n:Norm)-[:APPLIES_TO]->(w:WorkScope)<-[:INVOLVES]-(o:OTF)
            MERGE (n)-[m:MATCHES_OTF]->(o)
            SET m.via_workscope = w.name
            """,
        )

    driver.close()
    print("Matching graph built.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


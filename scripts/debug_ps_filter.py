from __future__ import annotations

import os

from neo4j import GraphDatabase

from scripts.export_mandatory_ps_table import _extract_ps_meta, _looks_like_ps


def main() -> int:
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687").strip() or "bolt://localhost:7687"
    user = os.getenv("NEO4J_USER", "neo4j").strip() or "neo4j"
    password = os.getenv("NEO4J_PASSWORD", "neo4j_password").strip() or "neo4j_password"
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as s:
            for r in s.run(
                """
                MATCH (ps:Document {source:'profstandard'})
                RETURN ps.id AS id, ps.path AS path, substring(ps.raw_text,0,60) AS head, ps.raw_text AS t
                ORDER BY ps.updated_at DESC
                """
            ):
                t = r["t"] or ""
                ok = _looks_like_ps(t)
                meta = _extract_ps_meta(t)
                head = (r["head"] or "").replace("\n", " ")
                print(
                    ("OK " if ok else "NO "),
                    r["id"],
                    "|",
                    r["path"],
                    "|",
                    head,
                    "| code=",
                    meta.get("code_ps") or "",
                    "order=",
                    meta.get("order_mintrud") or "",
                )
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


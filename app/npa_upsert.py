from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from neo4j import GraphDatabase


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str
    user: str
    password: str


CONSTRAINTS = [
    # new taxonomy for NPA -> WorkScope/Norm/Requirements
    "CREATE CONSTRAINT workscope_name IF NOT EXISTS FOR (w:WorkScope) REQUIRE w.name IS UNIQUE",
    "CREATE CONSTRAINT norm_id IF NOT EXISTS FOR (n:Norm) REQUIRE n.id IS UNIQUE",
    # Requirement already has hash constraint if you ran PS ingest,
    # but creating again is safe with IF NOT EXISTS.
    "CREATE CONSTRAINT requirement_hash IF NOT EXISTS FOR (r:Requirement) REQUIRE r.hash IS UNIQUE",
    # Document already has document_id constraint in our PS schema,
    # but safe to create again.
    "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE",
]


MERGE_CYPHER = """
WITH $doc_id AS docId, $source AS source, $path AS path, $title AS title,
     $original_path AS original_path, $original_filename AS original_filename
MERGE (d:Document {id: docId})
SET d.source = source,
    d.path = path,
    d.title = title,
    d.original_path = original_path,
    d.original_filename = original_filename,
    d.updated_at = datetime()

WITH d, coalesce($norms, []) AS norms
UNWIND norms AS norm

MERGE (n:Norm {id: norm.id})
SET n.number = norm.number,
    n.text = norm.text

MERGE (n)-[:MENTIONED_IN]->(d)

WITH d, n, norm
// WorkScope
FOREACH (_ IN CASE WHEN norm.workscope IS NULL THEN [] ELSE [1] END |
  MERGE (w:WorkScope {name: norm.workscope})
  MERGE (n)-[:APPLIES_TO]->(w)
  MERGE (w)-[:MENTIONED_IN]->(d)
)

WITH d, n, norm

// Requirements
UNWIND norm.requirements AS req
MERGE (r:Requirement {hash: req.hash})
SET r.type = req.kind,
    r.text = req.text,
    r.source = "npa"

MERGE (n)-[:SETS_REQUIREMENT]->(r)
FOREACH (_ IN CASE WHEN norm.workscope IS NULL THEN [] ELSE [1] END |
  MERGE (w2:WorkScope {name: norm.workscope})
  MERGE (w2)-[:HAS_REQUIREMENT]->(r)
)
RETURN d.id AS documentId
"""


def ensure_schema(cfg: Neo4jConfig) -> None:
    driver = GraphDatabase.driver(cfg.uri, auth=(cfg.user, cfg.password))
    try:
        with driver.session() as s:
            for q in CONSTRAINTS:
                s.run(q)
    finally:
        driver.close()


def upsert_npa_document(
    cfg: Neo4jConfig,
    *,
    doc_id: str,
    source: str,
    path: str,
    title: str,
    original_path: str | None = None,
    original_filename: str | None = None,
    norms: list[dict[str, Any]],
) -> None:
    driver = GraphDatabase.driver(cfg.uri, auth=(cfg.user, cfg.password))
    try:
        with driver.session() as s:
            s.run(
                MERGE_CYPHER,
                doc_id=doc_id,
                source=source,
                path=path,
                title=title,
                original_path=original_path,
                original_filename=original_filename,
                norms=norms,
            )
    finally:
        driver.close()


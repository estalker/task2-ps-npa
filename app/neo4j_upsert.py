from __future__ import annotations

from dataclasses import dataclass

from neo4j import GraphDatabase

from .schema import Extraction


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str
    user: str
    password: str


CONSTRAINTS = [
    "CREATE CONSTRAINT profession_name IF NOT EXISTS FOR (p:Profession) REQUIRE p.name IS UNIQUE",
    "CREATE CONSTRAINT qualification_name IF NOT EXISTS FOR (q:Qualification) REQUIRE q.name IS UNIQUE",
    "CREATE CONSTRAINT requirement_hash IF NOT EXISTS FOR (r:Requirement) REQUIRE r.hash IS UNIQUE",
    "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE",
]


MERGE_CYPHER = """
WITH $doc_id AS docId, $source AS source, $path AS path, $raw_text AS rawText
MERGE (d:Document {id: docId})
SET d.source = source,
    d.path = path,
    d.raw_text = rawText,
    d.updated_at = datetime()

WITH d, coalesce($extractions, []) AS extractions
UNWIND extractions AS ex
WITH d,
     ex.profession AS profession,
     ex.qualification AS qualification,
     ex.ps_general_code AS ps_general_code,
     ex.education AS education,
     ex.education_hash AS education_hash,
     ex.experience_hash AS experience_hash,
     ex.experience AS experience

FOREACH (_ IN CASE WHEN ps_general_code IS NULL THEN [] ELSE [1] END |
  SET d.ps_general_code = ps_general_code
)

FOREACH (_ IN CASE WHEN profession IS NULL THEN [] ELSE [1] END |
  MERGE (p:Profession {name: profession})
  MERGE (p)-[:MENTIONED_IN]->(d)
)

FOREACH (_ IN CASE WHEN qualification IS NULL THEN [] ELSE [1] END |
  MERGE (q:Qualification {name: qualification})
  MERGE (q)-[:MENTIONED_IN]->(d)
)

FOREACH (_ IN CASE WHEN profession IS NULL OR qualification IS NULL THEN [] ELSE [1] END |
  MERGE (p:Profession {name: profession})
  MERGE (q:Qualification {name: qualification})
  MERGE (p)-[:HAS_QUALIFICATION]->(q)
)

FOREACH (_ IN CASE WHEN education IS NULL THEN [] ELSE [1] END |
  MERGE (r:Requirement {hash: education_hash})
  SET r.type = "education", r.text = education
  MERGE (r)-[:MENTIONED_IN]->(d)
)

FOREACH (_ IN CASE WHEN experience IS NULL THEN [] ELSE [1] END |
  MERGE (r:Requirement {hash: experience_hash})
  SET r.type = "experience", r.text = experience
  MERGE (r)-[:MENTIONED_IN]->(d)
)

FOREACH (_ IN CASE WHEN profession IS NULL OR education IS NULL THEN [] ELSE [1] END |
  MERGE (p:Profession {name: profession})
  MERGE (r:Requirement {hash: education_hash})
  MERGE (p)-[:REQUIRES]->(r)
)

FOREACH (_ IN CASE WHEN profession IS NULL OR experience IS NULL THEN [] ELSE [1] END |
  MERGE (p:Profession {name: profession})
  MERGE (r:Requirement {hash: experience_hash})
  MERGE (p)-[:REQUIRES]->(r)
)

FOREACH (_ IN CASE WHEN qualification IS NULL OR education IS NULL THEN [] ELSE [1] END |
  MERGE (q:Qualification {name: qualification})
  MERGE (r:Requirement {hash: education_hash})
  MERGE (q)-[:REQUIRES]->(r)
)

FOREACH (_ IN CASE WHEN qualification IS NULL OR experience IS NULL THEN [] ELSE [1] END |
  MERGE (q:Qualification {name: qualification})
  MERGE (r:Requirement {hash: experience_hash})
  MERGE (q)-[:REQUIRES]->(r)
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


def upsert_document(
    cfg: Neo4jConfig,
    doc_id: str,
    source: str,
    path: str,
    raw_text: str,
    extractions: list[Extraction],
) -> None:
    driver = GraphDatabase.driver(cfg.uri, auth=(cfg.user, cfg.password))
    try:
        with driver.session() as s:
            payload = [
                {
                    "profession": e.profession,
                    "qualification": e.qualification,
                    "ps_general_code": e.ps_general_code,
                    "education": e.education,
                    "experience": e.experience,
                    "education_hash": (
                        None
                        if not e.education
                        else __import__("hashlib")
                        .sha1(("education:" + e.education).encode("utf-8"))
                        .hexdigest()
                    ),
                    "experience_hash": (
                        None
                        if not e.experience
                        else __import__("hashlib")
                        .sha1(("experience:" + e.experience).encode("utf-8"))
                        .hexdigest()
                    ),
                }
                for e in extractions
                if e is not None
            ]
            if not payload:
                payload = [
                    {
                        "profession": None,
                        "qualification": None,
                        "ps_general_code": None,
                        "education": None,
                        "experience": None,
                        "education_hash": None,
                        "experience_hash": None,
                    }
                ]

            s.run(
                MERGE_CYPHER,
                doc_id=doc_id,
                source=source,
                path=path,
                raw_text=raw_text,
                extractions=payload,
            )
    finally:
        driver.close()


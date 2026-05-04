"""
Селективные сбросы Neo4j по этапам пайплайна (без DETACH DELETE всего графа).
"""

from __future__ import annotations

import os

from neo4j import Driver


def _database() -> str | None:
    v = os.getenv("NEO4J_DATABASE", "neo4j")
    return v.strip() if v else None


def reset_profstandard_subgraph(driver: Driver) -> None:
    """
    Этап 1: убрать только данные ПС (profstandard) и связанный слой OTF/Role,
    не трогая НПА и общие узлы, которые ещё используются.
    """
    db = _database()
    with driver.session(database=db) as s:
        s.run(
            """
            MATCH (d:Document {source: 'profstandard'})-[:HAS_OTF]->(o:OTF)
            OPTIONAL MATCH (o)-[:HAS_ROLE]->(r:Role)
            DETACH DELETE o, r
            """
        ).consume()
        s.run(
            """
            MATCH (d:Document {source: 'profstandard'})
            DETACH DELETE d
            """
        ).consume()

        for _ in range(12):
            s.run(
                """
                MATCH (p:Profession)
                WHERE NOT (p)-[:MENTIONED_IN]->(:Document)
                DETACH DELETE p
                """
            ).consume()
            s.run(
                """
                MATCH (q:Qualification)
                WHERE NOT (q)-[:MENTIONED_IN]->(:Document)
                DETACH DELETE q
                """
            ).consume()
            s.run(
                """
                MATCH (r:Requirement)
                WHERE NOT (r)--()
                DETACH DELETE r
                """
            ).consume()


def reset_npa_subgraph(driver: Driver) -> None:
    """
    Этап 2: удалить документы НПА, нормы и «осиротевшие» WorkScope / Requirement от НПА.
    """
    db = _database()
    with driver.session(database=db) as s:
        s.run(
            """
            MATCH (d:Document {source: 'npa'})<-[:MENTIONED_IN]-(n:Norm)
            DETACH DELETE n
            """
        ).consume()
        s.run(
            """
            MATCH (d:Document {source: 'npa'})
            DETACH DELETE d
            """
        ).consume()
        for _ in range(8):
            s.run(
                """
                MATCH (w:WorkScope)
                WHERE NOT (w)--()
                DETACH DELETE w
                """
            ).consume()
            s.run(
                """
                MATCH (r:Requirement)
                WHERE NOT (r)--()
                DETACH DELETE r
                """
            ).consume()


def reset_matching_layer(driver: Driver) -> None:
    """
    Этап 3 (перед build_matching): удалить все OTF/Role и связи сопоставления.
    Документы ПС/НПА и Norm/WorkScope не удаляются.
    """
    db = _database()
    with driver.session(database=db) as s:
        s.run(
            """
            MATCH (o:OTF)
            OPTIONAL MATCH (o)-[:HAS_ROLE]->(r:Role)
            DETACH DELETE o, r
            """
        ).consume()

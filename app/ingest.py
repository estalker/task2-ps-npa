from __future__ import annotations

import argparse
import hashlib
import os
import re
from pathlib import Path
import json

from tqdm import tqdm

from .chunking import chunk_text
from .docx_text import extract_text_from_docx
from .llm_extract import try_extract_with_llm
from .neo4j_upsert import Neo4jConfig, ensure_schema, upsert_document
from .schema import Extraction


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _doc_id_from_bytes(doc_source: str, b: bytes) -> str:
    """
    Stable document id across renames / path changes.
    Include source to avoid accidental collisions between different pipelines.
    """
    return _sha1(f"{doc_source}:{_sha256_bytes(b)}")


def _extract_requirement_block(raw_text: str, heading_re: str) -> str | None:
    """
    Best-effort extraction for profstandard requirement sections.
    Captures text after a heading until the next likely heading.
    """
    if not raw_text:
        return None
    m = re.search(heading_re, raw_text, flags=re.IGNORECASE)
    if not m:
        return None
    start = m.end()

    tail = raw_text[start:]
    # Stop at the next heading-like line (common in profstandards) or after a reasonable limit.
    stop = len(tail)
    for mm in re.finditer(r"\n\s*(Требования|Дополнительные\s+характеристики|Особые\s+условия|Необходимые\s+знания|Необходимые\s+умения)\b.*\n", tail, flags=re.IGNORECASE):
        stop = mm.start()
        break
    block = tail[:stop].strip()
    block = re.sub(r"\n{3,}", "\n\n", block)
    if not block:
        return None
    # Keep it small enough to store and query.
    return block[:2000]


def _extract_qualification_hint(raw_text: str) -> str | None:
    if not raw_text:
        return None
    m = re.search(r"(Уровень\s*\(?подуровень\)?\s*квалификации)\s*[:\-]?\s*(.+)", raw_text, flags=re.IGNORECASE)
    if m:
        return (m.group(2) or "").strip()[:200]
    m = re.search(r"\bУровень\s+квалификации\s*[:\-]?\s*(\d{1,2})\b", raw_text, flags=re.IGNORECASE)
    if m:
        return f"Уровень квалификации {m.group(1)}"
    return None


def main() -> int:
    p = argparse.ArgumentParser(description="Ingest docx profstandards into Neo4j")
    p.add_argument("--input", default="input", help="Folder with .docx files")
    p.add_argument("--doc-source", default="profstandard", help="Document source label")
    p.add_argument("--no-llm", action="store_true", help="Skip LLM extraction (raw text only)")
    p.add_argument("--llm-max-chars", type=int, default=1500, help="Chunk size to send to LLM")
    p.add_argument("--llm-overlap", type=int, default=400, help="Chunk overlap chars")
    p.add_argument("--llm-max-chunks", type=int, default=30, help="Max chunks per document")
    p.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "neo4j://localhost:7687"))
    p.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", "neo4j"))
    p.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", "neo4j_password"))
    args = p.parse_args()

    input_dir = Path(args.input)
    input_dir.mkdir(parents=True, exist_ok=True)
    # best-effort: map stored (short) name -> original name from upload manifest
    manifest: dict[str, str] = {}
    mf = input_dir / ".upload_manifest.jsonl"
    if mf.exists():
        try:
            for ln in mf.read_text(encoding="utf-8", errors="replace").splitlines():
                try:
                    obj = json.loads(ln)
                    if isinstance(obj, dict) and obj.get("stored") and obj.get("original"):
                        manifest[str(obj["stored"])] = str(obj["original"])
                except Exception:
                    continue
        except Exception:
            manifest = {}

    cfg = Neo4jConfig(uri=args.neo4j_uri, user=args.neo4j_user, password=args.neo4j_password)
    ensure_schema(cfg)

    files = sorted(input_dir.glob("*.docx"))
    if not files:
        print(f"No .docx files found in: {input_dir.resolve()}")
        return 0

    for f in tqdm(files, desc="Ingesting"):
        print(f"[DOC] reading {f.name} ...", flush=True)
        orig = manifest.get(f.name)
        doc_bytes = f.read_bytes()
        raw_text = extract_text_from_docx(f)
        print(f"[DOC] {f.name}: extracted {len(raw_text)} chars", flush=True)
        doc_id = _doc_id_from_bytes(args.doc_source, doc_bytes)

        extractions: list[Extraction] = []

        # cheap heuristic: profstandard title is often inside «...»
        # This usually corresponds to the profstandard / profession family.
        title_profession: str | None = None
        if raw_text:
            m = re.search(r"«([^»]{3,200})»", raw_text)
            if m:
                title_profession = m.group(1)
                extractions.append(Extraction(profession=title_profession))

        # best-effort non-LLM extraction for key requirement sections
        if raw_text:
            education = _extract_requirement_block(
                raw_text,
                r"\n\s*Требования\s+к\s+образованию\s+и\s+обучению\s*[:\-]?\s*\n",
            )
            experience = _extract_requirement_block(
                raw_text,
                r"\n\s*Требования\s+к\s+опыту\s+практической\s+работы\s*[:\-]?\s*\n",
            )
            qualification = _extract_qualification_hint(raw_text)
            if qualification or education or experience:
                extractions.append(
                    Extraction(
                        profession=title_profession,
                        qualification=qualification,
                        education=education,
                        experience=experience,
                    )
                )

        if raw_text and not args.no_llm:
            chunks = chunk_text(raw_text, max_chars=args.llm_max_chars, overlap=args.llm_overlap)
            chunks = chunks[: max(1, args.llm_max_chunks)]
            for ch in chunks:
                print(f"[LLM] {f.name}: chunk {ch.index + 1}/{len(chunks)} ({len(ch.text)} chars)", flush=True)
                try:
                    ex = try_extract_with_llm(ch.text)
                    if ex:
                        extractions.append(ex)
                except Exception as e:
                    print(f"[WARN] LLM extract failed for {f.name} chunk#{ch.index}: {e}")

        # If upload used a shortened stored name, keep original separately.
        # Also prefer original (human) filename for "path" when it exists.
        effective_path = orig or str(f.resolve())
        upsert_document(
            cfg=cfg,
            doc_id=doc_id,
            source=args.doc_source,
            path=effective_path,
            original_path=orig,
            original_filename=(orig.replace("\\", "/").split("/")[-1] if orig else None),
            raw_text=raw_text,
            extractions=extractions,
        )

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


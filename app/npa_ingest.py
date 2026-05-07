from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import json

from .npa_extract import extract_norm_segments
from .npa_llm_extract import try_extract_npa_with_llm
from .npa_upsert import Neo4jConfig, ensure_schema, upsert_npa_document
from .rtf_to_text import rtf_to_text


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


def main() -> int:
    p = argparse.ArgumentParser(description="Ingest NPA RTF into Neo4j (WorkScope + qualification requirements)")
    p.add_argument("--input", default="input", help="Folder with NPA files (currently: .rtf)")
    p.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "neo4j://localhost:7687"))
    p.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", "neo4j"))
    p.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", "neo4j_password"))
    p.add_argument("--out-md", default="output/npa_workscopes_requirements.md")
    p.add_argument("--use-llm", action="store_true", help="Use Ollama LLM to parse workscope/requirements from Norm text")
    p.add_argument("--llm-max-calls", type=int, default=30, help="Max LLM calls per run (safety)")
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

    rtf_files = sorted(input_dir.glob("*.rtf"))
    if not rtf_files:
        print(f"No .rtf files found in {input_dir.resolve()}")
        return 0

    # report collector: workscope -> kind -> list of requirement texts
    report: dict[str, dict[str, set[str]]] = {}

    for f in rtf_files:
        print(f"[NPA] extracting {f.name} ...", flush=True)
        orig = manifest.get(f.name)
        doc_path = str(f.resolve())
        doc_bytes = f.read_bytes()
        doc_id = _doc_id_from_bytes("npa", doc_bytes)
        title = Path(orig).stem if orig else f.stem

        text = rtf_to_text(doc_bytes)
        norms = extract_norm_segments(doc_title=title, doc_path=doc_path, text=text)

        # Optional: refine a limited number of Norms via LLM
        llm_budget = max(0, int(args.llm_max_calls))
        if args.use_llm and llm_budget > 0:
            refined = []
            for ns in norms:
                if llm_budget <= 0:
                    refined.append(ns)
                    continue
                print(f"[LLM] norm {ns.norm_number}: parsing ...", flush=True)
                try:
                    llm = try_extract_npa_with_llm(ns.norm_text)
                except Exception as e:
                    print(f"[WARN] LLM parse failed for norm {ns.norm_number}: {type(e).__name__}: {e}", flush=True)
                    llm = None
                if not llm:
                    refined.append(ns)
                    continue
                workscope = llm.get("workscope") or ns.workscope
                # merge requirements: prefer LLM when it provided the kind
                req_map = {r.kind: r.text for r in ns.requirements}
                for item in llm.get("requirements", []) or []:
                    k = item.get("kind")
                    txt = item.get("text")
                    if k and txt:
                        req_map[str(k)] = str(txt)

                from .npa_extract import NpaRequirement, NormSegment  # local import to avoid cycles

                reqs = [
                    NpaRequirement(kind=k, text=v, hash=_sha1(f"{k}:{v}"))
                    for k, v in req_map.items()
                    if k in ("education", "experience", "training", "permit") and v
                ]
                refined.append(
                    NormSegment(
                        doc_title=ns.doc_title,
                        doc_path=ns.doc_path,
                        norm_number=ns.norm_number,
                        norm_text=ns.norm_text,
                        workscope=workscope,
                        requirements=reqs,
                    )
                )
                llm_budget -= 1
            norms = refined

        norms_payload: list[dict] = []
        for ns in norms:
            norm_id = _sha1(f"{doc_id}:{ns.norm_number}")
            reqs_payload = [{"kind": r.kind, "text": r.text, "hash": r.hash} for r in ns.requirements]
            norms_payload.append(
                {
                    "id": norm_id,
                    "number": ns.norm_number,
                    "text": ns.norm_text,
                    "workscope": ns.workscope,
                    "requirements": reqs_payload,
                }
            )

            if ns.workscope:
                report.setdefault(ns.workscope, {})
                for r in ns.requirements:
                    report[ns.workscope].setdefault(r.kind, set()).add(r.text)

        upsert_npa_document(
            cfg,
            doc_id=doc_id,
            source="npa",
            path=doc_path,
            title=title,
            original_path=orig,
            original_filename=(orig.replace("\\", "/").split("/")[-1] if orig else None),
            norms=norms_payload,
        )
        print(f"[NPA] {f.name}: saved norms={len(norms_payload)}", flush=True)

    out_md = Path(args.out_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("# NPA: WorkScope и требования (все найденные .rtf)")
    lines.append("")
    lines.append("Алгоритм: для каждого сегмента вида `N. ...` извлекаем WorkScope и класифицируем требования (education/experience/training/permit) регулярками.")
    lines.append("")

    for workscope in sorted(report.keys(), key=lambda s: s.lower()):
        lines.append(f"## WorkScope: {workscope}")
        for kind in ["education", "experience", "training", "permit"]:
            if kind in report[workscope]:
                texts = sorted(report[workscope][kind])
                lines.append("")
                lines.append(f"### {kind}")
                for t in texts[:6]:
                    lines.append(f"- {t}")
                if len(texts) > 6:
                    lines.append(f"- ... и ещё {len(texts) - 6}")
        lines.append("")

    out_md.write_text("\n".join(lines), encoding="utf-8")
    print(f"[NPA] report written: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


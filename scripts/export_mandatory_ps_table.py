from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# Repo root on sys.path before any `from app...` (works from /workspace when cwd is scripts/)
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from neo4j import GraphDatabase

from app.industry_cpa import industry_from_vpd_code


def _get_env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v else default


_RANK_RE = re.compile(
    r"^(?P<prefix>.+?)\s+"
    r"(?P<rank>\d+)\s*"
    r"(?:[-–‑]?\s*(?:го|й|й-го))?\s+"
    r"разряд[а-я]*$",
    flags=re.IGNORECASE,
)


def _compress_int_ranges(values: list[int]) -> str:
    """
    [1,2,3,5,7,8] -> "1-3, 5, 7-8"
    """
    if not values:
        return ""
    vals = sorted(set(values))
    ranges: list[tuple[int, int]] = []
    start = prev = vals[0]
    for v in vals[1:]:
        if v == prev + 1:
            prev = v
            continue
        ranges.append((start, prev))
        start = prev = v
    ranges.append((start, prev))

    parts: list[str] = []
    for a, b in ranges:
        parts.append(f"{a}-{b}" if a != b else str(a))
    return ", ".join(parts)


def _group_titles_by_rank(items: list[str]) -> list[str]:
    """
    Group items like:
      "Помощник ... 1 разряда", "Помощник ... 2-го разряда" -> "Помощник ... 1-2 разряда"
    Keeps non-matching items untouched and preserves first-seen order for grouped prefixes.
    """
    if not items:
        return []

    prefix_to_ranks: dict[str, list[int]] = {}
    prefix_first_idx: dict[str, int] = {}

    for i, raw in enumerate(items):
        s = " ".join((raw or "").split())
        if not s:
            continue
        m = _RANK_RE.match(s)
        if not m:
            continue
        prefix = " ".join(m.group("prefix").split())
        try:
            rank = int(m.group("rank"))
        except Exception:
            continue
        prefix_to_ranks.setdefault(prefix, []).append(rank)
        prefix_first_idx.setdefault(prefix, i)

    if not prefix_to_ranks:
        return items

    emitted: set[str] = set()
    out: list[str] = []
    for i, raw in enumerate(items):
        s = " ".join((raw or "").split())
        if not s:
            continue
        m = _RANK_RE.match(s)
        if not m:
            out.append(s)
            continue

        prefix = " ".join(m.group("prefix").split())
        if prefix not in prefix_to_ranks:
            out.append(s)
            continue

        # Emit only once, at first occurrence.
        if prefix in emitted:
            continue
        if prefix_first_idx.get(prefix, i) != i:
            continue

        ranks = prefix_to_ranks.get(prefix) or []
        compressed = _compress_int_ranges(ranks)
        if compressed and ("," in compressed or "-" in compressed):
            out.append(f"{prefix} {compressed} разряда")
        else:
            # Single rank: keep the original string (preserve wording like "6-го разряда")
            out.append(s)
        emitted.add(prefix)

    return out


def _extract_ps_meta(ps_text: str) -> dict[str, str]:
    # Name + codes (best-effort, different PS templates exist).
    # We track:
    # - general_code: code from "Общие сведения" section (often like 19.071) — this is what user wants as "Код ПС"
    # - reg_number: registration number (3-6 digits), often shown near the PS title
    marker = "ПРОФЕССИОНАЛЬНЫЙ СТАНДАРТ"
    idx = ps_text.find(marker)
    reg_number = ""
    general_code = ""
    name = ""
    order = ""
    if idx != -1:
        after = ps_text[idx + len(marker) : idx + len(marker) + 800]
        mcode = re.search(r"\b(\d{3,6})\b", after)
        if mcode:
            reg_number = mcode.group(1)
            name = after[: mcode.start()].strip()
            # clean name from extra tokens/newlines
            name = " ".join(name.split())

    # Alternative: explicit "Регистрационный номер" lines
    if not reg_number:
        m = re.search(r"регистрационн\w*\s+номер\w*\s*[:\-]?\s*(\d{3,6})\b", ps_text, flags=re.IGNORECASE)
        if m:
            reg_number = m.group(1)

    # Alternative: "Рег. № 1426" / "№ 1426" near "профессиональный стандарт"
    if not reg_number:
        m = re.search(r"профессиональн\w*\s+стандарт[^\n]{0,200}?№\s*(\d{3,6})\b", ps_text, flags=re.IGNORECASE)
        if m:
            reg_number = m.group(1)

    if not name:
        # Try to capture a common variant: "Профессиональный стандарт <name>" in one line
        m = re.search(r"профессиональн\w*\s+стандарт\s+([^\n]{10,220})", ps_text, flags=re.IGNORECASE)
        if m:
            cand = " ".join(m.group(1).split())
            # strip trailing numbers/sections
            cand = re.sub(r"\s+\d{3,6}\b.*$", "", cand).strip()
            name = cand

    # General info code from "ОБЩИЕ СВЕДЕНИЯ" block (typical: 19.071)
    mgeneral = None
    mblock = re.search(r"ОБЩИЕ\s+СВЕДЕНИЯ(.{0,2000})", ps_text, flags=re.IGNORECASE | re.DOTALL)
    if mblock:
        block = mblock.group(1)
        # Prefer explicit "Код" label
        mgeneral = re.search(r"\bкод\b\s*[:\-]?\s*(\d{2}\.\d{3})\b", block, flags=re.IGNORECASE)
        if not mgeneral:
            mgeneral = re.search(r"\b(\d{2}\.\d{3})\b", block)
    if not mgeneral:
        # Fallback: anywhere in text (avoid grabbing numbers from unrelated lists by requiring dot format)
        mgeneral = re.search(r"\b(\d{2}\.\d{3})\b", ps_text)
    if mgeneral:
        general_code = mgeneral.group(1)

    morder = re.search(r"от «(\d{1,2})»\s*([а-яё]+)\s*(\d{4}) г\.?\s*№\s*(\d+н|\d+)", ps_text, re.IGNORECASE)
    if morder:
        day, month, year, num = morder.groups()
        # Normalize month casing but keep original word
        order = f"от «{day}» {month} {year} г. № {num}"

    return {
        # What we show in table as "Код ПС": prefer general info code; fallback to registration number.
        "code_ps": general_code or reg_number,
        "code_ps_general": general_code,
        "reg_number": reg_number,
        "name_ps": name,
        "order_mintrud": order,
    }


# Начало раздела I: не ловим одиночную строку «I» из списков (ОТФ A…I и т.п.).
_SECTION_I_START = re.compile(
    r"(?:^|\r?\n)\s*(?:"
    r"I(?:[\.)]|\s+(?=ОБЩИЕ|Общие|общие))"
    r"|1(?:[\.)]|\s+)(?=ОБЩИЕ|Общие|общие)"
    r"|Раздел\s+I(?:[\.)]|\s+(?=ОБЩИЕ|Общие|общие))"
    r")",
    re.IGNORECASE | re.MULTILINE,
)


def _slice_section_i(ps_text: str) -> str:
    """Фрагмент текста ПС от строки с разделом I до начала II (не включая II)."""
    t = ps_text or ""
    m = _SECTION_I_START.search(t)
    if not m:
        return ""
    start = m.start()
    after = t[m.end() :]
    m2 = re.search(r"(?:^|\r?\n)\s*II[\s.)]", after, re.IGNORECASE | re.MULTILINE)
    if m2:
        return t[start : m.end() + m2.start()]
    return t[start : m.end() + 12000]


_VPD_PATTERNS = (
    # Таблицы из docx часто дают «... | Вид ... | 19.071 ...»
    r"вид\s+профессиональн\w*\s+деятельности\s*[|:\-–]?\s*([^\n\r]{1,700})",
    r"вид\s+профессиональн\w*\s+деятельности\s*\r?\n\s*([^\n\r]{1,700})",
    # Сокращённо в шапках
    r"вид\s+проф\.?\s*деятельности\s*[|:\-–]?\s*([^\n\r]{1,700})",
)

# Код NN.NNN на строке, ниже подпись (наименование вида проф. деятельности) — типичный docx Минтруда
_VPD_CODE_THEN_CAPTION = re.compile(
    r"(?P<code>\d{2}\.\d{3})\s*\r?\n\s*[\(（]?\s*наименование\s+вида\s+профессиональн\w*\s+деятельности",
    re.IGNORECASE | re.MULTILINE,
)


def _vpd_from_code_caption_block(block: str, m: re.Match[str]) -> tuple[str, str | None]:
    code = (m.group("code") or "").strip()
    before = block[max(0, m.start() - 3500) : m.start()]
    lines = [x.strip() for x in before.splitlines() if x.strip()]
    desc = ""
    for ln in reversed(lines[-25:]):
        if len(ln) < 12:
            continue
        if re.match(r"^(I\.|II\.|III\.|IV\.|V\.|VI\.|код|наименование|группа|уровень)\b", ln, re.I):
            continue
        if re.match(r"^\d{3,5}$", ln):
            continue
        if re.search(r"^\d+\.\d+\s+", ln):  # 3.1. ОТФ…
            continue
        desc = ln
        break
    # В ячейке «Вид…» код не показываем (пользователю нужен только текст).
    raw = " ".join((desc or "").split())
    if len(raw) > 800:
        raw = raw[:800] + "..."
    return (raw, code)


def _parse_vpd_raw_from_block(block: str) -> tuple[str, str | None]:
    raw = ""
    for pat in _VPD_PATTERNS:
        m = re.search(pat, block, re.IGNORECASE)
        if m:
            raw = (m.group(1) or "").strip()
            break
    if not raw:
        m_cc = _VPD_CODE_THEN_CAPTION.search(block)
        if m_cc:
            return _vpd_from_code_caption_block(block, m_cc)
        return "", None
    low = raw.lower()
    for sep in ("\nкод", "\nнаименование", "\nуровень", "\nпрофесси", "\nрегистрацион"):
        j = low.find(sep)
        if j > 8:
            raw = raw[:j].strip()
            low = raw.lower()
    raw = " ".join(raw.split())
    if len(raw) > 800:
        raw = raw[:800] + "..."
    mc = re.search(r"\b(\d{2}\.\d{3})\b", raw)
    code = mc.group(1) if mc else None
    # Убираем ведущий код NN.NNN из ячейки «Вид…»
    raw_no_code = re.sub(r"^\s*\d{2}\.\d{3}\s*", "", raw).strip()
    return (raw_no_code or raw, code)


def _extract_vpd_field_section_i(ps_text: str) -> tuple[str, str | None]:
    """
    Из раздела I: значение поля «Вид профессиональной деятельности» (как в документе)
    и код NN.NNN при наличии.

    В docx порядок абзацев часто такой, что «Общие сведения» оказываются после десятков
    страниц основного текста — не ограничиваемся первыми 50k символов. Срез I→II
    иногда получается коротким ложным (оглавление «1. ОБЩИЕ … / II.») — такие срезы
    не используем, если в них нет подписи «Вид…».
    """
    t = ps_text or ""
    blocks: list[str] = []

    # Сначала последние вхождения «I. Общие сведения» — первое часто оглавление, дальше — сам раздел
    for m in reversed(list(re.finditer(r"(?:^|\r?\n)\s*I\.\s*Общие\s+сведения\s*", t, re.MULTILINE | re.IGNORECASE))):
        blocks.append(t[m.start() : m.start() + 18000])
        if len(blocks) >= 3:
            break

    m_os = re.search(r"ОБЩИЕ\s+СВЕДЕНИЯ", t, re.IGNORECASE)
    if m_os:
        blocks.append(t[m_os.start() : m_os.start() + 16000])

    si = _slice_section_i(t)
    if (si or "").strip() and len(si) >= 200 and "вид" in si.lower():
        blocks.append(si)
    elif (si or "").strip() and len(si) >= 800:
        blocks.append(si)

    blocks.append(t[:80000])
    if len(t) > 80000:
        blocks.append(t)

    seen: set[str] = set()
    for block in blocks:
        if not (block or "").strip() or block in seen:
            continue
        seen.add(block)
        raw, code = _parse_vpd_raw_from_block(block)
        if raw:
            return (raw, code)
    return "—", None


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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Export mandatory PS markdown table from Neo4j")
    ap.add_argument(
        "--rephrase-with-llm",
        action="store_true",
        help="Use LLM to rephrase norm snippets in the table (OPENAI_BASE_URL + OPENAI_MODEL)",
    )
    ap.add_argument("--llm-max-calls", type=int, default=50, help="Max LLM calls for rephrasing (safety)")
    if argv is None:
        argv = sys.argv[1:]
    args = ap.parse_args(argv)

    try_rephrase_fn = None
    if args.rephrase_with_llm:
        from app.table_llm_rephrase import try_rephrase_table_snippet as try_rephrase_fn

    # Query NPA in graph via "proper" matching edges Norm->OTF.
    neo4j_uri = _get_env("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = _get_env("NEO4J_USER", "neo4j")
    neo4j_password = _get_env("NEO4J_PASSWORD", "neo4j_password")
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def clean_cell(v: object) -> str:
        return " ".join(str(v or "").split())

    llm_budget = max(0, int(args.llm_max_calls)) if args.rephrase_with_llm and try_rephrase_fn else 0

    try:
        # Load all PS documents (multi-PS)
        with driver.session() as s:
            ps_docs = list(
                s.run(
                    """
                    MATCH (ps:Document {source:'profstandard'})
                    RETURN ps.id AS id, ps.path AS path, ps.raw_text AS raw_text, ps.ps_general_code AS ps_general_code
                    ORDER BY ps.updated_at DESC
                    """
                )
            )
            if not ps_docs:
                raise SystemExit("No profstandard documents in Neo4j. Run PS ingest first.")

            out_lines: list[str] = []
            out_lines.append("# Список обязательных профессиональных стандартов (черновик под текущие данные)")
            out_lines.append("")
            col_gazprom = (
                "Наименование профессии (должности) дочернего общества, организации и филиала ПАО «Газпром», "
                "в отношении которой установлена обязательность применения профессиональных стандартов "
                "(переформулирование нейросетью при включённом режиме)"
            )
            col_norm_raw = "Исходный фрагмент пункта НПА (без нейросети, для сравнения)"
            hdr = (
                f"| № п/п | Наименование профессионального стандарта | Код ПС | "
                f"Наименование и реквизиты документа, утвердившего профессиональный стандарт | {col_gazprom} | {col_norm_raw} | "
                "Нормативный правовой акт Российской Федерации, устанавливающий требования к квалификации работников "
                "(пункт, часть, статья, раздел) | Код ОТФ | "
                "Возможное наименование профессии (должности) в соответствии с профессиональным стандартом | "
                "Вид профессиональной деятельности (из раздела I ПС) | "
                "Отрасль профессиональной деятельности |"
            )
            out_lines.append(hdr)
            out_lines.append("|" + "|".join(["---"] * 11) + "|")

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
                # Prefer LLM-extracted code stored on Document, if present
                doc_code = ps.get("ps_general_code") if isinstance(ps, dict) else None

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

                # If graph doesn't have OTF/Role layer (older or simplified pipeline),
                # fallback to professions mentioned in the PS document.
                if not role_names:
                    profs = [
                        clean_cell(r["name"])
                        for r in s.run(
                            "MATCH (p:Profession)-[:MENTIONED_IN]->(ps:Document {id:$ps_id}) RETURN DISTINCT p.name AS name ORDER BY name",
                            ps_id=ps_id,
                        )
                        if r.get("name") and clean_cell(r.get("name"))
                    ]
                    role_names = profs

                role_names = _group_titles_by_rank(role_names)
                roles_cell = "<br>".join(role_names[:25]) + ("<br>..." if len(role_names) > 25 else "")

                name_ps_cell = clean_cell(ps_meta["name_ps"] or Path(ps.get("path") or "").stem)
                code_ps_cell = clean_cell(doc_code or ps_meta["code_ps"])
                order_cell_s = clean_cell(ps_meta["order_mintrud"])
                vpd_raw, vpd_code_i = _extract_vpd_field_section_i(ps_text)
                view_activity_cell = clean_cell(vpd_raw) if vpd_raw and vpd_raw != "—" else "—"
                industry_cell = industry_from_vpd_code(vpd_code_i)

                applied_raw_cell = (norm_text[:500] + ("..." if len(norm_text) > 500 else "")) if norm_text else "—"
                applied_to_cell = applied_raw_cell
                if llm_budget > 0 and try_rephrase_fn and norm_text and norm_text != "—":
                    plain = " ".join(str(norm_text).split())
                    if len(plain) >= 40:
                        try:
                            r = try_rephrase_fn(plain)
                            if r:
                                applied_to_cell = r
                                llm_budget -= 1
                        except Exception:
                            pass

                npa_cell = clean_cell(f"{npa_title} (пункт {norm_number})").strip() if npa_title or norm_number else "—"
                otf_cell = clean_cell(", ".join(matched_otf_codes)) if matched_otf_codes else "—"

                approving_parts: list[str] = []
                if order_cell_s:
                    approving_parts.append(order_cell_s)
                reg_n = clean_cell(ps_meta.get("reg_number") or "")
                if reg_n:
                    approving_parts.append(f"Регистрационный номер {reg_n}")
                approving_doc_cell = "<br>".join(approving_parts) if approving_parts else "—"

                # De-dup: same PS can be ingested from multiple files/representations.
                key = (code_ps_cell or "").strip() or f"{name_ps_cell}::{order_cell_s}".strip()
                if not hasattr(main, "_seen_keys"):
                    setattr(main, "_seen_keys", set())
                seen: set[str] = getattr(main, "_seen_keys")
                if key in seen:
                    continue
                seen.add(key)

                out_lines.append(
                    f"| {len(seen)} | {name_ps_cell} | {code_ps_cell} | {approving_doc_cell} | {applied_to_cell} | "
                    f"{applied_raw_cell} | {npa_cell} | {otf_cell} | {roles_cell} | {view_activity_cell} | "
                    f"{industry_cell} |"
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


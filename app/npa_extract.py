from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class NpaRequirement:
    kind: str  # education | experience | training | permit
    text: str
    hash: str


@dataclass(frozen=True)
class NormSegment:
    doc_title: str
    doc_path: str
    norm_number: str  # e.g. "200"
    norm_text: str
    workscope: str | None
    requirements: list[NpaRequirement]


WORKSCOPE_PATTERNS = [
    # "по бурению, освоению, ремонту ... допускаются лица ..."
    re.compile(r"по\s+(.+?)\s*,?\s*допускаются\s+лица", flags=re.IGNORECASE | re.DOTALL),
    re.compile(r"по\s+(.+?)\s*,?\s*должны\s+иметь", flags=re.IGNORECASE | re.DOTALL),
    # "к ... работам по ... допускаются лица ..."
    re.compile(r"работам\s+по\s+(.+?)\s*,?\s*допускаются\s+лица", flags=re.IGNORECASE | re.DOTALL),
    # "Работники ... осуществляющие ... выполнение работ по ... должны ..."
    re.compile(
        r"работник\w+.*?выполн\w+\s+работ\s+по\s+(.+?)\s*(?:,|\.)?\s*(?:должн\w+|раз\s+в|дополнительно)",
        flags=re.IGNORECASE | re.DOTALL,
    ),
    # "Работники ... осуществляющие ... по ... должны ..."
    re.compile(
        r"работник\w+.*?по\s+(.+?)\s*(?:,|\.)?\s*(?:должн\w+|раз\s+в|дополнительно)",
        flags=re.IGNORECASE | re.DOTALL,
    ),
]


def _extract_workscope(norm_text: str) -> str | None:
    t = norm_text.strip()
    for rx in WORKSCOPE_PATTERNS:
        m = rx.search(t)
        if m:
            scope = m.group(1).strip()
            # cut trailing phrases that are usually not part of scope
            scope = re.split(r"\s*(?:допускаются|должны)\s+лица", scope, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            # keep it reasonable
            scope = scope.replace("\n", " ").strip()
            return scope[:300]
    # fallback: a shortened excerpt
    return None


def _requirement_clause(text: str) -> dict[str, str]:
    """
    Extract a single best-effort clause for education/experience/training/permit.
    We keep it regex-based to avoid overreliance on LLM.
    """
    lower = text.lower()

    education = None
    experience = None
    training = None
    permit = None

    # education / training in Russian prof standards
    # education: "... допускаются лица, имеющие профессиональное образование ..."
    m = re.search(
        r"(профессионал\w+\s+образован\w+[^.]{0,300}?\.)", text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        education = " ".join(m.group(1).split())

    # also catch "среднее профессиональное образование ..." phrases
    m = re.search(
        r"(средн\w* профессиональн\w+ образован\w+[^.]{0,500}?\.)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        education = " ".join(m.group(1).split())

    # experience: "Не менее одного года по профессии ..."
    m = re.search(
        r"(не\s+менее\s+[^.]{0,180}?\s+(год|лет)\s+[^.]{0,220}?\.)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        experience = " ".join(m.group(1).split())

    # experience: "опыт ... не менее ..."
    m = re.search(
        r"(опыт\s+[^.]{0,240}?\s+не\s+менее\s+[^.]{0,200}?\.)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m and not experience:
        experience = " ".join(m.group(1).split())

    # training / qualification improvement
    m = re.search(
        r"(должн\w+\s+пройти\s+обучен\w+[^.]{0,260}?\.)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        training = " ".join(m.group(1).split())

    m = re.search(
        r"(должн\w+\s+получить\s+[^.]{0,260}?\bквалификац\w+[^.]{0,260}?\.)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m and not training:
        training = " ".join(m.group(1).split())

    # permit-like: "аттестованные ...", "проверку знаний", "допуск к работе"
    m = re.search(
        r"(аттестован\w+\s+[^.]{0,260}?\.)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        permit = " ".join(m.group(1).split())

    m = re.search(
        r"(проход[иі]т\w*\s+[^.]{0,260}?\bпровер\w+\s+знани\w+[^.]{0,260}?\.)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m and not permit:
        permit = " ".join(m.group(1).split())

    return {
        "education": education,
        "experience": experience,
        "training": training,
        "permit": permit,
    }


def extract_norm_segments(doc_title: str, doc_path: str, text: str) -> list[NormSegment]:
    """
    Split by patterns like:
      200. ...
      201. ...
    and return only segments that likely contain qualification requirements.
    """
    # Normalize whitespace a bit for regex stability
    t = re.sub(r"[ \t]+", " ", text)
    t = t.replace("\r", "\n")

    # Find candidates "N. " at line starts
    rx = re.compile(r"(?m)^\s*(\d{1,4})\.\s+(.*?)(?=^\s*\d{1,4}\.\s+|\Z)", flags=re.DOTALL)
    segs: list[NormSegment] = []
    for m in rx.finditer(t):
        num = m.group(1)
        seg_text = m.group(2).strip()
        if not seg_text:
            continue
        low = seg_text.lower()
        # heuristic to keep only parts that mention qualification / admission
        if not (
            "допуска" in low
            or "требован" in low
            or "образован" in low
            or "профессиональн" in low
            or "обучен" in low
            or "квалифик" in low
            or "опыт" in low
            or "аттест" in low
            or "провер" in low
        ):
            continue

        scope = _extract_workscope(seg_text)
        reqs = _requirement_clause(seg_text)

        extracted: list[NpaRequirement] = []
        for kind in ["education", "experience", "training", "permit"]:
            txt = reqs.get(kind)
            if txt:
                # include kind in hash to avoid cross-kind merge collisions
                h = _sha1(f"{kind}:{txt}")
                extracted.append(NpaRequirement(kind=kind, text=txt, hash=h))

        # keep only segments where we found at least one requirement
        if extracted:
            segs.append(
                NormSegment(
                    doc_title=doc_title,
                    doc_path=doc_path,
                    norm_number=num,
                    norm_text=seg_text[:6000],
                    workscope=scope,
                    requirements=extracted,
                )
            )
    return segs


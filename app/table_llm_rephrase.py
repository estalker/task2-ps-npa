from __future__ import annotations

import json
import os
import urllib.request
from urllib.parse import urlparse


PROMPT = """Ты редактор служебной таблицы. Дан текст нормативного пункта (НПА), обычно в стиле:
«К … работам … допускаются лица, имеющие … образование … и аттестованные …».

Нужна ОДНА строка для ячейки таблицы «к каким профессиям/должностям относится пункт» — в фокусе перечень видов работ/деятельности, а НЕ требования к образованию и аттестации в конце.

Целевой формат (пример смысла, подставь виды работ из исходного текста):
«Профессии (должности) работников, осуществляющих руководство и ведение работ по бурению, освоению, ремонту, …»
или, если в тексте только «ведение» без «руководство»:
«Профессии (должности) работников, осуществляющих ведение работ по …»

Правила:
- Возьми из исходного текста блок с перечислением видов работ (бурение, освоение, ремонт скважин, геофизика, добыча нефти и газа и т.п.) и вставь его после «по …» / «работ по …», сохраняя терминологию НПА.
- Убери хвост про «допускаются лица», «имеющие профессиональное образование», «аттестованные», «соответствующее занимаемой должности» и аналогичные квалификационные условия — они не входят в эту ячейку.
- Не выдумывай виды работ, которых нет в тексте. Не добавляй вводных («данный пункт», «согласно тексту»).
- Одно законченное предложение; в конце — точка.
- Если текст уже почти в нужном виде — приведи к шаблону выше, не дублируя префикс дважды.

Верни строго один JSON-объект:
{{"rephrased": string}}

Текст пункта:
<<<{TEXT}>>>
"""


def _extract_first_json_object(s: str) -> str:
    s = s.strip()
    start = s.find("{")
    if start == -1:
        raise ValueError("no json object start")
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                return s[start : i + 1]
    raise ValueError("unterminated json object")


def _ollama_generate(prompt: str, *, model: str, timeout_s: int, base_url: str) -> str:
    parsed = urlparse(base_url)
    if parsed.scheme and parsed.netloc:
        root = f"{parsed.scheme}://{parsed.netloc}"
    else:
        root = base_url.replace("/v1", "").rstrip("/")
        if not root.startswith(("http://", "https://")):
            root = "http://" + root
    url = root.rstrip("/") + "/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.1, "num_predict": 768},
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        body = resp.read()
    out = json.loads(body)
    return (out.get("response") or "").strip()


def try_rephrase_table_snippet(text: str, *, max_in: int = 6000) -> str | None:
    """
    Turn norm paragraph into table cell: «Профессии (должности) работников, осуществляющих …».
    Returns None if LLM unavailable or on failure.
    """
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    base_url = os.getenv("OPENAI_BASE_URL", "").strip()
    if not api_key and not base_url:
        return None
    if not base_url:
        return None

    raw_in = " ".join((text or "").split())
    if not raw_in:
        return None

    model = os.getenv("OPENAI_MODEL", "qwen2.5:3b-instruct").strip() or "qwen2.5:3b-instruct"
    timeout_s = int(os.getenv("OLLAMA_TIMEOUT_S", "120") or "120")

    raw = _ollama_generate(PROMPT.format(TEXT=raw_in[:max_in]), model=model, timeout_s=timeout_s, base_url=base_url)
    try:
        data = json.loads(raw)
    except Exception:
        data = json.loads(_extract_first_json_object(raw))
    if not isinstance(data, dict):
        return None
    out = data.get("rephrased")
    if not isinstance(out, str):
        return None
    out = " ".join(out.split()).strip()
    if not out:
        return None
    return out[:2000]

from __future__ import annotations

import json
import os
import urllib.request
from urllib.parse import urlparse


PROMPT = """Ты редактор служебной таблицы. Дан текст нормативного пункта (НПА), часто в стиле:
«К … работам … допускаются лица, имеющие … образование … и аттестованные …».

Нужна ОДНА строка для ячейки «к каким профессиям/должностям относится пункт»: кто и какую деятельность ведёт, БЕЗ хвоста про допуск, образование и аттестацию.

Смысл ячейки целиком формируешь **ты** (модель) по тексту НПА; никаких шаблонов или подстановок «снаружи» нет — только твой ответ в JSON.

Заголовок ячейки (переформулируй по смыслу, не копируй дословно без нужды):
- В начале: «Профессии (должности) работников, …» — дальше опиши, **какую деятельность** они осуществляют по отношению к перечисленным работам.
- Если пункт **вводит несколько различимых пластов деятельности** (два, три и больше — например сочетание направления/организации, непосредственного выполнения, контроля, сопровождения и т.д.; в нормах это может быть цепочка «к …, … и … работ», несколько отглагольных конструкций или перечисление ролей до общего списка работ), **сохрани в ответе столько же смысловых слоёв**, сколько реально заложено в исходнике. Не схлопывай их в одно обобщение, если норма их разводит. Формулировки пластов можно назвать иначе, главное — **различие ролей/видов деятельности** остаётся ясным для читателя.
- Если по смыслу в исходнике **один** пласт — не придумывай дополнительные.

Перечень видов работ после «по» / в объектной части:
- Сохрани состав и порядок видов работ из НПА, формулировки по возможности ближе к источнику; падеж приведи к связке с предлогом «по» (дательный/винительный — как принято в таких таблицах).
- Не добавляй виды работ, которых нет в тексте.

Убери только квалификационный хвост: «допускаются лица», требования к образованию, аттестации, «соответствующее занимаемой должности» и т.п.

Одно законченное предложение, точка в конце. Без вводных («данный пункт», «согласно тексту»). Не дублируй префикс «Профессии (должности)…» дважды.

Верни строго один JSON-объект:
{{"rephrased": string}}

Текст пункта:
<<<{TEXT}>>>
"""

_PREFIX = "Профессии (должности) работников, осуществляющих"


def _normalize_prefix(s: str) -> str:
    s = " ".join((s or "").split()).strip()
    if not s:
        return s

    low = s.lower()
    pref_low = _PREFIX.lower()
    if low.startswith(pref_low):
        return s

    # If model started with близким заголовком, нормализуем его к требуемому виду.
    head = "Профессии (должности) работников,"
    if low.startswith(head.lower()):
        tail = s.split(",", 1)[1].strip() if "," in s else ""
        if tail.lower().startswith("осуществляющих"):
            return f"{head} {tail}"
        if tail:
            return f"{_PREFIX} {tail}"
        return _PREFIX

    return f"{_PREFIX} {s}"


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
        # temperature 0: одинаковый вход НПА → одинаковая ячейка при повторных строках (19.005 / 19.071 и т.д.)
        "options": {"temperature": 0, "num_predict": 768},
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
    Один вызов LLM (Ollama): пункт НПА → ячейка «Профессии (должности) работников, …».
    Число смысловых пластов деятельности (2, 3, …) определяет и переносит только модель по промпту.
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
    out = _normalize_prefix(out)
    return out[:2000]

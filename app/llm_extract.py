from __future__ import annotations

import json
import os
import urllib.request
from urllib.parse import urlparse

from .schema import Extraction


PROMPT = """Ты извлекаешь факты ТОЛЬКО из предоставленного фрагмента профстандарта/НПА.
Никаких знаний “из головы”. Если в тексте нет — верни null.

Нужно извлечь только:
- profession: string|null (наименование профессии/должности/ОТФ/квалификации, если в тексте так оформлено)
- qualification: string|null (уровень/квалификация/ОТФ/код/наименование квалификации — если явно указано)
- education: string|null (требования к образованию/обучению — если явно указано)
- experience: string|null (требования к опыту практической работы — если явно указано)

Верни строго один JSON-объект без пояснений и без markdown:
{{
  "profession": string|null,
  "qualification": string|null,
  "education": string|null,
  "experience": string|null
}}

Текст:
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


def try_extract_with_llm(text: str) -> Extraction | None:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    base_url = os.getenv("OPENAI_BASE_URL", "").strip()

    # If user didn't configure any LLM at all — skip.
    # For many local OpenAI-compatible servers, any non-empty key works.
    if not api_key and not base_url:
        return None

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    timeout_s = int(os.getenv("OLLAMA_TIMEOUT_S", "120") or "120")

    # Ollama is easiest/fastest via native API: /api/generate
    # Allow any reachable base URL (e.g. http://host.docker.internal:11434/v1 from Docker).
    if base_url:
        parsed = urlparse(base_url)
        if parsed.scheme and parsed.netloc:
            root = f"{parsed.scheme}://{parsed.netloc}"
        else:
            # fallback: accept raw host:port or http://host:port without /v1
            root = base_url.replace("/v1", "").rstrip("/")
            if not root.startswith(("http://", "https://")):
                root = "http://" + root
        url = root.rstrip("/") + "/api/generate"
        payload = {
            "model": model,
            "prompt": PROMPT.format(TEXT=text[:20000]),
            "stream": False,
            "format": "json",
            # Keep it small but allow longer requirement phrases.
            "options": {"temperature": 0, "num_predict": 220},
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
        raw = (out.get("response") or "").strip()
        # Ollama may return either a pure JSON object or extra text around it.
        try:
            data = json.loads(raw)
        except Exception:
            data = json.loads(_extract_first_json_object(raw))
        return Extraction.model_validate(data)

    # For this prototype, we only support Ollama usage to keep it simple.
    return None


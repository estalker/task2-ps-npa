from __future__ import annotations

import json
import os
import urllib.request
from typing import Any, Literal, TypedDict
from urllib.parse import urlparse


Kind = Literal["education", "experience", "training", "permit"]


class RequirementItem(TypedDict):
    kind: Kind
    text: str


class NpaLlmResult(TypedDict, total=False):
    workscope: str | None
    requirements: list[RequirementItem]


PROMPT = """Ты извлекаешь факты ТОЛЬКО из предоставленного фрагмента НПА.
Никаких знаний “из головы”. Если в тексте нет — верни null/пустые значения.

Задача: извлечь
- workscope: string|null — какие работы/категории работ/на кого распространяется пункт (кратко, без воды)
- requirements: array — требования к квалификации/допускам в этом пункте

В requirements допускаются только типы:
- education (образование / профессиональное обучение)
- experience (опыт работы)
- training (обучение/получение квалификации/проверка знаний, если это квалификационное требование)
- permit (аттестация/допуск/разрешение, если прямо указано)

Верни строго один JSON-объект без пояснений и без markdown:
{{
  "workscope": string|null,
  "requirements": [
    {{"kind":"education"|"experience"|"training"|"permit", "text": string}}
  ]
}}

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
        "options": {"temperature": 0, "num_predict": 180},
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


def try_extract_npa_with_llm(text: str) -> NpaLlmResult | None:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    base_url = os.getenv("OPENAI_BASE_URL", "").strip()

    # If user didn't configure any LLM at all — skip.
    if not api_key and not base_url:
        return None

    model = os.getenv("OPENAI_MODEL", "qwen2.5:3b-instruct").strip() or "qwen2.5:3b-instruct"
    timeout_s = int(os.getenv("OLLAMA_TIMEOUT_S", "120") or "120")

    # Only support Ollama in this prototype.
    if not base_url:
        return None

    # Keep context small to avoid slow local inference on long norms.
    raw = _ollama_generate(PROMPT.format(TEXT=text[:2500]), model=model, timeout_s=timeout_s, base_url=base_url)

    # Ollama may return either pure JSON or extra text around it.
    try:
        data: Any = json.loads(raw)
    except Exception:
        data = json.loads(_extract_first_json_object(raw))

    if not isinstance(data, dict):
        return None

    workscope = data.get("workscope", None)
    if isinstance(workscope, str):
        workscope = workscope.strip() or None
    else:
        workscope = None

    reqs_in = data.get("requirements", []) or []
    reqs: list[RequirementItem] = []
    if isinstance(reqs_in, list):
        for item in reqs_in:
            if not isinstance(item, dict):
                continue
            kind = item.get("kind")
            text_v = item.get("text")
            if kind not in ("education", "experience", "training", "permit"):
                continue
            if not isinstance(text_v, str):
                continue
            t = " ".join(text_v.strip().split())
            if not t:
                continue
            reqs.append({"kind": kind, "text": t[:600]})

    # Normalize: if both empty, skip
    if workscope is None and not reqs:
        return None

    return {"workscope": workscope, "requirements": reqs}


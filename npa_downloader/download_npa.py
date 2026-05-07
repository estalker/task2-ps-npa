from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import requests
from requests import Response
from bs4 import BeautifulSoup
from charset_normalizer import from_bytes


GOOGLE_SEARCH_URL = "https://www.google.com/search"
YANDEX_SEARCH_URL = "https://yandex.ru/search/"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_filename(name: str, max_len: int = 160) -> str:
    name = name.strip()
    name = re.sub(r"[<>:\"/\\\\|?*\x00-\x1F]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    if not name:
        return "download"
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def load_completed_soft_queries(report_path: Path) -> set[str]:
    """
    Возвращает set sha1(query_soft), которые уже успешно скачаны (ok=true).
    """
    done: set[str] = set()
    if not report_path.exists():
        return done
    try:
        for line in report_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("ok") is True and isinstance(rec.get("query_soft"), str):
                done.add(_sha1(rec["query_soft"]))
    except Exception:
        return done
    return done


def load_last_records(report_path: Path) -> dict[str, dict]:
    """
    query_soft -> last json record (the latest occurrence in file).
    """
    last: dict[str, dict] = {}
    if not report_path.exists():
        return last
    for line in report_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:
            continue
        qs = rec.get("query_soft")
        if isinstance(qs, str) and qs:
            last[qs] = rec
    return last


def iter_retry_queries(report_path: Path, *, mode: str) -> Iterable[str]:
    """
    mode:
      - failed: only last records with ok=false and error contains 'download_failed'
      - ssl: subset of failed where error contains SSL/cert issues
      - all_failed: last records with ok=false (includes not_found)
    Returns query_raw strings (preferred) to re-run.
    """
    mode = (mode or "failed").strip().lower()
    last = load_last_records(report_path)
    for qs, rec in last.items():
        if rec.get("ok") is True:
            continue
        err = str(rec.get("error") or "")
        err_l = err.lower()
        if mode == "failed":
            if "download_failed" not in err:
                continue
        elif mode == "ssl":
            if ("ssl" not in err_l) and ("certificate" not in err_l):
                continue
        elif mode == "all_failed":
            pass
        else:
            if "download_failed" not in err:
                continue

        raw = rec.get("query_raw")
        yield raw if isinstance(raw, str) and raw.strip() else qs


def soften_query(raw: str) -> str:
    """
    Делает запрос менее точным: убирает уточнения в скобках про статьи/пункты,
    хвостовые мусорные цифры и лишние кавычки.
    """
    q = raw.strip()
    q = q.strip('"').strip()
    q = q.replace("“", '"').replace("”", '"').replace("«", '"').replace("»", '"')
    q = q.strip('"').strip()

    # Убираем последние круглые скобки (обычно "статья ...", "пункт ...")
    q = re.sub(r"\s*\([^()]{0,200}\)\s*$", "", q).strip()
    # Иногда несколько скобок в конце
    q = re.sub(r"\s*\([^()]{0,200}\)\s*$", "", q).strip()

    # Убираем одиночные хвостовые цифры (часто артефакты типа "... )7", "... )8")
    q = re.sub(r"\s*\d+\s*$", "", q).strip()

    # Нормализуем пробелы
    q = re.sub(r"\s+", " ", q).strip()
    return q


def google_search_html(session: requests.Session, query: str, *, hl: str = "ru", num: int = 10, timeout: int = 30) -> str:
    params = {"q": query, "hl": hl, "num": str(num), "pws": "0"}
    r = session.get(GOOGLE_SEARCH_URL, params=params, timeout=timeout)
    r.raise_for_status()
    return r.text


def yandex_search_html(
    session: requests.Session,
    query: str,
    *,
    num: int = 10,
    timeout: int = 30,
    mime: Optional[str] = None,
) -> str:
    # Yandex: text=..., mime=rtf (может сработать), либо в query используем filetype:rtf
    params = {"text": query, "numdoc": str(num)}
    if mime:
        params["mime"] = mime
    r = session.get(YANDEX_SEARCH_URL, params=params, timeout=timeout)
    r.raise_for_status()
    return r.text


def extract_result_links_from_yandex(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    links: list[str] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        # Редиректы Yandex вида /clck/jsredir?...&url=<encoded>
        if href.startswith("https://yandex.ru/clck/jsredir") or href.startswith("http://yandex.ru/clck/jsredir"):
            p = urlparse(href)
            qs = parse_qs(p.query)
            target = (qs.get("url") or [""])[0]
            if target:
                links.append(target)
            continue
        if href.startswith("/clck/jsredir"):
            p = urlparse("https://yandex.ru" + href)
            qs = parse_qs(p.query)
            target = (qs.get("url") or [""])[0]
            if target:
                links.append(target)
            continue

        if href.startswith("http://") or href.startswith("https://"):
            links.append(href)

    out: list[str] = []
    seen: set[str] = set()
    for u in links:
        u = u.strip()
        if not u:
            continue
        host = urlparse(u).netloc.lower()
        if host.endswith("yandex.ru") or host.endswith("ya.ru") or host.endswith("yandex.net"):
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _looks_like_google_block(html: str) -> bool:
    s = html.lower()
    return ("unusual traffic" in s) or ("our systems have detected" in s) or ("sorry" in s and "automated queries" in s)


def _looks_like_google_enablejs(html: str) -> bool:
    s = html.lower()
    return ("/httpservice/retry/enablejs" in s) or ("enablejs" in s and "http-equiv" in s)


def _looks_like_yandex_verification(html: str) -> bool:
    s = html.lower()
    return ("checkcaptchafast" in s) or ("smart-captcha" in s) or ("<title>ÐÐµÑÐ¸ÑÐ¸ÐºÐ°ÑÐ¸Ñ</title>".lower() in s)


def yandex_playwright_search_links(query: str, *, num: int = 10, timeout_ms: int = 30000, mime: Optional[str] = None) -> list[str]:
    """
    JS-рендеринг Yandex выдачи через Playwright persistent profile.
    Нужен, когда Yandex отдаёт SmartCaptcha/верификацию вместо результатов.
    """
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError  # type: ignore
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return []

    params = {"text": query, "numdoc": str(num)}
    if mime:
        params["mime"] = mime
    url = YANDEX_SEARCH_URL + "?" + "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())

    headless = os.getenv("NPA_HEADFUL", "").strip() not in ("1", "true", "yes", "on")
    profile_dir = os.getenv("NPA_PROFILE_DIR", "").strip() or str(Path(__file__).with_name(".pw_profile"))

    links: list[str] = []
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=headless,
                locale="ru-RU",
            )
            try:
                context.set_default_timeout(timeout_ms)
                context.set_default_navigation_timeout(timeout_ms)
            except Exception:
                pass

            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # Если попали на верификацию/капчу — даём пользователю пройти её вручную.
            # Важно: после Enter НЕ надо заново делать goto — достаточно взять текущий DOM выдачи,
            # потому что пользователь уже видит результаты.
            if _looks_like_yandex_verification(page.content()) and not headless:
                print("[INFO] Yandex verification/SmartCaptcha detected. Solve it in the opened browser window (until results are visible), then press Enter here to continue.")
                try:
                    input()
                except KeyboardInterrupt:
                    context.close()
                    return []

            # Берём HTML и пытаемся распарсить именно выдачу
            html = ""
            try:
                html = page.content()
            except Exception:
                html = ""

            # если всё ещё верификация — попробуем подождать чуть-чуть и перечитать DOM
            if html and _looks_like_yandex_verification(html) and not headless:
                time.sleep(2)
                try:
                    html = page.content()
                except Exception:
                    pass

            if html and not _looks_like_yandex_verification(html):
                context.close()
                return extract_result_links_from_yandex(html)

            # Fallback: если парсинг HTML не дал результатов — соберём ссылки из DOM как раньше
            try:
                page.wait_for_selector("a[href]", timeout=timeout_ms)
            except Exception:
                pass

            for a in page.query_selector_all("a[href]"):
                href = (a.get_attribute("href") or "").strip()
                if not href:
                    continue
                if href.startswith("/"):
                    href = "https://yandex.ru" + href
                if href.startswith("http://") or href.startswith("https://"):
                    links.append(href)
            context.close()
    except (PlaywrightTimeoutError, KeyboardInterrupt):
        return []
    except Exception:
        return []

    # разворачиваем jsredir + дедуп + фильтр
    out: list[str] = []
    seen: set[str] = set()
    for u in links:
        p = urlparse(u)
        host = p.netloc.lower()
        if host.endswith("yandex.ru") and p.path.startswith("/clck/jsredir"):
            qs = parse_qs(p.query)
            target = (qs.get("url") or [""])[0]
            if target:
                u = target
                p = urlparse(u)
                host = p.netloc.lower()
        if host.endswith("yandex.ru") or host.endswith("ya.ru") or host.endswith("yandex.net"):
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def playwright_search_links(query: str, *, hl: str = "ru", num: int = 10, timeout_ms: int = 30000) -> list[str]:
    """
    JS-рендеринг Google выдачи через Playwright (fallback),
    когда HTML-версия отдаётся без результатов или требует enablejs.
    """
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError  # type: ignore
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return []

    url = f"{GOOGLE_SEARCH_URL}?q={quote_plus(query)}&hl={hl}&num={num}&pws=0"

    links: list[str] = []
    headless = os.getenv("NPA_HEADFUL", "").strip() not in ("1", "true", "yes", "on")
    profile_dir = os.getenv("NPA_PROFILE_DIR", "").strip() or str(Path(__file__).with_name(".pw_profile"))

    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=headless,
                locale="ru-RU",
            )
            try:
                context.set_default_timeout(timeout_ms)
                context.set_default_navigation_timeout(timeout_ms)
            except Exception:
                pass

            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # Часто появляется consent-страница. Пытаемся принять.
            for txt in ("Принять все", "Я согласен", "Согласиться", "Accept all", "I agree"):
                try:
                    btn = page.get_by_role("button", name=txt)
                    if btn.count() > 0:
                        btn.first.click(timeout=1500)
                        page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
                        break
                except Exception:
                    pass

            try:
                page.wait_for_selector("a[href]", timeout=timeout_ms)
            except Exception:
                pass
            for a in page.query_selector_all("a[href]"):
                href = a.get_attribute("href") or ""
                if not href:
                    continue
                if href.startswith("http://") or href.startswith("https://"):
                    links.append(href)
            context.close()
    except (PlaywrightTimeoutError, KeyboardInterrupt):
        return []
    except Exception:
        return []

    # нормализация: разворачиваем google.com/url?... в целевой url
    out: list[str] = []
    seen: set[str] = set()
    for u in links:
        p = urlparse(u)
        host = p.netloc.lower()
        if host.endswith("gstatic.com"):
            continue
        if host.endswith("google.com") and p.path.startswith("/url"):
            qs = parse_qs(p.query)
            target = (qs.get("q") or qs.get("url") or [""])[0]
            if target:
                u = target
                p = urlparse(u)
                host = p.netloc.lower()
                if host.endswith("gstatic.com") or host.endswith("google.com"):
                    continue
        if host.endswith("google.com"):
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def extract_result_links_from_google(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    links: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if not href:
            continue

        # Google часто отдаёт ссылки через /url?q=...
        if href.startswith("/url?"):
            qs = parse_qs(urlparse(href).query)
            u = (qs.get("q") or qs.get("url") or [""])[0]
            if u:
                links.append(u)
            continue

        # Иногда встречаются прямые внешние ссылки
        if href.startswith("http://") or href.startswith("https://"):
            links.append(href)

    # Fallback: современная выдача часто прячет URL в JS/JSON, без явных <a href="/url?q=">.
    # Тогда вытаскиваем все https://... из текста и фильтруем.
    if not links:
        raw_urls = re.findall(r"https?://[^\s\"'<>]+", html)
        for u in raw_urls:
            u = (
                u.replace("\\u0026", "&")
                .replace("\\u003d", "=")
                .replace("\\u003f", "?")
                .replace("\\/", "/")
            )
            u = u.strip().rstrip(").,;\"'")
            links.append(u)

    # Дедуп
    out: list[str] = []
    seen: set[str] = set()
    for u in links:
        u = u.strip()
        if not u:
            continue
        # Отсекаем гугловые служебные
        host = urlparse(u).netloc.lower()
        if host.endswith("google.com") or host.endswith("gstatic.com"):
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def is_rtf_url(url: str) -> bool:
    p = urlparse(url)
    path = (p.path or "").lower()
    if path.endswith(".rtf"):
        return True
    # Некоторые делают download через query
    q = (p.query or "").lower()
    return (".rtf" in path) or ("file=rtf" in q) or ("format=rtf" in q) or ("rtf" in q and "download" in q)


def find_rtf_on_page(session: requests.Session, page_url: str, *, timeout: int = 30) -> Optional[str]:
    try:
        r = session.get(page_url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
    except Exception:
        return None

    ctype = (r.headers.get("content-type") or "").lower()
    if "application/rtf" in ctype or "text/rtf" in ctype:
        return r.url

    # Если уже .rtf (но сервер отдаёт text/html) — тоже принимаем
    if is_rtf_url(r.url):
        return r.url

    soup = BeautifulSoup(r.text, "lxml")
    candidates: list[str] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        abs_url = urljoin(r.url, href)
        if is_rtf_url(abs_url):
            candidates.append(abs_url)

    if not candidates:
        return None

    # Предпочтём прямые .rtf
    candidates.sort(key=lambda u: 0 if urlparse(u).path.lower().endswith(".rtf") else 1)
    return candidates[0]


@dataclass(frozen=True)
class DownloadResult:
    ok: bool
    query_raw: str
    query_soft: str
    selected_url: Optional[str]
    saved_path: Optional[str]
    error: Optional[str]


def download_file(session: requests.Session, url: str, dest_path: Path, *, timeout: int = 60) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with session.get(url, timeout=timeout, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 128):
                if chunk:
                    f.write(chunk)


def _is_retryable_download_error(err: Exception) -> bool:
    # SSL/cert issues or rate limit are good candidates for "try next link"
    try:
        import requests as _rq

        if isinstance(err, _rq.exceptions.SSLError):
            return True
        if isinstance(err, _rq.exceptions.HTTPError):
            resp: Optional[Response] = getattr(err, "response", None)
            if resp is not None and resp.status_code in (403, 429, 451):
                return True
    except Exception:
        pass
    return False


def yandex_candidates(session: requests.Session, query_soft: str, *, timeout: int, max_results_to_probe: int = 6) -> list[str]:
    ya_query = f"{query_soft} filetype:rtf"
    try:
        ya_html = yandex_search_html(session, ya_query, num=10, timeout=timeout, mime="rtf")
        if _looks_like_yandex_verification(ya_html) and os.getenv("NPA_DISABLE_PLAYWRIGHT", "").strip() not in ("1", "true", "yes", "on"):
            links = yandex_playwright_search_links(ya_query, num=10, timeout_ms=timeout * 1000, mime="rtf")
        else:
            links = extract_result_links_from_yandex(ya_html)
    except Exception:
        links = []

    # Expand pages that might contain rtf links
    out: list[str] = []
    seen: set[str] = set()

    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)

    # Probe some pages for embedded rtf
    for u in links[:max_results_to_probe]:
        rtf = find_rtf_on_page(session, u, timeout=timeout)
        if rtf and rtf not in seen:
            seen.add(rtf)
            out.insert(0, rtf)  # prefer direct rtf

    # Prefer direct .rtf
    out.sort(key=lambda u: 0 if urlparse(u).path.lower().endswith(".rtf") else 1)
    return out


def iter_queries(list_path: Path) -> Iterable[str]:
    data = list_path.read_bytes()
    text: Optional[str] = None

    def score_ru(t: str) -> float:
        # доля кириллицы среди букв/цифр/пробелов
        if not t:
            return 0.0
        cyr = len(re.findall(r"[А-Яа-яЁё]", t))
        alpha = len(re.findall(r"[A-Za-zА-Яа-яЁё]", t))
        return cyr / max(1, alpha)

    best_enc: Optional[str] = None
    best_score = -1.0

    for enc in ("utf-8-sig", "utf-8", "cp1251", "cp866", "koi8-r"):
        try:
            candidate = data.decode(enc, errors="strict")
        except UnicodeDecodeError:
            continue
        sc = score_ru(candidate)
        if sc > best_score:
            best_score = sc
            best_enc = enc
            text = candidate

    # Если ни одна из типичных кодировок не дала осмысленный русский текст — fallback на charset_normalizer
    if text is None or best_score < 0.2:
        best = from_bytes(data).best()
        text = str(best) if best is not None else data.decode("utf-8", errors="replace")

    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        yield s


def build_session() -> requests.Session:
    s = requests.Session()
    # Имитация обычного браузера, чтобы меньше банило
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.6,en;q=0.4",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return s


def choose_rtf_url(
    session: requests.Session,
    query_soft: str,
    *,
    max_results_to_probe: int = 6,
    timeout: int = 30,
    engine: str = "auto",
) -> tuple[Optional[str], Optional[str]]:
    """
    Возвращает (rtf_url, debug_note).
    """
    engine = (engine or "auto").strip().lower()

    if engine not in ("auto", "google", "yandex"):
        engine = "auto"

    if engine in ("auto", "google"):
    # 1) Сначала пытаемся найти прямую ссылку на rtf в выдаче
        search_query = f"{query_soft} filetype:rtf"
        html = google_search_html(session, search_query, num=10, timeout=timeout)
        if _looks_like_google_block(html):
            if engine == "google":
                return None, "google_blocked"
            links = []
        elif _looks_like_google_enablejs(html) and os.getenv("NPA_DISABLE_PLAYWRIGHT", "").strip() not in ("1", "true", "yes", "on"):
            links = playwright_search_links(search_query, num=10, timeout_ms=timeout * 1000)
        else:
            links = extract_result_links_from_google(html)

        rtf_links = [u for u in links if is_rtf_url(u)]
        if rtf_links:
            return rtf_links[0], "direct_rtf_from_serp"

        # 2) Если прямых rtf нет — откроем несколько результатов и поищем rtf внутри
        to_probe = links[:max_results_to_probe]
        for u in to_probe:
            rtf = find_rtf_on_page(session, u, timeout=timeout)
            if rtf:
                return rtf, "rtf_found_on_result_page"

        # 3) Последняя попытка: запрос без filetype, но с "rtf"
        fallback_query = f"{query_soft} rtf"
        html2 = google_search_html(session, fallback_query, num=10, timeout=timeout)
        if _looks_like_google_block(html2):
            if engine == "google":
                return None, "google_blocked"
            links2 = []
        elif _looks_like_google_enablejs(html2) and os.getenv("NPA_DISABLE_PLAYWRIGHT", "").strip() not in ("1", "true", "yes", "on"):
            links2 = playwright_search_links(fallback_query, num=10, timeout_ms=timeout * 1000)
        else:
            links2 = extract_result_links_from_google(html2)
        rtf_links2 = [u for u in links2 if is_rtf_url(u)]
        if rtf_links2:
            return rtf_links2[0], "direct_rtf_from_serp_fallback"

        for u in links2[:max_results_to_probe]:
            rtf = find_rtf_on_page(session, u, timeout=timeout)
            if rtf:
                return rtf, "rtf_found_on_result_page_fallback"

    # 4) Fallback: Yandex
    if engine in ("auto", "yandex"):
        ya_query = f"{query_soft} filetype:rtf"
        try:
            ya_html = yandex_search_html(session, ya_query, num=10, timeout=timeout, mime="rtf")
            if _looks_like_yandex_verification(ya_html) and os.getenv("NPA_DISABLE_PLAYWRIGHT", "").strip() not in ("1", "true", "yes", "on"):
                ya_links = yandex_playwright_search_links(ya_query, num=10, timeout_ms=timeout * 1000, mime="rtf")
            else:
                ya_links = extract_result_links_from_yandex(ya_html)
        except Exception:
            ya_links = []

        ya_rtf = [u for u in ya_links if is_rtf_url(u)]
        if ya_rtf:
            return ya_rtf[0], "yandex_direct_rtf"

        for u in ya_links[:max_results_to_probe]:
            rtf = find_rtf_on_page(session, u, timeout=timeout)
            if rtf:
                return rtf, "yandex_rtf_found_on_result_page"
    else:
        return None, "engine_yandex_disabled"

    return None, "not_found"


def run_one(
    session: requests.Session,
    query_raw: str,
    download_dir: Path,
    *,
    timeout: int,
    overwrite: bool,
    engine: str,
) -> DownloadResult:
    query_soft = soften_query(query_raw)

    rtf_url, note = choose_rtf_url(session, query_soft, timeout=timeout, engine=engine)
    if not rtf_url:
        err = f"rtf_not_found ({note})"
        return DownloadResult(False, query_raw, query_soft, None, None, err)

    # Имя файла: по soft query + sha1, чтобы избежать коллизий и слишком длинных имён
    base = _safe_filename(query_soft)
    fname = f"{base}__{_sha1(query_soft)[:10]}.rtf"
    dest = download_dir / fname

    if dest.exists() and not overwrite:
        return DownloadResult(True, query_raw, query_soft, rtf_url, str(dest), "already_exists")

    # Attempt download; on SSL/429 errors try other links from the same SERP (yandex)
    candidates = [rtf_url]
    if engine == "yandex":
        try:
            extra = yandex_candidates(session, query_soft, timeout=timeout)
            for u in extra:
                if u not in candidates:
                    candidates.append(u)
        except Exception:
            pass

    errors: list[str] = []
    for idx, u in enumerate(candidates[:12], start=1):
        try:
            download_file(session, u, dest, timeout=max(60, timeout))
            return DownloadResult(True, query_raw, query_soft, u, str(dest), None)
        except Exception as e:
            errors.append(f"{idx}) {u} -> {type(e).__name__}: {e}")
            if not _is_retryable_download_error(e):
                break

    short = " | ".join(errors[:3]) + (f" | ... +{len(errors)-3} more" if len(errors) > 3 else "")
    return DownloadResult(False, query_raw, query_soft, rtf_url, None, f"download_failed: {short}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", default=str(Path(__file__).with_name("npa_list.txt")), help="Path to npa_list.txt")
    ap.add_argument("--out", default=str(Path(__file__).with_name("npa_download")), help="Output directory for downloads")
    ap.add_argument("--report", default=str(Path(__file__).with_name("download_report.jsonl")), help="JSONL report path")
    ap.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    ap.add_argument("--sleep-min", type=float, default=2.0, help="Min sleep between queries (seconds)")
    ap.add_argument("--sleep-max", type=float, default=5.0, help="Max sleep between queries (seconds)")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of lines to process (0 = all)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    ap.add_argument("--no-playwright", action="store_true", help="Disable Playwright fallback")
    ap.add_argument("--engine", choices=["auto", "yandex", "google"], default="auto", help="Search engine preference")
    ap.add_argument(
        "--retry-from-report",
        choices=["off", "failed", "ssl", "all_failed"],
        default="off",
        help="Retry queries based on last records in download_report.jsonl",
    )
    args = ap.parse_args()

    list_path = Path(args.list)
    out_dir = Path(args.out)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    session = build_session()

    ok = 0
    fail = 0
    done = load_completed_soft_queries(report_path)
    seen_in_run: set[str] = set()

    if args.retry_from_report != "off":
        query_iter: Iterable[str] = iter_retry_queries(report_path, mode=args.retry_from_report)
    else:
        query_iter = iter_queries(list_path)

    try:
        for i, q in enumerate(query_iter, start=1):
            if args.limit and i > args.limit:
                break
            # allow turning off playwright via env/flag
            if args.no_playwright:
                os.environ["NPA_DISABLE_PLAYWRIGHT"] = "1"
            query_soft = soften_query(q)
            key = _sha1(query_soft)
            if key in done or key in seen_in_run:
                print(f"[SKIP] {query_soft} (already downloaded)")
                continue
            seen_in_run.add(key)

            res = run_one(session, q, out_dir, timeout=args.timeout, overwrite=args.overwrite, engine=args.engine)
            record = {
                "ts_utc": _utc_now_iso(),
                "ok": res.ok,
                "query_raw": res.query_raw,
                "query_soft": res.query_soft,
                "selected_url": res.selected_url,
                "saved_path": res.saved_path,
                "error": res.error,
            }
            with open(report_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

            if res.ok:
                ok += 1
                done.add(_sha1(res.query_soft))
                print(f"[OK] {res.query_soft} -> {res.saved_path}")
            else:
                fail += 1
                print(f"[FAIL] {res.query_soft}: {res.error}")

            time.sleep(random.uniform(args.sleep_min, args.sleep_max))
    except KeyboardInterrupt:
        print(f"Interrupted. ok={ok}, fail={fail}. Partial report: {report_path}")
        return 130

    print(f"Done. ok={ok}, fail={fail}. Report: {report_path}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())


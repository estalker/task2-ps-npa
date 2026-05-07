## NPA downloader (RTF)

Скрипт читает `npa_list.txt`, ищет документы через `google.com` (ориентируясь на `.rtf`) и скачивает файлы в `npa_download/`.

### Установка

```bash
python -m pip install -r npa_downloader/requirements.txt
python -m playwright install chromium
```

### Запуск

```bash
python npa_downloader/download_npa.py
```

Файлы сохраняются в `npa_downloader/npa_download/`, отчёт — `npa_downloader/download_report.jsonl`.

### Источник `pravo.gov.ru`

Если нужно искать прямо через `pravo.gov.ru/search/` (страница результатов формируется JavaScript’ом), используйте Playwright в “видимом” режиме:

```powershell
$env:NPA_HEADFUL="1"
python npa_downloader\download_npa.py --engine pravo --timeout 60
```

### Список `ps_list.txt`

Если нужно качать из `ps_list.txt` (в `ps_download/`):

```powershell
python npa_downloader\download_npa.py --kind ps --engine pravo --timeout 60
```

### Повторить только упавшие

Например, повторить только те, что упали на SSL/сертификатах:

```powershell
$env:NPA_HEADFUL="1"
python npa_downloader\download_npa.py --engine yandex --retry-from-report ssl --timeout 60
```

### Примечания

- Если Google начнёт выдавать CAPTCHA/блокировки (в отчёте будет `google_blocked`), придётся запускать через прокси/другую сеть или заменить источник поиска (например, корпоративный поисковик/зеркало).
- Если выдача не парсится (Google требует JS/consent), можно запустить Playwright в “видимом” режиме один раз, принять окна и сохранить cookies:

```bash
set NPA_HEADFUL=1
python npa_downloader/download_npa.py --limit 1 --sleep-min 0 --sleep-max 0
```

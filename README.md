# Docx → Neo4j prototype (Profstandards)

Put `.docx` files into `input/`, then run the ingester. It will:

- extract raw text from docx
- (optional) call an LLM to extract strict JSON facts
- upsert a small graph into Neo4j (Document, Profession, Process, Permit, Hazard)

## Prereqs

- Docker Desktop
- Python 3.10+

## 1) Start Neo4j (free Community)

```bash
docker compose up -d
```

Open Neo4j Browser: `http://localhost:7474`

Default creds (dev only):

- user: `neo4j`
- password: `neo4j_password`

## 2) Python setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 3) Put docx and ingest

Drop `.docx` into `input/` and run:

```bash
python -m app.ingest --input input --doc-source "profstandard"
```

## 4) (Optional) enable LLM extraction

This prototype supports OpenAI-compatible chat completions.

Set env vars:

```bash
setx OPENAI_API_KEY "..."
setx OPENAI_MODEL "gpt-4.1-mini"
```

Then re-open the terminal and ingest again — the ingester will store both raw text and extracted facts.

### Local open LLM option: Ollama (recommended)

1) Install Ollama, then pull a model:

```bash
ollama pull qwen2.5:7b-instruct
```

2) Run the Ollama OpenAI-compatible server:

```bash
ollama serve
```

3) Point the ingester to the local server:

```bash
setx OPENAI_BASE_URL "http://localhost:11434/v1"
setx OPENAI_API_KEY "local"
setx OPENAI_MODEL "qwen2.5:7b-instruct"
```

### Quick start scripts (Windows)

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_all.ps1
```

Then in a separate terminal:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_ollama.ps1
```

To run ingest with local Ollama vars in the same process:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_ingest.ps1
```

If you see Cyrillic "mojibake" (�����) in the console, run ingest via `scripts/run_ingest.ps1`
— it forces UTF-8 output for Python/PowerShell.

## Frontend UI (PS/NPA file manager + result table)

This repo includes an optional `frontend` container (FastAPI + static UI) that:

- manages **two file sets**:
  - `input/ps` (ПС: `.docx`, `.rtf`)
  - `input/npa` (НПА: `.docx`, `.rtf` — current ingester uses `.rtf`)
- runs the end-to-end pipeline and produces `output/list_mandatory_ps.md`
- renders the markdown table as an **HTML table**
- lets you download the result as **CSV**

### Start

```bash
docker compose up -d --build
```

Then open UI:

- `http://localhost:8080`

Notes:

- Neo4j is available at `http://localhost:7474`
- The frontend uses `bolt://neo4j:7687` inside compose network.

### Ollama modes (host vs docker)

#### Mode A (default): Ollama on the host machine

- Run Ollama on the host at `http://localhost:11434`
- Start compose normally:

```bash
docker compose up -d --build
```

Frontend will use `OPENAI_BASE_URL=http://host.docker.internal:11434/v1`.

#### Mode B: Ollama in Docker (recommended for Linux)

Start with override compose file (works with both `docker compose` and legacy `docker-compose`):

```bash
docker compose -f docker-compose.yml -f docker-compose.ollama.yml up -d --build
```

Pull models (inside container):

```bash
docker exec -it task2_ollama ollama pull qwen2.5:3b-instruct
docker exec -it task2_ollama ollama pull qwen2.5:7b-instruct
```

## 5) Quality checks (Cypher)

Run these in Neo4j Browser:

```cypher
// 1) One process — one hazard
MATCH (pr:Process)-[:HAS_HAZARD]->(h)
WITH pr, count(DISTINCT h) as c
WHERE c > 1
RETURN pr.name, c;

// 2) Permits without any source doc mention
MATCH (p:Permit)
WHERE NOT (p)-[:MENTIONED_IN]->(:Document)
RETURN p.name;

// 3) Mixed hazards per profession (if you enforce 1 hazard type per profession)
MATCH (p:Profession)-[:PERFORMS]->(pr)-[:HAS_HAZARD]->(h)
WITH p, collect(DISTINCT h.type) as hazards
WHERE size(hazards) > 1
RETURN p.name, hazards;
```


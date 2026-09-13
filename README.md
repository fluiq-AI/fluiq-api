# fluiq-api

Control-plane API for Fluiq: observability, evaluation and security for LLM and
agent applications, against a single trace.

FastAPI + asyncpg + ClickHouse + Kafka.

> **Status: archived.** Fluiq ran from 10 April to September 2026 and never found
> customers. The hosted service at `api.getfluiq.com` is shut down and the
> infrastructure is gone. The code is MIT and stays public because it works, and
> because some of it is worth reading. Nothing here is maintained — fork it
> freely.

---

## What this is

The API is the front door for everything except trace ingestion volume. SDKs
([Python](https://github.com/fluiq-AI/fluiq-sdk),
[TypeScript](https://github.com/fluiq-AI/fluiq-sdk-typescript)) send spans here;
the API authenticates, enforces quota, and publishes to Kafka. Three workers do
the slow work off the request path:

```
  SDK ──► POST /ingest ──► Kafka: trace topic ──► fluiq-worker-tracer ──► ClickHouse
                                                          │
                                                          └─► trace-persisted topic
                                                                   │
                            ┌──────────────────────────────────────┴────────┐
                            ▼                                               ▼
                   fluiq-worker-evaluator                          fluiq-worker-security
                   (LLM-as-judge, agentic eval)                    (injection, PII, secrets)
                            │                                               │
                            └───────────────► ClickHouse ◄──────────────────┘
```

Companion repos:

| Repo | Role |
|---|---|
| [fluiq-worker-tracer](https://github.com/fluiq-AI/fluiq-worker-tracer) | Persists spans to ClickHouse, fans out to eval/security |
| [fluiq-worker-evaluator](https://github.com/fluiq-AI/fluiq-worker-evaluator) | LLM-as-judge and trajectory-level agentic evaluation |
| [fluiq-worker-security](https://github.com/fluiq-AI/fluiq-worker-security) | Prompt injection, jailbreak, PII, secret scanning |
| [fluiq-sdk](https://github.com/fluiq-AI/fluiq-sdk) | Python SDK — instruments in two lines |
| [fluiq-sdk-typescript](https://github.com/fluiq-AI/fluiq-sdk-typescript) | TypeScript SDK |
| [fluiq-frontend](https://github.com/fluiq-AI/fluiq-frontend) | Next.js dashboard and marketing site |
| [guardrail-bench](https://github.com/SaurabhKumbhar24/guardrail-bench) | Benchmark of eight output guardrails, 949 cases |

## Stack

- **FastAPI** with async everywhere
- **Postgres** (asyncpg) — orgs, users, API keys, prompts, datasets, judge
  prompts, billing, blog
- **ClickHouse** — traces, evaluations, security findings, audit log, cost
  rollups
- **Kafka** (aiokafka) — work handoff to the three workers, plus synchronous
  request-reply for the blocking security gate
- **Redis** — optional, used by `fluiq.optimize()` response caching

## Quick start

Brings up Kafka, ClickHouse, Postgres, Redis, MinIO and the API. Both schemas
are applied automatically on first boot, and MinIO stands in for S3 so dataset
imports and blog media work locally.

```bash
cp .env.example .env.development   # fill in the three secrets at the top
docker compose up --build
```

The API is then on `http://localhost:8080`, with OpenAPI docs at
`http://localhost:8080/docs`, and the MinIO console on `http://localhost:9101`.

This compose file also creates the `fluiq-ai` Docker network. Bring it up first,
then start the three workers against that same network — see each worker's
README — and traces will persist, evaluate and scan end to end.

### Without Docker

`config.py` calls `load_dotenv()` with no arguments, which reads `.env` — not
`.env.development`. Compose injects that file as real environment variables, so
the two paths need different filenames:

```bash
cp .env.example .env
# then point the hosts at localhost rather than the container names:
#   POSTGRES_DSN=postgresql://fluiq:fluiq@localhost:5432/fluiq
#   CLICKHOUSE_HOST=localhost
#   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
#   REDIS_URL=redis://localhost:6379/0

python -m venv .api_venv
.api_venv/Scripts/pip install -r requirements.txt   # POSIX: .api_venv/bin/pip
.api_venv/Scripts/python -m uvicorn main:app --reload --port 8080
```

You still need the data stores reachable; `docker compose up kafka clickhouse
postgres redis` starts those without the API.

## Configuration

Every setting is an environment variable read in [`config.py`](config.py), and
almost none of them have a fallback — several are wrapped in `int()`/`float()`,
so a missing value raises `TypeError` at import rather than failing later with a
useful message. [`.env.example`](.env.example) therefore lists every variable,
with defaults matching `docker-compose.yml`. The three you must fill in:

| Variable | Why |
|---|---|
| `JWT_SECRET` | Signs dashboard sessions |
| `AUDIT_HMAC_SECRET` | Tamper-evident audit chain |
| `ANTHROPIC_API_KEY` | LLM-as-judge and the security semantic classifier |

Google and GitHub OAuth (`GOOGLE_CLIENT_ID`, `GITHUB_CLIENT_ID`, …) are optional;
email/password auth works without them.

## API surface

Everything is under `/api/v1` except `/admin`. Route groups live in
[`routes/`](routes/), one package each:

**Ingest and traces** — `trace`, `otel` (OpenInference/OTel import from
LangSmith, Langfuse, Phoenix, Braintrust), `agents`, `views`, `monitor`

**Evaluation** — `evaluate`, `evaluate/judge_prompts`, `rubrics`, `aggregates`,
`datasets`, `prompts`, `feedback`, `review`

**Security** — `secure`, `guardrails`, `online_rules`, `audit`

**Platform** — `auth`, `organizations`, `api_keys`, `quota`, `billing`,
`alerts`, `credentials`, `resources`, `models`

**Site** — `blog`, `contact`, `leads`, `demo`

## Notable design decisions

Some of these were bought with outages, so they are documented rather than left
to be rediscovered.

**Evaluation never runs inline.** The API publishes to the Kafka eval topic and,
where a caller is blocking, awaits a correlated reply. Judging is slow and
model-dependent; putting it on the request path makes ingestion latency a
function of Anthropic's p99. See `routes/evaluate/`.

**Judge failures fail open.** If the evaluator errors, times out or the model is
down, the span is recorded unscored and the request proceeds. A quality tool
that takes production down when it breaks is worse than no quality tool. The
same holds for the security gate and `fluiq.optimize()` — both are opt-in and
both fail open.

**Schemas apply themselves at pool start.** `db_queues/postgresql/__init__.py`
runs `schema.sql` when the connection pool opens, and the ClickHouse client does
the same. Every statement is `CREATE TABLE IF NOT EXISTS` / `ADD COLUMN IF NOT
EXISTS`, so boot is idempotent and there is no separate migration step. The
tradeoff is that destructive changes have to be done by hand.

**Retention is the paid axis, not volume.** Observability was free and unlimited
on every tier. Free orgs got 14 days, enforced by a per-row ClickHouse TTL whose
`retention_days` is stamped at ingest rather than by a background sweep, so
changing a plan doesn't rewrite history.

**Per-run rollups are precomputed.** Cost, quality and security counts per
`root_trace_id` come from an `AggregatingMergeTree` fed by materialized views
that update incrementally on child insert. Summing children at read time meant a
trace list page issued one query per run and stampeded ClickHouse.

**Dataset trajectories are pinned, not referenced.** Importing a run into a
dataset copies the entire span tree into a no-TTL table, so an evaluation set
stays reproducible after the source traces expire.

## If you self-host this

A full security audit is in
[Documentations/security-audit-2026-07-18.md](https://github.com/fluiq-AI/Documentations/blob/main/security-audit-2026-07-18.md).
Most findings were fixed; these were not, and they matter if you run this
against real traffic:

- **Raw PII and secrets persist in `traces.event` in plaintext** (M8, deferred).
  The security worker detects and flags them, but the original span body is
  stored unredacted in ClickHouse. Anyone with read access to the trace table
  sees whatever your agents saw. If you self-host, this is the first thing to
  fix.
- **Fail-open is not per-category** (H4, partial). The gate can be set to block
  on degraded, but you cannot currently say "fail closed for secrets, fail open
  for everything else."
- **`system` and `developer` role content is excluded from scanning** (L4).
  Deliberate — it is your own text, not user input — but it means a poisoned
  system prompt is not caught here.

The audit lists the rest, including what was fixed and when.

## Tests

```bash
.api_venv/Scripts/python -m pytest tests/
```

`tests/` covers the security scanners, quota, audit chaining and the eval
request-reply path. There is no coverage gate.

## Licence

MIT. See [LICENSE](LICENSE).

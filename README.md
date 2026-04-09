# Statcast NL Query (MVP)

Plain-language baseball questions → Statcast-backed tables via a small Python data service (pybaseball) and an LLM tool-calling layer.

Default clone location used here: `~/Desktop/statcast-query`.

## Layout

- `apps/web` — Next.js UI (chat + tables) and API routes (LLM orchestration).
- `services/data` — FastAPI + pybaseball wrappers, in-memory TTL cache.

## Prerequisites

- Node 20+
- Python 3.11+
- `ANTHROPIC_API_KEY` (set in `apps/web/.env.local`)

## Run locally

### 1. Data service

```bash
cd services/data
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8765
```

The service enables **pybaseball disk caching** on startup (`pybaseball.cache.enable()`). That does not make the *first* Savant pull faster, but **repeat identical requests** (same function + args) are served from cache, and long Statcast runs are less painful if interrupted. Override the cache directory with env `PYBASEBALL_CACHE` if you want it inside the project.

### 2. Web app

```bash
cd apps/web
cp .env.example .env.local
# Edit .env.local: ANTHROPIC_API_KEY, DATA_SERVICE_URL=http://127.0.0.1:8765

npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## Quick test

With the data service already running on port **8765**:

```bash
bash scripts/smoke-test.sh
```

That hits `/health`, then small `pitcher_pitch_arsenal` and `pitching_season_stats` requests (first run may be slow while pybaseball/Savant respond).

**Full UI test:** set `ANTHROPIC_API_KEY` in `apps/web/.env.local`, run `npm run dev`, open http://localhost:3000, and ask something like: *“Top 5 qualified pitchers by average four-seam spin in 2024.”*

## Environment variables (web)

| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | Anthropic API key for chat + tools |
| `DATA_SERVICE_URL` | Base URL of the FastAPI service (default `http://127.0.0.1:8765`) |
| `ANTHROPIC_MODEL` | Optional model id (default `claude-3-5-haiku-latest`) |

## Upgrade path: Postgres

Replace the Python service internals with queries against a nightly-synced Postgres replica of leaderboards / Statcast aggregates. Keep the same HTTP tool contracts (`POST /v1/pitcher_pitch_arsenal`, `POST /v1/pitching_season_stats`, `POST /v1/batting_season_stats`, `POST /v1/resolve_player`, `POST /v1/statcast_pitches`, `POST /v1/statcast_spin_variance`, `POST /v1/batter_hit_distance_by_park`) so the Next.js layer stays unchanged.

## Compliance

Cache responses, avoid tight loops against Baseball Savant, and respect MLB/third-party terms of use.

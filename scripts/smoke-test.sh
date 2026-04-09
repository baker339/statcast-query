#!/usr/bin/env bash
# Run the data service first (see README), then: bash scripts/smoke-test.sh
set -euo pipefail
BASE="${DATA_SERVICE_URL:-http://127.0.0.1:8765}"

echo "== GET $BASE/health"
curl -sS "$BASE/health" | python3 -m json.tool || curl -sS "$BASE/health"
echo

echo "== POST /v1/pitcher_pitch_arsenal (2024, avg_spin, row_cap=5)"
curl -sS -X POST "$BASE/v1/pitcher_pitch_arsenal" \
  -H "Content-Type: application/json" \
  -d '{"year":2024,"min_pitches":250,"arsenal_type":"avg_spin","row_cap":5}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('columns:', d.get('columns',[])[:8], '...'); print('rows:', len(d.get('rows',[])))"

echo
echo "== POST /v1/pitching_season_stats (2024, min_ip=20, SO+Name)"
curl -sS -X POST "$BASE/v1/pitching_season_stats" \
  -H "Content-Type: application/json" \
  -d '{"season":2024,"min_ip":20,"metrics":["Name","Team","IP","SO"],"row_cap":5}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('rows:', d.get('rows',[]))"

echo
echo "== POST /v1/mlb_stat_leaders (2026 pitcher strikeouts, limit=3)"
curl -sS -X POST "$BASE/v1/mlb_stat_leaders" \
  -H "Content-Type: application/json" \
  -d '{"season":2026,"leader_category":"strikeOuts","limit":3,"leader_game_types":"R"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('source:', d.get('source')); print('rows:', d.get('rows',[]))"

echo
echo "== POST /v1/batting_season_stats (2025 BOS, PA/OPS — MLB fallback if FanGraphs fails)"
curl -sS -X POST "$BASE/v1/batting_season_stats" \
  -H "Content-Type: application/json" \
  -d '{"season":2025,"min_pa":50,"team_abbr":"BOS","metrics":["Name","Team","PA","HR","AVG","OBP","SLG"],"row_cap":8}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('source:', d.get('source')); print('nrows:', len(d.get('rows',[]))); print('note:', (d.get('note') or '')[:120])"

echo
echo "Smoke test done."

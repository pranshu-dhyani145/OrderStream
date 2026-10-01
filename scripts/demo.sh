#!/usr/bin/env bash
# Usage: ./scripts/demo.sh [base_url]   (default http://localhost:8000)
set -euo pipefail
BASE="${1:-http://localhost:8000}"

echo "Sending 6 orders (3 users, 2 orders each)..."
for user in U-101 U-102 U-103; do
  for n in 1 2; do
    curl -s -X POST "$BASE/orders" -H "Content-Type: application/json" \
      -d "{\"user_id\":\"$user\",\"amount\":$((RANDOM % 4000 + 500)),\"items\":[\"Keyboard\",\"Mouse\"]}" \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['order_id'], d['user_id'], '-> partition', d['kafka']['partition'], 'offset', d['kafka']['offset'])"
  done
done

sleep 2
echo; echo "Stats:"; curl -s "$BASE/stats"; echo

#!/usr/bin/env bash
# Golden-path walkthrough of Gatepass with curl + jq.
#
#   BASE_URL=https://api.example.com TOKEN=<user access token> ADMIN_TOKEN=<admin access token> \
#     ./scripts/golden-path.sh
#
# TOKEN / ADMIN_TOKEN are Cognito ACCESS tokens (not ID tokens). The admin token must
# belong to a user in the Cognito group "admin". Tokens are read from the environment
# and never printed. Optional: WAIT_EXPIRY=true also waits for an unused hold to expire
# (HOLD_TTL, 10 minutes by default) and shows the tickets coming back.
#
# What it exercises (see docs/PLAN.md "Golden path"):
#   1 admin creates a venue + event, publishes it        -> EventPublished
#   2 inventory appears in reservations (async)          -> EventBridge -> SQS -> consumer
#   3 anyone lists / reads the event
#   4 user places a hold (Idempotency-Key, replayed once)
#   5 user creates an order from the hold (replayed once)
#   6 payment is simulated asynchronously; poll until paid / payment_failed
#   7 ticket e-mail + file: poll the ticket-url endpoint, list notifications
#   8 user cancels the paid order                         -> OrderCancelled -> inventory restored
set -euo pipefail

: "${BASE_URL:?set BASE_URL, e.g. https://api.example.com}"
: "${TOKEN:?set TOKEN (user access token)}"
: "${ADMIN_TOKEN:?set ADMIN_TOKEN (admin access token)}"
BASE_URL="${BASE_URL%/}"
POLL_SECONDS="${POLL_SECONDS:-90}"

for tool in curl jq; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 2; }
done

uuid() { if command -v uuidgen >/dev/null; then uuidgen | tr 'A-Z' 'a-z'; else cat /proc/sys/kernel/random/uuid; fi; }

# future_date <days>: RFC 3339 UTC timestamp N days ahead (GNU date, then BSD/macOS date).
future_date() {
  date -u -d "+$1 days" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+"$1"d +%Y-%m-%dT%H:%M:%SZ
}

step() { printf '\n== %s\n' "$*"; }

RESP_BODY=""; RESP_CODE=""
# call <token|-> <method> <path> [json-body] [extra curl args...]
# Sets RESP_CODE and RESP_BODY. Never prints the token.
call() {
  local token="$1" method="$2" path="$3" body="${4:-}"; shift 4 || shift $#
  local args=(-sS -X "$method" "$BASE_URL$path" -H 'Accept: application/json' -w $'\n%{http_code}')
  [ "$token" != "-" ] && args+=(-H "Authorization: Bearer $token")
  [ -n "$body" ] && args+=(-H 'Content-Type: application/json' --data "$body")
  local out; out="$(curl "${args[@]}" "$@")"
  RESP_CODE="${out##*$'\n'}"; RESP_BODY="${out%$'\n'*}"
  printf '   %s %s -> %s\n' "$method" "$path" "$RESP_CODE"
}

expect() { # expect <code...>: abort unless RESP_CODE is one of the arguments
  local c; for c in "$@"; do [ "$RESP_CODE" = "$c" ] && return 0; done
  echo "   unexpected status $RESP_CODE (wanted: $*)" >&2
  echo "$RESP_BODY" | jq . 2>/dev/null >&2 || echo "$RESP_BODY" >&2
  exit 1
}

# poll <seconds> <description> <command...>: run until the command succeeds or time runs out.
poll() {
  local limit="$1" what="$2"; shift 2
  local deadline=$(( $(date +%s) + limit ))
  until "$@"; do
    [ "$(date +%s)" -ge "$deadline" ] && { echo "   timed out waiting for: $what" >&2; return 1; }
    sleep 2
  done
}

# ---- 1 catalog -------------------------------------------------------------------
step "1. Admin creates a venue and a draft event, then publishes it"
call "$ADMIN_TOKEN" POST /catalog/v1/venues '{"name":"Golden Path Arena","city":"Lagos","address":"1 Example Road","capacity":500}'
expect 201
VENUE_ID="$(jq -r .id <<<"$RESP_BODY")"

EVENT_BODY="$(jq -n --arg v "$VENUE_ID" --arg s "$(future_date 3)" --arg e "$(future_date 4)" '{
  venueId: $v, title: "Golden Path Live", description: "Created by scripts/golden-path.sh",
  startsAt: $s, endsAt: $e,
  ticketTypes: [{name:"General", priceCents:5000, currency:"USD", capacity:20}, {name:"VIP", priceCents:15000, currency:"USD", capacity:5}]}')"
call "$ADMIN_TOKEN" POST /catalog/v1/events "$EVENT_BODY"
expect 201
EVENT_ID="$(jq -r .id <<<"$RESP_BODY")"
TT_ID="$(jq -r '.ticketTypes[] | select(.name=="General") | .id' <<<"$RESP_BODY")"
echo "   event $EVENT_ID, ticket type $TT_ID"

call "$ADMIN_TOKEN" POST "/catalog/v1/events/$EVENT_ID/publish"
expect 200 503   # 503 = published but not announced yet: repeating the call re-announces
if [ "$RESP_CODE" = 503 ]; then
  echo "   announcement failed, repeating publish (idempotent)"; call "$ADMIN_TOKEN" POST "/catalog/v1/events/$EVENT_ID/publish"; expect 200
fi

# ---- 2 inventory -------------------------------------------------------------------
step "2. Wait for reservations to create inventory from EventPublished"
have_inventory() {
  call - GET "/reservations/v1/events/$EVENT_ID/inventory" >/dev/null
  [ "$RESP_CODE" = 200 ] && [ "$(jq '.ticketTypes | length' <<<"$RESP_BODY")" -ge 1 ]
}
poll "$POLL_SECONDS" "inventory for the event" have_inventory
available() { jq -r --arg t "$TT_ID" '.ticketTypes[] | select(.ticketTypeId==$t) | .available' <<<"$RESP_BODY"; }
echo "   available: $(available)"

# ---- 3 browse -----------------------------------------------------------------------
step "3. Anyone lists and reads the event"
call - GET "/catalog/v1/events?city=Lagos&limit=5"; expect 200
echo "   $(jq '.items | length' <<<"$RESP_BODY") event(s) listed"
call - GET "/catalog/v1/events/$EVENT_ID"; expect 200
echo "   status: $(jq -r .status <<<"$RESP_BODY")"

# ---- 4 hold ---------------------------------------------------------------------------
step "4. User places a hold (and repeats the request with the same Idempotency-Key)"
HOLD_BODY="$(jq -n --arg e "$EVENT_ID" --arg t "$TT_ID" '{eventId:$e, ticketTypeId:$t, quantity:2}')"
HOLD_KEY="$(uuid)"
call "$TOKEN" POST /reservations/v1/holds "$HOLD_BODY" -H "Idempotency-Key: $HOLD_KEY"; expect 201
HOLD_ID="$(jq -r .holdId <<<"$RESP_BODY")"
echo "   hold $HOLD_ID expires at $(jq -r .expiresAt <<<"$RESP_BODY")"
call "$TOKEN" POST /reservations/v1/holds "$HOLD_BODY" -H "Idempotency-Key: $HOLD_KEY"; expect 201
[ "$(jq -r .holdId <<<"$RESP_BODY")" = "$HOLD_ID" ] || { echo "   replay returned a different hold!" >&2; exit 1; }
echo "   replay returned the same hold (idempotent)"

# ---- 5 order -----------------------------------------------------------------------------
step "5. User turns the hold into an order (and repeats the request)"
ORDER_BODY="$(jq -n --arg h "$HOLD_ID" '{holdId:$h}')"
ORDER_KEY="$(uuid)"
call "$TOKEN" POST /orders/v1/orders "$ORDER_BODY" -H "Idempotency-Key: $ORDER_KEY"; expect 201
ORDER_ID="$(jq -r .id <<<"$RESP_BODY")"
echo "   order $ORDER_ID status $(jq -r .status <<<"$RESP_BODY"), amount $(jq -r .amountCents <<<"$RESP_BODY") cents"
call "$TOKEN" POST /orders/v1/orders "$ORDER_BODY" -H "Idempotency-Key: $ORDER_KEY"; expect 201
[ "$(jq -r .id <<<"$RESP_BODY")" = "$ORDER_ID" ] || { echo "   replay created another order!" >&2; exit 1; }

# ---- 6 payment ------------------------------------------------------------------------------
step "6. Wait for the simulated payment (about 80% succeed)"
order_settled() {
  call "$TOKEN" GET "/orders/v1/orders/$ORDER_ID" >/dev/null
  case "$(jq -r .status <<<"$RESP_BODY")" in paid|payment_failed|cancelled) return 0 ;; *) return 1 ;; esac
}
poll "$POLL_SECONDS" "the payment to settle" order_settled
STATUS="$(jq -r .status <<<"$RESP_BODY")"
echo "   order status: $STATUS"
call "$TOKEN" GET "/orders/v1/orders/$ORDER_ID/history"; expect 200
jq -r '.history[] | "   \(.at)  \(.status) \(.reason // "")"' <<<"$RESP_BODY"

if [ "$STATUS" != paid ]; then
  step "Payment failed: the tickets should return to the inventory"
  settled_back() { call - GET "/reservations/v1/events/$EVENT_ID/inventory" >/dev/null; [ "$(available)" = 20 ]; }
  poll "$POLL_SECONDS" "inventory restored" settled_back && echo "   available: $(available)"
  echo "Run the script again to see a successful payment."
  exit 0
fi

# ---- 7 ticket + notification --------------------------------------------------------------------
step "7. Ticket file and notification"
have_ticket() { call "$TOKEN" GET "/notifications/v1/orders/$ORDER_ID/ticket-url" >/dev/null; [ "$RESP_CODE" = 200 ]; }
poll "$POLL_SECONDS" "the ticket to be generated" have_ticket
echo "   ticket link (valid $(jq -r .expiresAt <<<"$RESP_BODY")): $(jq -r '.url | sub("\\?.*";"?...")' <<<"$RESP_BODY")"
call "$TOKEN" GET "/notifications/v1/me/notifications?limit=5"; expect 200
jq -r '.items[] | "   \(.createdAt)  \(.type)  \(.channel)  \(.status) \(.reason // "")"' <<<"$RESP_BODY"

# ---- 8 cancel ------------------------------------------------------------------------------------
step "8. User cancels the paid order; tickets return to the inventory"
call "$TOKEN" POST "/orders/v1/orders/$ORDER_ID/cancel" '{"reason":"golden path cleanup"}'; expect 200
echo "   order status: $(jq -r .status <<<"$RESP_BODY")"
call - GET "/reservations/v1/events/$EVENT_ID/inventory" >/dev/null
restored() { call - GET "/reservations/v1/events/$EVENT_ID/inventory" >/dev/null; [ "$(available)" = 20 ]; }
poll "$POLL_SECONDS" "inventory restored after the cancellation" restored
echo "   available: $(available) (back to capacity)"

# ---- optional: expiry -----------------------------------------------------------------------------
if [ "${WAIT_EXPIRY:-false}" = true ]; then
  step "9. (optional) An unused hold expires and returns its tickets"
  call "$TOKEN" POST /reservations/v1/holds "$HOLD_BODY" -H "Idempotency-Key: $(uuid)"; expect 201
  HOLD_ID="$(jq -r .holdId <<<"$RESP_BODY")"
  echo "   hold $HOLD_ID expires at $(jq -r .expiresAt <<<"$RESP_BODY"); available now: $(call - GET "/reservations/v1/events/$EVENT_ID/inventory" >/dev/null; available)"
  hold_expired() { call "$TOKEN" GET "/reservations/v1/holds/$HOLD_ID" >/dev/null; [ "$(jq -r .status <<<"$RESP_BODY")" = expired ]; }
  poll 900 "the hold to expire" hold_expired
  poll 120 "the tickets to come back" restored
  echo "   hold expired and tickets returned (available: $(available))"
fi

step "Golden path complete"

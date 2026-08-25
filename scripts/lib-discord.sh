load_discord_webhook() {
    DISCORD_WEBHOOK=""
    if [ -r /opt/pricegetter/discord-webhook.txt ]; then
        DISCORD_WEBHOOK=$(cat /opt/pricegetter/discord-webhook.txt)
    fi
}

notify_discord() {
    local message="$1"
    [ -n "${DISCORD_WEBHOOK:-}" ] || return 0
    local payload
    payload=$(jq -n --arg content "$message" '{content: $content}') || return 0
    curl -sS -m 10 -H "Content-Type: application/json" -d "$payload" "$DISCORD_WEBHOOK" >/dev/null 2>&1 || true
}

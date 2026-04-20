#!/usr/bin/env bash
# Egress firewall: allowlist-only outbound connections.
# Refreshed automatically every 15 min by cron (/etc/cron.d/docverse-firewall).
# Manual re-run: sudo /usr/local/bin/init-firewall.sh
set -euo pipefail

ALLOWED_DOMAINS=(
    # Anthropic (inference + auth + telemetry)
    api.anthropic.com
    console.anthropic.com
    statsig.anthropic.com
    sentry.io

    # GitHub
    github.com
    api.github.com
    objects.githubusercontent.com
    raw.githubusercontent.com
    codeload.github.com
    uploads.github.com

    # Python packages
    pypi.org
    files.pythonhosted.org

    # Node packages
    registry.npmjs.org

    # Docker Hub (for DinD image pulls)
    registry-1.docker.io
    auth.docker.io
    production.cloudflare.docker.com
)

resolve_to_ips() {
    local domain="$1"
    dig +short A "$domain" 2>/dev/null | grep -E '^[0-9]+\.' || true
    dig +short AAAA "$domain" 2>/dev/null | grep -E '^[0-9a-f]+:' || true
}

add_ip_to_set() {
    local ip="$1"
    if [[ "$ip" == *:* ]]; then
        ipset add allowed_ips6 "$ip" -exist
    else
        ipset add allowed_ips "$ip" -exist
    fi
}

add_cidr_to_set() {
    local cidr="$1"
    if [[ "$cidr" == *:* ]]; then
        ipset add allowed_nets6 "$cidr" -exist
    else
        ipset add allowed_nets "$cidr" -exist
    fi
}

echo "=== Initializing egress firewall ==="

# Create or flush ipsets (IPv4)
ipset create allowed_ips hash:ip -exist
ipset create allowed_nets hash:net -exist
ipset flush allowed_ips
ipset flush allowed_nets

# Create or flush ipsets (IPv6)
ipset create allowed_ips6 hash:ip family inet6 -exist
ipset create allowed_nets6 hash:net family inet6 -exist
ipset flush allowed_ips6
ipset flush allowed_nets6

# Always allow loopback and private networks (for DinD, container networking)
ipset add allowed_nets 127.0.0.0/8 -exist
ipset add allowed_nets 10.0.0.0/8 -exist
ipset add allowed_nets 172.16.0.0/12 -exist
ipset add allowed_nets 192.168.0.0/16 -exist

# Allow IPv6 loopback and link-local
ipset add allowed_nets6 ::1/128 -exist
ipset add allowed_nets6 fe80::/10 -exist

# Resolve allowed domains
for domain in "${ALLOWED_DOMAINS[@]}"; do
    ips=$(resolve_to_ips "$domain")
    for ip in $ips; do
        add_ip_to_set "$ip"
        echo "  Allowed: $domain -> $ip"
    done
done

# Fetch GitHub CIDRs from the meta API (best-effort)
if gh_meta=$(curl -sf --max-time 10 https://api.github.com/meta 2>/dev/null); then
    for key in web api git packages; do
        while IFS= read -r cidr; do
            [ -n "$cidr" ] && add_cidr_to_set "$cidr"
        done < <(echo "$gh_meta" | jq -r ".${key}[]? // empty" 2>/dev/null)
    done
    echo "  Added GitHub CIDR ranges from meta API"
fi

# Fetch Cloudflare edge CIDRs (Docker Hub CDN is on Cloudflare)
if cf_v4=$(curl -sf --max-time 10 https://www.cloudflare.com/ips-v4 2>/dev/null); then
    while IFS= read -r cidr; do
        [ -n "$cidr" ] && ipset add allowed_nets "$cidr" -exist
    done <<< "$cf_v4"
    echo "  Added Cloudflare IPv4 CIDRs"
fi
if cf_v6=$(curl -sf --max-time 10 https://www.cloudflare.com/ips-v6 2>/dev/null); then
    while IFS= read -r cidr; do
        [ -n "$cidr" ] && ipset add allowed_nets6 "$cidr" -exist
    done <<< "$cf_v6"
    echo "  Added Cloudflare IPv6 CIDRs"
fi

# Flush existing OUTPUT rules (idempotent re-run)
iptables -F OUTPUT 2>/dev/null || true
ip6tables -F OUTPUT 2>/dev/null || true

# Allow established connections
iptables -A OUTPUT -m state --state ESTABLISHED,RELATED -j ACCEPT
ip6tables -A OUTPUT -m state --state ESTABLISHED,RELATED -j ACCEPT

# Allow loopback
iptables -A OUTPUT -o lo -j ACCEPT
ip6tables -A OUTPUT -o lo -j ACCEPT

# Allow DNS (needed for resolution)
iptables -A OUTPUT -p udp --dport 53 -j ACCEPT
iptables -A OUTPUT -p tcp --dport 53 -j ACCEPT
ip6tables -A OUTPUT -p udp --dport 53 -j ACCEPT
ip6tables -A OUTPUT -p tcp --dport 53 -j ACCEPT

# Allow traffic to IPs in the allowlists
iptables -A OUTPUT -m set --match-set allowed_ips dst -j ACCEPT
iptables -A OUTPUT -m set --match-set allowed_nets dst -j ACCEPT
ip6tables -A OUTPUT -m set --match-set allowed_ips6 dst -j ACCEPT
ip6tables -A OUTPUT -m set --match-set allowed_nets6 dst -j ACCEPT

# Drop everything else with logging
iptables -A OUTPUT -j LOG --log-prefix "BLOCKED-EGRESS: " --log-level 4
iptables -A OUTPUT -j DROP
ip6tables -A OUTPUT -j LOG --log-prefix "BLOCKED-EGRESS6: " --log-level 4
ip6tables -A OUTPUT -j DROP

echo "=== Egress firewall active ==="
echo "Refreshed automatically every 15 min by cron (/etc/cron.d/docverse-firewall)."
echo "Manual re-run: sudo /usr/local/bin/init-firewall.sh"

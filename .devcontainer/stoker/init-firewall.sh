#!/usr/bin/env bash
# stoker-managed: sandbox:stoker/init-firewall.sh:8d95046b8cb62718
# Configure egress firewall for the stoker sandbox. Resolves every
# allowlisted hostname to its current IPs and installs an ipset that
# the iptables rules drop everything else against. Re-run periodically
# via firewall-refresh.cron to pick up DNS changes.
#
# Allowlist entries are either hostnames (resolved via getent ahosts)
# or CIDR ranges (lines containing '/', added directly). CIDR support
# matters for hosts behind anycast / round-robin DNS — github.com hands
# out a different /32 on each lookup, so a getent snapshot misses the IP
# the next git client will actually connect to. Seeding the allowlist
# with GitHub's published CIDRs covers every rotation in one shot.
set -euo pipefail

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ALLOWLIST="${THIS_DIR}/firewall-allowlist.txt"
EXTRA_ALLOWLIST="${STOKER_EXTRA_ALLOWLIST:-/etc/stoker/firewall-allowlist.extra.txt}"
SET_V4="stoker-allowed-v4"
SET_V6="stoker-allowed-v6"

# Ensure the parent of EXTRA_ALLOWLIST exists so `stoker sandbox
# firewall-refresh` can write the file before this script reads it.
mkdir -p /etc/stoker

if [ ! -r "${ALLOWLIST}" ]; then
    echo "stoker: missing firewall allowlist at ${ALLOWLIST}" >&2
    exit 1
fi

if ! command -v ipset >/dev/null 2>&1; then
    echo "stoker: ipset is required but missing; the sandbox image must install it" >&2
    exit 1
fi

if ! command -v ip6tables >/dev/null 2>&1; then
    echo "stoker: ip6tables is required but missing; the sandbox image must install it" >&2
    exit 1
fi

# rebuild_if_wrong_type detects a leftover ipset whose existing type
# differs from the `hash:net` we install below. `ipset create -exist`
# silently no-ops against a different-type set, so a pre-`ba7d1ad`
# sandbox that created `hash:ip` sets would otherwise keep them
# indefinitely — the CIDR `add` calls then fail under `add_entry`'s
# guard and the firewall stays stuck on a snapshot of one rotation.
# Flushing the matching iptables (or ip6tables) OUTPUT chain first
# releases the kernel reference so `ipset destroy` can succeed; the
# chain is repopulated from scratch below either way, so flushing
# here is no-op-friendly.
#
# `STOKER_FIREWALL_RESET=1` (the env-var contract `stoker sandbox
# firewall-refresh --reset` uses) forces the same flush/destroy path
# even when the existing set already matches `desired`, so operators
# can recover from any wedged-ipset state — not just the historical
# type mismatch — without rebuilding the whole sandbox.
rebuild_if_wrong_type() {
    local name="$1" desired="$2" tables="$3" current=""
    if ! current="$(ipset list -t "${name}" 2>/dev/null \
            | awk '/^Type:/ { print $2; exit }')"; then
        current=""
    fi
    if [ -z "${current}" ]; then
        return 0
    fi
    if [ "${current}" != "${desired}" ]; then
        echo "stoker: rebuilding ipset ${name} (was ${current}, want ${desired})" >&2
    elif [ "${STOKER_FIREWALL_RESET:-0}" = "1" ]; then
        echo "stoker: resetting ipset ${name} (STOKER_FIREWALL_RESET=1)" >&2
    else
        return 0
    fi
    "${tables}" -F OUTPUT || true
    ipset destroy "${name}"
}

rebuild_if_wrong_type "${SET_V4}" hash:net iptables
rebuild_if_wrong_type "${SET_V6}" hash:net ip6tables

# `hash:net` accepts both bare /32 (or /128) addresses and CIDR
# ranges, so the same set holds per-host getent results and the
# CIDRs we read straight from the allowlist.
ipset create -exist "${SET_V4}" hash:net family inet timeout 86400
ipset create -exist "${SET_V6}" hash:net family inet6 timeout 86400

resolved_count=0

# fetch_meta_cidrs pulls GitHub's currently-published CIDR ranges from
# https://api.github.com/meta and merges the `web api git packages`
# arrays into the v4 / v6 ipsets — the live source of truth for which
# anycast ranges GitHub is handing out today. The static CIDR block in
# `firewall-allowlist.txt` is a fallback used when this function can't
# get a usable answer (no network, parse error, `gh`/`curl`/`jq`
# missing, etc.). Best-effort: any failure here returns 1 silently and
# the script falls through to the static path; only the success branch
# emits the `stoker: fetched /meta — added N CIDRs` stderr line that
# the manual-QA checklist greps for.
#
# `actions` is deliberately not consumed — it lists Azure's full
# egress and would balloon the set without any matching call site
# from inside the sandbox.
fetch_meta_cidrs() {
    command -v jq >/dev/null 2>&1 || return 1
    local body=""
    if command -v gh >/dev/null 2>&1; then
        body="$(gh api meta 2>/dev/null)" || body=""
    fi
    if [ -z "${body}" ] && command -v curl >/dev/null 2>&1; then
        body="$(curl -sS https://api.github.com/meta 2>/dev/null)" || body=""
    fi
    if [ -z "${body}" ]; then
        return 1
    fi
    local cidrs=""
    if ! cidrs="$(printf '%s' "${body}" | jq -r '
        (.web // []) + (.api // []) + (.git // []) + (.packages // [])
        | unique
        | .[]
    ' 2>/dev/null)"; then
        return 1
    fi
    if [ -z "${cidrs}" ]; then
        return 1
    fi
    local added=0 entry=""
    while IFS= read -r entry; do
        [ -z "${entry}" ] && continue
        case "${entry}" in
            *:*) ipset add -exist "${SET_V6}" "${entry}" timeout 86400 ;;
            *)   ipset add -exist "${SET_V4}" "${entry}" timeout 86400 ;;
        esac
        added=$((added + 1))
    done <<<"${cidrs}"
    if [ "${added}" -eq 0 ]; then
        return 1
    fi
    echo "stoker: fetched /meta — added ${added} CIDRs" >&2
    resolved_count=$((resolved_count + added))
    return 0
}

fetch_meta_cidrs || true

# resolve_into_set returns 0 if at least one address was found for the
# host, 1 if both v4 and v6 lookups came back empty (NXDOMAIN, transient
# DNS failure, etc). Failures are caught locally so a single bad host
# can't take down the whole script under `set -euo pipefail`.
resolve_into_set() {
    local host="$1" v4="" v6=""
    if ! v4="$(getent ahosts "${host}" | awk '/STREAM/ && $1 !~ /:/ { print $1 }' | sort -u)"; then
        v4=""
    fi
    if ! v6="$(getent ahosts "${host}" | awk '/STREAM/ &&  $1 ~ /:/ { print $1 }' | sort -u)"; then
        v6=""
    fi
    if [ -z "${v4}" ] && [ -z "${v6}" ]; then
        echo "stoker: DNS lookup failed for ${host}" >&2
        return 1
    fi
    while IFS= read -r a; do
        [ -z "${a}" ] && continue
        ipset add -exist "${SET_V4}" "${a}" timeout 86400
    done <<<"${v4}"
    while IFS= read -r a; do
        [ -z "${a}" ] && continue
        ipset add -exist "${SET_V6}" "${a}" timeout 86400
    done <<<"${v6}"
    return 0
}

# add_entry handles a single allowlist line: a CIDR is added directly
# to the v4 or v6 set (chosen by whether the entry contains a colon),
# anything else is treated as a hostname and resolved via getent.
add_entry() {
    local entry="$1"
    case "${entry}" in
        */*)
            case "${entry}" in
                *:*) ipset add -exist "${SET_V6}" "${entry}" timeout 86400 ;;
                *)   ipset add -exist "${SET_V4}" "${entry}" timeout 86400 ;;
            esac
            return 0
            ;;
    esac
    resolve_into_set "${entry}"
}

while IFS= read -r entry; do
    [ -z "${entry}" ] && continue
    case "${entry}" in \#*) continue ;; esac
    if add_entry "${entry}"; then
        resolved_count=$((resolved_count + 1))
    fi
done < "${ALLOWLIST}"

if [ -r "${EXTRA_ALLOWLIST}" ]; then
    while IFS= read -r entry; do
        [ -z "${entry}" ] && continue
        case "${entry}" in \#*) continue ;; esac
        if add_entry "${entry}"; then
            resolved_count=$((resolved_count + 1))
        fi
    done < "${EXTRA_ALLOWLIST}"
fi

# Refuse to install rules against an empty allowlist: the iptables
# REJECT default would deny all egress, which is worse than aborting.
if [ "${resolved_count}" -eq 0 ]; then
    echo "stoker: no allowlisted hosts resolved — refusing to install firewall rules" >&2
    exit 1
fi

# The LOG rules immediately before each REJECT give operators the
# destination IP of any dropped packet via `journalctl -k | grep
# STOKER-EGRESS-DROP`. The chain is flushed above, so re-running the
# script naturally yields exactly one LOG rule per family.
iptables -F OUTPUT || true
iptables -A OUTPUT -m set --match-set "${SET_V4}" dst -j ACCEPT
iptables -A OUTPUT -o lo -j ACCEPT
iptables -A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
iptables -A OUTPUT -p udp --dport 53 -j ACCEPT
iptables -A OUTPUT -p tcp --dport 53 -j ACCEPT
iptables -A OUTPUT -j LOG --log-prefix "STOKER-EGRESS-DROP: "
iptables -A OUTPUT -j REJECT --reject-with icmp-net-prohibited

ip6tables -F OUTPUT || true
ip6tables -A OUTPUT -m set --match-set "${SET_V6}" dst -j ACCEPT
ip6tables -A OUTPUT -o lo -j ACCEPT
ip6tables -A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
ip6tables -A OUTPUT -p udp --dport 53 -j ACCEPT
ip6tables -A OUTPUT -p tcp --dport 53 -j ACCEPT
ip6tables -A OUTPUT -p icmpv6 -j ACCEPT
ip6tables -A OUTPUT -j LOG --log-prefix "STOKER-EGRESS-DROP: "
ip6tables -A OUTPUT -j REJECT --reject-with icmp6-adm-prohibited

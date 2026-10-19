#!/usr/bin/env bash
# Add/remove the fake devices' IP aliases on a NetworkManager connection.
#
#   scripts/ip-aliases.sh add    /etc/tuya-hub-proxy/config.yaml "Wired connection 1" [prefix]
#   scripts/ip-aliases.sh remove /etc/tuya-hub-proxy/config.yaml "Wired connection 1" [prefix]
#   scripts/ip-aliases.sh show   /etc/tuya-hub-proxy/config.yaml
#
# Dry run by default: prints the nmcli commands.  Set APPLY=1 to execute them.
# The IPs must be outside the router's DHCP pool.  Works with ipv4.method auto
# (DHCP address + static extra addresses) and with manual.
set -euo pipefail

action=${1:?add|remove|show}
config=${2:?config file}
conn=${3:-}
prefix=${4:-24}
python=${PYTHON:-python3}

ips=$("$python" - "$config" <<'PY'
import sys, yaml
cfg = yaml.safe_load(open(sys.argv[1]))
print(" ".join(c["bind_ip"] for c in cfg.get("children", [])))
PY
)

if [[ $action == show ]]; then
  for ip in $ips; do echo "$ip"; done
  exit 0
fi
[[ -n $conn ]] || { echo "connection name required (see: nmcli -t -f NAME con show)" >&2; exit 2; }

case $action in
  add) sign="+" ;;
  remove) sign="-" ;;
  *) echo "unknown action $action" >&2; exit 2 ;;
esac

run() {
  local q; q=$(printf '%q ' "$@")
  if [[ ${APPLY:-0} == 1 ]]; then echo "+ $q"; "$@"; else echo "$q"; fi
}

for ip in $ips; do
  run nmcli connection modify "$conn" "${sign}ipv4.addresses" "$ip/$prefix"
done
# reapply without bouncing the link (keeps an SSH session alive)
dev=$(nmcli -g GENERAL.DEVICES connection show "$conn" 2>/dev/null || true)
if [[ -n $dev ]]; then
  run nmcli device reapply "$dev"
else
  echo "# connection not active; changes apply on next activation" >&2
fi
[[ ${APPLY:-0} == 1 ]] || echo "# dry run -- re-run with APPLY=1 to execute" >&2

#!/usr/bin/env bash
# Add/remove the fake devices' IP aliases on a NetworkManager connection.
#
#   scripts/ip-aliases.sh add    /etc/tuya-hub-proxy/config.yaml "Wired connection 1" [prefix=32]
#   scripts/ip-aliases.sh remove /etc/tuya-hub-proxy/config.yaml "Wired connection 1" [prefix=32]
#   scripts/ip-aliases.sh show   /etc/tuya-hub-proxy/config.yaml
#
# Dry run by default: prints the nmcli commands.  Set APPLY=1 to execute them.
# The IPs must be outside the router's DHCP pool.  Works with ipv4.method auto
# (DHCP address + static extra addresses) and with manual.
#
# Aliases are added as /32 on purpose: with /24 NetworkManager installs them
# before the DHCP address, the first alias becomes the primary source address
# of the LAN route, and the host starts talking to the LAN as the fake device.
#
# If `nmcli device reapply` does not apply the change (seen on a bridge with
# NetworkManager 1.52), `nmcli connection up <conn>` does -- but on a bridge
# that also carries libvirt VMs it detaches their vnetN taps; re-attach them
# with `ip link set vnetN master <bridge>`.
set -euo pipefail

action=${1:?add|remove|show}
config=${2:?config file}
conn=${3:-}
prefix=${4:-32}
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
  if [[ ${APPLY:-0} == 1 && $action == add ]]; then
    sleep 2
    for ip in $ips; do
      ip -4 addr show dev "$dev" | grep -q " $ip/" || echo "WARNING: $ip not on $dev after reapply (see header comment)" >&2
    done
  fi
else
  echo "# connection not active; changes apply on next activation" >&2
fi
[[ ${APPLY:-0} == 1 ]] || echo "# dry run -- re-run with APPLY=1 to execute" >&2

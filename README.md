# tuya-hub-proxy

Expose the Zigbee sub-devices of a Tuya gateway as **stand-alone Tuya Wi-Fi
devices (protocol 3.3)**, so Home Assistant's
[tuya-local](https://github.com/make-all/tuya-local) can control each of them
independently — through a **single** connection to the gateway.

> Português: veja [README.pt-BR.md](README.pt-BR.md).

## The problem

tuya-local supports gateway sub-devices (`cid` / "sub device id"), but each
configured sub-device drives the shared gateway socket from its own receive
loop. With several sub-devices on one hub, these loops read and renegotiate on
the same connection concurrently; the 3.4 session negotiation gets interleaved
and the hub answers with garbage, which tinytuya reports as **error 914**
("Check device key or version"). The loops back off, retry together, and the
integration ends up in a 914 loop where no device works. See tuya-local issues
[#6265](https://github.com/make-all/tuya-local/issues/6265),
[#6409](https://github.com/make-all/tuya-local/issues/6409) and
[#6316](https://github.com/make-all/tuya-local/issues/6316).

## How it works

```
 Home Assistant (tuya-local)                tuya-hub-proxy                       Tuya Zigbee gateway
 ---------------------------      ----------------------------------------     -------------------
 entry "Front door"  --3.3-->  fake device  192.0.2.20:6668 \
 entry "Back door"   --3.3-->  fake device  192.0.2.21:6668  |-- DP cache --- ONE 3.4 session ---> hub
 entry "Hall switch" --3.3-->  fake device  192.0.2.22:6668 /   (on disk)    (one thread, queued    (cid per child)
                                                                              sends, heartbeat,
                                                                              backoff, 914 handling)
```

* **Upstream** — one `tinytuya.Device` for the gateway (protocol 3.4) plus one
  child `tinytuya.Device(cid=node_id, parent=gateway)` per sub-device. A single
  worker thread owns the socket: it connects/negotiates, sends queued commands,
  heartbeats and receives. Two negotiations or two concurrent reads cannot
  happen by construction. Pushes are dispatched by `cid`. Failed sessions are
  closed and retried with exponential backoff (with a higher floor after a 914).
  After every (re)connection all children are re-polled.
* **Downstream** — per sub-device, a fake device listens on its own IP
  (`bind_ip:6668`), with the sub-device's real device id and a local key *you*
  generate. It implements the device side of protocol 3.3: 55AA framing,
  CRC32, AES-128-ECB, version header, and the commands `DP_QUERY` (answers from
  cache), `CONTROL` (forwarded to the hub), `HEART_BEAT`, `UPDATEDPS`, and
  spontaneous `STATUS` pushes whenever the hub reports a change. All framing
  and crypto use tinytuya's own functions, so bytes match what tinytuya
  clients expect.
* **Cache** — last known DPs per child, persisted to disk. Sleepy sensors that
  do not answer a poll keep their last value. A per-kind default (e.g. DP 101
  `false` for door sensors) is announced until the hub reports the real value,
  so tuya-local can detect the device type even for a sensor that has not sent
  an event yet.
* **Observability** — JSON logs on stdout (journald), and
  `GET http://127.0.0.1:8765/health` with upstream state, per-child last
  update/push, connected clients and error counters (503 while the hub is down).
  Keys are never logged.

## Requirements

* Linux host on the same LAN as Home Assistant (tested on Python 3.11–3.14;
  Debian 13 ships 3.13), with NetworkManager if you use the alias script.
* The gateway's device id, IP and local key (e.g. `devices.json` from
  `python -m tinytuya wizard`). Give the gateway a DHCP reservation.
* One free static IP per sub-device, **outside the router's DHCP pool**.
* Nothing else may talk to the gateway locally (remove the gateway and its
  sub-devices from tuya-local/LocalTuya first).

## Installation

```bash
# 1. install
sudo python3 -m venv /opt/tuya-hub-proxy/venv
sudo /opt/tuya-hub-proxy/venv/bin/pip install git+https://github.com/OWNER/tuya-hub-proxy

# 2. generate the config (fake keys are random; nothing secret is printed)
sudo /opt/tuya-hub-proxy/venv/bin/tuya-hub-proxy-genconfig \
    --devices-json ~/.config/tuya/devices.json \
    --hub-id <gateway device id> --hub-ip <gateway ip> \
    --first-ip 192.0.2.20 -o /etc/tuya-hub-proxy/config.yaml
sudoedit /etc/tuya-hub-proxy/config.yaml     # review names / kinds / IPs

# 3. IP aliases (dry run first, then APPLY=1)
scripts/ip-aliases.sh add /etc/tuya-hub-proxy/config.yaml "Wired connection 1"
sudo APPLY=1 scripts/ip-aliases.sh add /etc/tuya-hub-proxy/config.yaml "Wired connection 1"

# 4. service
sudo cp packaging/tuya-hub-proxy.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now tuya-hub-proxy
journalctl -u tuya-hub-proxy -f
curl -s http://127.0.0.1:8765/health
```

Aliases are added as `/32` so the host keeps its own address as the source of
LAN traffic. If `nmcli device reapply` does not bring them up (seen on a bridge
with NetworkManager 1.52), use `nmcli connection up <conn>`; on a bridge that
also carries libvirt VMs this detaches their `vnetN` taps — re-attach with
`ip link set vnetN master <bridge>`.

`kind` controls the announced defaults: `door_sensor` (DP 101), `switch`
(DP 1) or `generic` (none). Extra `defaults` can be set per child.

## Adding the devices to tuya-local

For each child, add a tuya-local device **manually** (not via the cloud):

| Field | Value |
|---|---|
| Host | the child's `bind_ip` |
| Device ID | the child's `device_id` (the sub-device's own id) |
| Local key | the child's `fake_local_key` |
| Protocol version | `3.3` |
| Sub device ID (cid) | leave **empty** — each fake is a plain Wi-Fi device |
| Poll only | off |

Pick the type tuya-local suggests: *Door sensor* (`tuya_smart_zigbee_door_sensor`,
DP 101 contact, 103 battery, 102 tamper) for door/window sensors, *Simple
switch* (DP 1) for single-gang switches.

## Rolling back

1. `sudo systemctl disable --now tuya-hub-proxy`
2. `sudo APPLY=1 scripts/ip-aliases.sh remove /etc/tuya-hub-proxy/config.yaml "Wired connection 1"`
3. Delete the proxy-backed entries from tuya-local in Home Assistant.
4. Re-add the gateway in your previous integration (e.g. LocalTuya with the
   hub's real key and each sub-device's cid). The proxy changes nothing on the
   gateway itself.

## Limitations and known risks

* **Protocol coverage**: only what simple devices need (query, control,
  heartbeat, updatedps, status push). No 3.4/3.5 downstream, no discovery
  broadcast (add devices manually by IP), no device22 quirks beyond basic
  `CONTROL_NEW` queries.
* **Optimistic acks**: a `CONTROL` is acked once the command is queued/sent to
  the hub; the real state arrives later as a push. If the hub is offline the
  command waits up to `command_timeout` and is then dropped.
* **Sleepy sensors** report only on events; the cached value may be stale
  after the proxy was down while a door changed. The battery DP may be absent
  until the sensor reports it.
* **Single upstream**: the proxy must be the only local client of the gateway
  (the Tuya/Smart Life app via cloud is fine).
* Gateway push format variants are handled (`{"cid", "dps"}` and
  `{"data": {"cid", "dps"}}`), but so far the upstream side has only been
  tested against the bundled gateway simulator — field reports welcome.

## Development

```bash
python -m venv .venv && .venv/bin/pip install -e ".[test]"
.venv/bin/pytest -v
# optional: run tuya-local's own TuyaLocalDevice against the proxy (Python 3.14+)
git clone --depth 1 https://github.com/make-all/tuya-local /tmp/tuya-local
TUYA_LOCAL_PATH=/tmp/tuya-local .venv/bin/pytest -v tests/test_tuya_local_integration.py
```

The test suite needs no hardware: `tests/mock_gateway.py` simulates a 3.4
gateway (including the session-key negotiation and forced 914 failures), and
real `tinytuya.Device` clients talk to the fake devices.

## License

MIT — see [LICENSE](LICENSE). Uses [tinytuya](https://github.com/jasonacox/tinytuya) (MIT).
Not affiliated with Tuya.

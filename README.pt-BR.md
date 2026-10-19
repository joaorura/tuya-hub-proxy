# tuya-hub-proxy (português)

Expõe os sub-dispositivos Zigbee de um hub (gateway) Tuya como **dispositivos
Tuya Wi-Fi independentes (protocolo 3.3)**, para que a integração
[tuya-local](https://github.com/make-all/tuya-local) do Home Assistant controle
cada um separadamente — usando **uma única** conexão com o hub.

> English: [README.md](README.md).

## O problema

O tuya-local aceita sub-dispositivos de gateway (campo `cid` / "sub device
id"), mas cada sub-dispositivo cadastrado roda o próprio loop de leitura sobre
o socket compartilhado do hub. Com vários sub-dispositivos no mesmo hub, esses
loops leem e renegociam a sessão na mesma conexão ao mesmo tempo; a negociação
3.4 se embaralha, o hub responde lixo e o tinytuya reporta **erro 914**
("Check device key or version"). Os loops recuam, tentam de novo juntos, e a
integração fica presa num ciclo de 914 em que nenhum dispositivo funciona. Ver
as issues do tuya-local
[#6265](https://github.com/make-all/tuya-local/issues/6265),
[#6409](https://github.com/make-all/tuya-local/issues/6409) e
[#6316](https://github.com/make-all/tuya-local/issues/6316).

## Como funciona

```
 Home Assistant (tuya-local)               tuya-hub-proxy                      Hub Zigbee Tuya
 ---------------------------     ----------------------------------------    ----------------
 "Porta frente"   --3.3-->  falso 192.0.2.20:6668 \
 "Porta fundos"   --3.3-->  falso 192.0.2.21:6668  |-- cache de DPs --- UMA sessão 3.4 ---> hub
 "Interruptor"    --3.3-->  falso 192.0.2.22:6668 /   (em disco)       (uma thread, fila de  (cid por filho)
                                                                        envio, heartbeat,
                                                                        backoff, trata 914)
```

* **Upstream** — um `tinytuya.Device` do gateway (protocolo 3.4) e um filho
  `tinytuya.Device(cid=node_id, parent=gateway)` por sub-dispositivo. Uma única
  thread é dona do socket: conecta/negocia, envia os comandos da fila, manda
  heartbeat e recebe. Duas negociações ou duas leituras concorrentes são
  impossíveis por construção. Os pushes são despachados pelo `cid`. Sessão que
  falha é fechada e refeita com backoff exponencial (com piso maior depois de
  um 914). A cada (re)conexão todos os filhos são consultados de novo.
* **Downstream** — para cada sub-dispositivo, um dispositivo falso escuta no
  próprio IP (`bind_ip:6668`), com o device id real do filho e uma local key
  gerada por você. Implementa o lado dispositivo do protocolo 3.3: quadros
  55AA, CRC32, AES-128-ECB, cabeçalho de versão e os comandos `DP_QUERY`
  (responde do cache), `CONTROL` (repassa ao hub), `HEART_BEAT`, `UPDATEDPS` e
  `STATUS` espontâneo quando o hub reporta mudança. Empacotamento e
  criptografia usam as funções do próprio tinytuya, então os bytes batem com o
  que o cliente espera.
* **Cache** — últimos DPs conhecidos por filho, persistidos em disco. Sensor
  que "dorme" e não responde mantém o último valor. Um padrão por tipo (ex.: DP
  101 `false` para sensor de porta) é anunciado até o hub reportar o valor real,
  para o tuya-local reconhecer o tipo mesmo antes do primeiro evento.
* **Observabilidade** — logs JSON no stdout (journald) e
  `GET http://127.0.0.1:8765/health` com estado do upstream, último
  update/push por filho, clientes conectados e contadores de erro (503 enquanto
  o hub estiver fora). Chaves nunca vão para o log.

## Requisitos

* Host Linux na mesma rede do Home Assistant (testado em Python 3.11–3.14; o
  Debian 13 traz 3.13), com NetworkManager se for usar o script de aliases.
* Device id, IP e local key do hub (ex.: `devices.json` do
  `python -m tinytuya wizard`). Reserve o IP do hub no DHCP.
* Um IP estático livre por sub-dispositivo, **fora do pool DHCP do roteador**.
* Nenhum outro cliente local pode falar com o hub (remova antes o hub e os
  filhos do tuya-local/LocalTuya).

## Instalação

```bash
# 1. instalar
sudo python3 -m venv /opt/tuya-hub-proxy/venv
sudo /opt/tuya-hub-proxy/venv/bin/pip install git+https://github.com/OWNER/tuya-hub-proxy

# 2. gerar o config (as chaves falsas são aleatórias; nada secreto é impresso)
sudo /opt/tuya-hub-proxy/venv/bin/tuya-hub-proxy-genconfig \
    --devices-json ~/.config/tuya/devices.json \
    --hub-id <device id do hub> --hub-ip <ip do hub> \
    --first-ip 192.0.2.20 -o /etc/tuya-hub-proxy/config.yaml
sudoedit /etc/tuya-hub-proxy/config.yaml     # revisar nomes / kind / IPs

# 3. aliases de IP (primeiro em modo de simulação, depois APPLY=1)
scripts/ip-aliases.sh add /etc/tuya-hub-proxy/config.yaml "Wired connection 1"
sudo APPLY=1 scripts/ip-aliases.sh add /etc/tuya-hub-proxy/config.yaml "Wired connection 1"

# 4. serviço
sudo cp packaging/tuya-hub-proxy.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now tuya-hub-proxy
journalctl -u tuya-hub-proxy -f
curl -s http://127.0.0.1:8765/health
```

O script de aliases roda `nmcli connection modify "<conexão>" +ipv4.addresses
<ip>/32` para cada filho e depois `nmcli device reapply` (sem derrubar o link,
então a sessão SSH sobrevive). Os aliases são `/32` para o host continuar usando
o próprio IP como origem do tráfego na LAN. Se o `reapply` não aplicar (visto
numa bridge com NetworkManager 1.52), use `nmcli connection up <conexão>`; numa
bridge que também atende VMs do libvirt isso solta as interfaces `vnetN` — religue
com `ip link set vnetN master <bridge>`.

## Cadastro no tuya-local

Para cada filho, adicione um dispositivo no tuya-local **manualmente** (sem
nuvem):

| Campo | Valor |
|---|---|
| Host | `bind_ip` do filho |
| Device ID | `device_id` do filho (o id do próprio sub-dispositivo) |
| Local key | `fake_local_key` do filho |
| Versão do protocolo | `3.3` |
| Sub device ID (cid) | **vazio** — cada falso é um Wi-Fi comum |
| Poll only | desligado |

Escolha o tipo que o tuya-local sugerir: *Door sensor*
(`tuya_smart_zigbee_door_sensor`: DP 101 contato, 103 bateria, 102 tamper) para
sensores de porta/janela, *Simple switch* (DP 1) para interruptores de um botão.

## Como voltar atrás (ex.: para o LocalTuya)

1. `sudo systemctl disable --now tuya-hub-proxy`
2. `sudo APPLY=1 scripts/ip-aliases.sh remove /etc/tuya-hub-proxy/config.yaml "Wired connection 1"`
3. Apague do tuya-local as entradas que apontavam para os falsos.
4. Cadastre de novo o hub na integração anterior (ex.: LocalTuya com a local key
   real do hub e o cid de cada filho). O proxy não altera nada no hub.

## Limitações e riscos conhecidos

* **Cobertura do protocolo**: só o que dispositivos simples precisam (consulta,
  controle, heartbeat, updatedps, push de status). Sem 3.4/3.5 no downstream,
  sem broadcast de descoberta (cadastro manual por IP), sem as variações
  "device22" além de consultas `CONTROL_NEW` básicas.
* **Ack otimista**: o `CONTROL` é confirmado quando o comando entra na fila /
  sai para o hub; o estado real chega depois como push. Com o hub fora, o
  comando espera até `command_timeout` e é descartado.
* **Sensores que dormem** só reportam em evento; se a porta mudou enquanto o
  proxy estava parado, o cache fica defasado até o próximo evento. O DP de
  bateria pode faltar até o sensor reportá-lo.
* **Upstream único**: o proxy precisa ser o único cliente local do hub (o app
  Tuya/Smart Life via nuvem não atrapalha).
* As duas variantes de push do hub são tratadas (`{"cid", "dps"}` e
  `{"data": {"cid", "dps"}}`), mas até agora o upstream só foi testado contra o
  simulador incluído.

## Desenvolvimento

```bash
python -m venv .venv && .venv/bin/pip install -e ".[test]"
.venv/bin/pytest -v
# opcional: roda o TuyaLocalDevice do próprio tuya-local contra o proxy (Python 3.14+)
git clone --depth 1 https://github.com/make-all/tuya-local /tmp/tuya-local
TUYA_LOCAL_PATH=/tmp/tuya-local .venv/bin/pytest -v tests/test_tuya_local_integration.py
```

Os testes não precisam de hardware: `tests/mock_gateway.py` simula um hub 3.4
(inclusive a negociação de chave de sessão e falhas 914 forçadas) e clientes
`tinytuya.Device` reais conversam com os dispositivos falsos.

## Licença

MIT — ver [LICENSE](LICENSE). Usa o [tinytuya](https://github.com/jasonacox/tinytuya) (MIT).
Sem vínculo com a Tuya.

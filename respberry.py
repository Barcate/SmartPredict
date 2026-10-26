"""
Raspberry Pi — Receptor MQTT + Painel Web
Projeto SENAI — SmartPredict

Funções:
1. Conectar ao HiveMQ Cloud via MQTT/TLS
2. Receber dados do ESP32-C3
3. Ler o BME280 local via I2C
4. Salvar leituras completas em CSV para treinamento da IA
5. Descartar leituras incompletas
6. Manter o SQLite preparado, mas desativado
7. NOVO: servir um painel web na rede local com:
   - gráficos de todos os sensores
   - botões para iniciar / parar a gravação do CSV
   - botão para baixar o CSV
   - terminal ao vivo (tudo o que o programa imprime)
"""

import csv
import json
import os
import ssl
import socket
import sqlite3
import math
import threading
import logging
import hmac
import ipaddress
from collections import deque
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request, send_file


# ============================================================
# CONFIGURAÇÕES
# ============================================================

DIRETORIO_PROJETO = Path("/home/christopher/projeto-preditivo")
DIRETORIO_PROJETO.mkdir(parents=True, exist_ok=True)

load_dotenv(DIRETORIO_PROJETO / ".env")

BROKER = os.getenv("MQTT_BROKER", "").strip()
PORTA = int(os.getenv("MQTT_PORT", "8883"))
USUARIO = os.getenv("MQTT_USUARIO", "").strip()
SENHA = os.getenv("MQTT_SENHA", "")

# Painel web
WEB_PORTA = int(os.getenv("WEB_PORT", "5000"))
# Endereço do painel da própria ESP32 (ex.: http://192.168.0.50). Vazio = oculto.
ESP32_PANEL_URL = os.getenv("ESP32_PANEL_URL", "").strip()
# Login do painel (OBRIGATÓRIO se for acessar pela internet). Senha vazia = sem login.
WEB_USUARIO = os.getenv("WEB_USUARIO", "admin").strip()
WEB_SENHA = os.getenv("WEB_SENHA", "")
# "1" = já começa gravando (comportamento antigo); "0" = começa pausado
SALVAR_AO_INICIAR = False  # começa pausado; iniciar pelo painel

TOPICO_ESP32 = "senai/manutencao/maquina01/sensores"

ARQUIVO_CSV = DIRETORIO_PROJETO / "dados_treinamento.csv"
BANCO = str(DIRETORIO_PROJETO / "dados_sensores.db")

I2C_BUS = 1
BME280_ENDERECO = 0x76

HISTORICO_MAX = 300   # pontos mantidos em memória para os gráficos
LOGS_MAX = 500        # linhas do terminal mantidas em memória


# ============================================================
# ESTADO COMPARTILHADO (thread MQTT <-> servidor web)
# ============================================================

estado_lock = threading.Lock()

salvando = SALVAR_AO_INICIAR
esp32_ip = None   # preenchido automaticamente com o IP enviado pela ESP32
mqtt_conectado = False
linhas_salvas_sessao = 0

historico = deque()  # histórico completo carregado do CSV + novas leituras
logs = deque(maxlen=LOGS_MAX)
log_contador = 0

ultima_leitura_esp32 = {
    "mic_nivel": None,
    "temperatura_analogica": None,
    "vibracao": None,
    "recebido_em": None,
}


def log(mensagem):
    """Imprime no terminal E guarda para o terminal do site."""

    global log_contador

    print(mensagem, flush=True)

    with estado_lock:
        log_contador += 1
        logs.append({
            "id": log_contador,
            "t": datetime.now().strftime("%H:%M:%S"),
            "msg": str(mensagem).strip("\n"),
        })


# ============================================================
# SQLITE — PREPARADO, MAS DESATIVADO
# ============================================================

def criar_banco():
    """Cria a tabela SQLite para uso futuro."""

    conexao = sqlite3.connect(BANCO)

    try:
        conexao.execute("""
            CREATE TABLE IF NOT EXISTS leituras (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                temperatura_ambiente REAL,
                umidade REAL,
                pressao REAL,
                mic_nivel REAL,
                temperatura_maquina REAL,
                vibracao_x REAL,
                vibracao_y REAL,
                vibracao_z REAL
            )
        """)
        conexao.commit()
        log(f"[SQLITE] Banco pronto: {BANCO}")
    finally:
        conexao.close()


def salvar_sqlite(leitura):
    """Salva uma leitura completa no SQLite, se ativado."""

    ambiente = leitura["ambiente"]
    maquina = leitura["maquina"]
    vibracao = maquina.get("vibracao") or {}

    conexao = sqlite3.connect(BANCO)

    try:
        conexao.execute("""
            INSERT INTO leituras (
                timestamp, temperatura_ambiente, umidade, pressao,
                mic_nivel, temperatura_maquina,
                vibracao_x, vibracao_y, vibracao_z
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            leitura["timestamp"],
            ambiente.get("temperatura"),
            ambiente.get("umidade"),
            ambiente.get("pressao"),
            maquina.get("mic_nivel"),
            maquina.get("temperatura_analogica"),
            vibracao.get("x"),
            vibracao.get("y"),
            vibracao.get("z"),
        ))
        conexao.commit()
    finally:
        conexao.close()


# ============================================================
# BME280 — SENSOR AMBIENTE NA RASPBERRY PI
# ============================================================

def ler_bme280():
    bus = None

    try:
        import smbus2
        import bme280

        bus = smbus2.SMBus(I2C_BUS)

        parametros = bme280.load_calibration_params(
            bus, BME280_ENDERECO
        )

        dados = bme280.sample(
            bus, BME280_ENDERECO, parametros
        )

        return {
            "temperatura": round(dados.temperature, 2),
            "umidade": round(dados.humidity, 2),
            "pressao": round(dados.pressure, 2),
        }

    except Exception as erro:
        log(f"[BME280] Erro ao ler sensor: {erro}")

        return {
            "temperatura": None,
            "umidade": None,
            "pressao": None,
        }

    finally:
        if bus is not None:
            bus.close()


# ============================================================
# MONTAR LEITURA
# ============================================================

def montar_leitura():
    return {
        "timestamp": datetime.now().astimezone().isoformat(
            timespec="seconds"
        ),
        "ambiente": ler_bme280(),
        "maquina": ultima_leitura_esp32.copy(),
    }


# ============================================================
# VALIDAR SINAIS OBRIGATÓRIOS
# ============================================================

def valor_valido(valor):
    """Aceita números finitos; rejeita None, texto e NaN."""

    if isinstance(valor, bool):
        return False

    if not isinstance(valor, (int, float)):
        return False

    return math.isfinite(valor)


def validar_leitura(leitura):
    ambiente = leitura.get("ambiente", {})
    maquina = leitura.get("maquina", {})
    vibracao = maquina.get("vibracao")

    obrigatorios = {
        "temperatura ambiente": ambiente.get("temperatura"),
        "umidade ambiente": ambiente.get("umidade"),
        "pressao ambiente": ambiente.get("pressao"),
        "temperatura da maquina": maquina.get(
            "temperatura_analogica"
        ),
    }

    if isinstance(vibracao, dict):
        obrigatorios["vibracao X"] = vibracao.get("x")
        obrigatorios["vibracao Y"] = vibracao.get("y")
        obrigatorios["vibracao Z"] = vibracao.get("z")
    else:
        obrigatorios["vibracao X"] = None
        obrigatorios["vibracao Y"] = None
        obrigatorios["vibracao Z"] = None

    faltando = [
        nome
        for nome, valor in obrigatorios.items()
        if not valor_valido(valor)
    ]

    if faltando:
        log(
            "[CSV] LEITURA DESCARTADA — sinais ausentes ou inválidos: "
            + ", ".join(faltando)
        )
        return False

    return True


# ============================================================
# CSV — SALVAR PARA TREINAMENTO DA IA
# ============================================================

def salvar_csv(leitura):
    """Retorna True se salvou, False se deu erro."""

    global linhas_salvas_sessao

    ambiente = leitura["ambiente"]
    maquina = leitura["maquina"]
    vibracao = maquina.get("vibracao") or {}

    campos = [
        "timestamp",
        "recebido_em",
        "temperatura_ambiente_c",
        "umidade_ambiente_pct",
        "pressao_ambiente_hpa",
        "temperatura_maquina",
        "vibracao_x",
        "vibracao_y",
        "vibracao_z",
        "mic_nivel",
    ]

    linha = {
        "timestamp": leitura["timestamp"],
        "recebido_em": maquina.get("recebido_em"),
        "temperatura_ambiente_c": ambiente.get("temperatura"),
        "umidade_ambiente_pct": ambiente.get("umidade"),
        "pressao_ambiente_hpa": ambiente.get("pressao"),
        "temperatura_maquina": maquina.get(
            "temperatura_analogica"
        ),
        "vibracao_x": vibracao.get("x"),
        "vibracao_y": vibracao.get("y"),
        "vibracao_z": vibracao.get("z"),
        "mic_nivel": maquina.get("mic_nivel"),
    }

    try:
        novo_arquivo = (
            not ARQUIVO_CSV.exists()
            or ARQUIVO_CSV.stat().st_size == 0
        )

        with ARQUIVO_CSV.open(
            "a", newline="", encoding="utf-8"
        ) as arquivo:
            escritor = csv.DictWriter(
                arquivo, fieldnames=campos
            )

            if novo_arquivo:
                escritor.writeheader()

            escritor.writerow(linha)

        with estado_lock:
            linhas_salvas_sessao += 1

        log(f"[CSV] Leitura completa salva: {ARQUIVO_CSV}")
        return True

    except Exception as erro:
        log(f"[CSV] Erro ao salvar: {erro}")
        return False


# ============================================================
# HISTÓRICO PARA OS GRÁFICOS
# ============================================================

def _num(valor):
    return valor if valor_valido(valor) else None


def registrar_historico(leitura):
    """Guarda toda leitura recebida (completa ou não) para os gráficos."""

    ambiente = leitura["ambiente"]
    maquina = leitura["maquina"]
    vibracao = maquina.get("vibracao") or {}

    ponto = {
        "hora": datetime.now().strftime("%H:%M:%S"),
        "temp_amb": _num(ambiente.get("temperatura")),
        "umid": _num(ambiente.get("umidade")),
        "pres": _num(ambiente.get("pressao")),
        "temp_maq": _num(maquina.get("temperatura_analogica")),
        "vx": _num(vibracao.get("x")),
        "vy": _num(vibracao.get("y")),
        "vz": _num(vibracao.get("z")),
        "mic": _num(maquina.get("mic_nivel")),
    }

    with estado_lock:
        historico.append(ponto)


# ============================================================
# EXIBIR LEITURA
# ============================================================

def imprimir_leitura(leitura):
    ambiente = leitura["ambiente"]
    maquina = leitura["maquina"]

    linhas = [
        "=" * 65,
        "SMARTPREDICT — LEITURA DOS SENSORES",
        "=" * 65,
        f"Timestamp: {leitura['timestamp']}",
        "",
        "BME280 — AMBIENTE (RASPBERRY PI)",
        f"Temperatura: {ambiente['temperatura']} °C",
        f"Umidade:     {ambiente['umidade']} %",
        f"Pressão:     {ambiente['pressao']} hPa",
        "",
        "ESP32-C3 — MÁQUINA",
        f"Temperatura: {maquina.get('temperatura_analogica')}",
        f"Vibração:    {maquina.get('vibracao')}",
        f"Microfone:   {maquina.get('mic_nivel')}",
        f"Recebido em: {maquina.get('recebido_em')}",
        "=" * 65,
    ]

    log("\n" + "\n".join(linhas))


# ============================================================
# MQTT — CALLBACKS
# ============================================================

def ao_conectar(client, userdata, flags, reason_code, properties):
    global mqtt_conectado

    if reason_code == 0:
        mqtt_conectado = True
        log("\n[MQTT] Conectado ao HiveMQ Cloud!")

        resultado = client.subscribe(TOPICO_ESP32)

        if resultado[0] == mqtt.MQTT_ERR_SUCCESS:
            log(f"[MQTT] Inscrito em: {TOPICO_ESP32}")
        else:
            log(f"[MQTT] Erro ao assinar tópico: {resultado[0]}")
    else:
        mqtt_conectado = False
        log(f"[MQTT] Falha de conexão: {reason_code}")


def ao_desconectar(
    client, userdata, disconnect_flags, reason_code, properties
):
    global mqtt_conectado

    mqtt_conectado = False
    log(f"[MQTT] Desconectado. Motivo: {reason_code}")


def ao_receber(client, userdata, msg):
    global ultima_leitura_esp32, esp32_ip

    try:
        mensagem = msg.payload.decode("utf-8")
        dados = json.loads(mensagem)

        if not isinstance(dados, dict):
            log("[MQTT] Descartado: JSON não é um objeto.")
            return

        vibracao = dados.get("vibracao")

        if vibracao is not None and not isinstance(vibracao, dict):
            log("[MQTT] Descartado: formato de vibração inválido.")
            return

        # Não reutilizar valores antigos de uma mensagem anterior.
        nova_leitura = {
            "mic_nivel": dados.get("mic_nivel"),
            "temperatura_analogica": dados.get(
                "temperatura_analogica"
            ),
            "vibracao": vibracao,
            "recebido_em": datetime.now().astimezone().isoformat(
                timespec="seconds"
            ),
        }

        ultima_leitura_esp32 = nova_leitura

        # IP da ESP32 (para exibir o painel dela no site)
        ip_recebido = dados.get("ip")

        if isinstance(ip_recebido, str):
            try:
                ipaddress.ip_address(ip_recebido)

                if ip_recebido != esp32_ip:
                    esp32_ip = ip_recebido
                    log(f"[WEB] Painel da ESP32 detectado: http://{esp32_ip}")
            except ValueError:
                pass

        log(f"\n[MQTT] Mensagem recebida: {msg.topic}")

        leitura = montar_leitura()
        imprimir_leitura(leitura)
        registrar_historico(leitura)

        if validar_leitura(leitura):
            if salvando:
                salvar_csv(leitura)
            else:
                log("[CSV] Gravação PAUSADA — leitura não foi salva.")
        else:
            log("[CSV] Registro incompleto NÃO foi salvo.")

    except json.JSONDecodeError:
        log("[MQTT] Descartado: JSON inválido.")

    except Exception as erro:
        log(f"[ERRO] {type(erro).__name__}: {erro}")


# ============================================================
# CRIAR CLIENTE MQTT
# ============================================================

def criar_cliente_mqtt():
    cliente = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv311,
    )

    cliente.username_pw_set(USUARIO, SENHA)

    cliente.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    cliente.tls_insecure_set(False)

    cliente.on_connect = ao_conectar
    cliente.on_disconnect = ao_desconectar
    cliente.on_message = ao_receber

    return cliente


# ============================================================
# SERVIDOR WEB (Flask)
# ============================================================

app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.ERROR)  # sem ruído no terminal


@app.before_request
def exigir_login():
    """Pede usuário e senha em todas as rotas, se WEB_SENHA estiver definida."""

    if not WEB_SENHA:
        return None

    auth = request.authorization

    if auth:
        usuario_ok = hmac.compare_digest(
            (auth.username or "").encode("utf-8"),
            WEB_USUARIO.encode("utf-8"),
        )
        senha_ok = hmac.compare_digest(
            (auth.password or "").encode("utf-8"),
            WEB_SENHA.encode("utf-8"),
        )

        if usuario_ok and senha_ok:
            return None

    return Response(
        "Acesso restrito",
        401,
        {"WWW-Authenticate": 'Basic realm="SmartPredict"'},
    )


@app.get("/")
def pagina_inicial():
    return Response(PAGINA_HTML, mimetype="text/html")


@app.get("/api/estado")
def api_estado():
    with estado_lock:
        pontos = list(historico)
        resposta = {
            "salvando": salvando,
            "mqtt_conectado": mqtt_conectado,
            "linhas_salvas_sessao": linhas_salvas_sessao,
            "historico": pontos,
            "ultimo": pontos[-1] if pontos else None,
            "esp32_painel_url": ESP32_PANEL_URL,
        }

    try:
        resposta["csv_bytes"] = ARQUIVO_CSV.stat().st_size if ARQUIVO_CSV.exists() else 0
        if ARQUIVO_CSV.exists():
            with ARQUIVO_CSV.open("r", newline="", encoding="utf-8") as f:
                resposta["csv_linhas_total"] = max(sum(1 for _ in csv.reader(f)) - 1, 0)
        else:
            resposta["csv_linhas_total"] = 0
    except OSError:
        resposta["csv_bytes"] = 0
        resposta["csv_linhas_total"] = 0

    return jsonify(resposta)


@app.get("/api/logs")
def api_logs():
    desde = request.args.get("desde", default=0, type=int)

    with estado_lock:
        novos = [item for item in logs if item["id"] > desde]
        ultimo = log_contador

    return jsonify({"logs": novos, "ultimo": ultimo})


@app.post("/api/salvar/iniciar")
def api_iniciar():
    global salvando

    with estado_lock:
        salvando = True

    log("[WEB] Gravação do CSV INICIADA pelo painel.")
    return jsonify({"salvando": True})


@app.post("/api/salvar/parar")
def api_parar():
    global salvando

    with estado_lock:
        salvando = False

    log("[WEB] Gravação do CSV PARADA pelo painel.")
    return jsonify({"salvando": False})


@app.get("/api/download")
def api_download():
    if not ARQUIVO_CSV.exists() or ARQUIVO_CSV.stat().st_size == 0:
        return jsonify({"erro": "Ainda não existe CSV salvo."}), 404

    nome = f"dados_treinamento_{datetime.now():%Y%m%d_%H%M%S}.csv"
    log(f"[WEB] Download do CSV solicitado ({nome}).")

    return send_file(
        ARQUIVO_CSV,
        mimetype="text/csv",
        as_attachment=True,
        download_name=nome,
    )


@app.post("/api/csv/apagar")
def api_apagar_csv():
    """Apaga o CSV após solicitação explícita pelo painel e pausa a gravação."""
    global salvando, linhas_salvas_sessao
    with estado_lock:
        salvando = False
        linhas_salvas_sessao = 0
        historico.clear()
    try:
        if ARQUIVO_CSV.exists():
            ARQUIVO_CSV.unlink()
        log("[WEB] CSV apagado pelo painel; gravação pausada.")
        return jsonify({"ok": True})
    except OSError as erro:
        log(f"[ERRO] Não foi possível apagar CSV: {erro}")
        return jsonify({"ok": False, "erro": str(erro)}), 500


# ============================================================
# PÁGINA HTML (painel)
# ============================================================

PAGINA_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SmartPredict — Painel</title>
<style>
  :root {
    --bg:#0f1420; --card:#171e2e; --borda:#263049; --txt:#e6eaf2;
    --suave:#8892a6; --ok:#43a047; --erro:#e53935; --azul:#1e88e5;
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--txt);
         font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif; }
  header { display:flex; flex-wrap:wrap; gap:12px; align-items:center;
           justify-content:space-between; padding:14px 20px;
           border-bottom:1px solid var(--borda); }
  h1 { font-size:18px; margin:0; }
  main { padding:16px 20px 40px; max-width:1300px; margin:0 auto; }
  .badge { display:inline-flex; align-items:center; gap:6px; font-size:13px;
           padding:4px 10px; border-radius:999px; background:var(--card);
           border:1px solid var(--borda); margin-left:6px; }
  .dot { width:9px; height:9px; border-radius:50%; background:var(--suave); }
  .dot.ok { background:var(--ok); } .dot.erro { background:var(--erro); }
  .controles { display:flex; flex-wrap:wrap; gap:10px; align-items:center;
               background:var(--card); border:1px solid var(--borda);
               border-radius:10px; padding:12px; margin-bottom:16px; }
  button, a.btn { background:var(--azul); color:#fff; border:0; cursor:pointer;
            border-radius:8px; padding:9px 14px; font-size:14px;
            text-decoration:none; display:inline-block; }
  button.verde { background:var(--ok); } button.verm { background:var(--erro); }
  button.cinza { background:#3a4560; }
  button:disabled { opacity:.45; cursor:not-allowed; }
  .info { color:var(--suave); font-size:13px; margin-left:auto; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr));
           gap:10px; margin-bottom:16px; }
  .valor { background:var(--card); border:1px solid var(--borda);
           border-radius:10px; padding:10px 12px; }
  .valor small { color:var(--suave); display:block; font-size:12px; }
  .valor b { font-size:22px; font-variant-numeric:tabular-nums; }
  .valor span { color:var(--suave); font-size:13px; margin-left:3px; }
  .graficos { display:grid; grid-template-columns:repeat(auto-fit,minmax(380px,1fr));
              gap:14px; margin-bottom:16px; }
  .grafico { background:var(--card); border:1px solid var(--borda);
             border-radius:10px; padding:12px; }
  .grafico h3 { margin:0 0 6px; font-size:14px; font-weight:600; }
  .legenda { font-size:12px; color:var(--suave); margin-bottom:4px; }
  .legenda i { display:inline-block; width:10px; height:10px; border-radius:2px;
               margin:0 4px 0 10px; vertical-align:middle; }
  .legenda i:first-child { margin-left:0; }
  canvas { width:100%; height:190px; display:block; }
  .terminal { background:#0a0d14; border:1px solid var(--borda);
              border-radius:10px; overflow:hidden; }
  .terminal header { padding:8px 12px; font-size:13px; }
  #log { margin:0; padding:10px 12px; height:320px; overflow:auto;
         font:12px/1.45 ui-monospace,Consolas,monospace; color:#b8f7c4;
         white-space:pre-wrap; word-break:break-word; }
  #log .h { color:#6b7a99; }
  #log .w { color:#ffd54f; }
  #esp32-box { display:none; background:var(--card); border:1px solid var(--borda);
               border-radius:10px; margin-bottom:16px; overflow:hidden; }
  #esp32-box header { padding:8px 12px; font-size:13px; }
  #esp32-box a { color:#90caf9; font-size:12px; }
  #esp32-frame { width:100%; height:520px; border:0; background:#fff; display:block; }
  footer { color:var(--suave); font-size:12px; margin-top:14px; }
  code { background:var(--card); padding:1px 5px; border-radius:4px; }
</style>
</head>
<body>
<header>
  <h1>SmartPredict — Manutenção Preditiva</h1>
  <div>
    <span class="badge"><span class="dot" id="dot-mqtt"></span><span id="txt-mqtt">MQTT…</span></span>
    <span class="badge"><span class="dot" id="dot-grav"></span><span id="txt-grav">Gravação…</span></span>
  </div>
</header>

<main>
  <div class="controles">
    <button class="verde" id="btn-iniciar">▶ Iniciar gravação</button>
    <button class="verm" id="btn-parar">■ Parar gravação</button>
    <a class="btn" href="/api/download" id="btn-baixar">⬇ Baixar CSV</a>
    <button class="verm" id="btn-apagar">🗑 Apagar CSV</button>
    <span class="info" id="info-csv">—</span>
  </div>

  <div class="cards" id="cards"></div>
  <div class="graficos" id="graficos"></div>

  <div id="esp32-box">
    <header>
      <span>Painel da ESP32</span>
      <a id="esp32-link" href="#" target="_blank" rel="noopener">Abrir em nova aba ↗</a>
    </header>
    <iframe id="esp32-frame" title="Painel da ESP32"></iframe>
  </div>

  <div class="terminal">
    <header>
      <span>Terminal do Raspberry Pi</span>
      <button class="cinza" id="btn-limpar" style="padding:4px 10px;font-size:12px">Limpar tela</button>
    </header>
    <pre id="log"></pre>
  </div>

  <footer id="rodape"></footer>
</main>

<script>
const CARDS = [
  ['temp_amb','Temp. ambiente','°C'], ['umid','Umidade','%'],
  ['pres','Pressão','hPa'], ['temp_maq','Temp. máquina',''],
  ['vx','Vibração X',''], ['vy','Vibração Y',''], ['vz','Vibração Z',''],
  ['mic','Microfone','']
];

const GRAFICOS = [
  {id:'g-temp', titulo:'Temperaturas', series:[['temp_amb','Ambiente','#4fc3f7'],['temp_maq','Máquina','#ff8a65']]},
  {id:'g-umid', titulo:'Umidade (%)', series:[['umid','Umidade','#81c784']]},
  {id:'g-pres', titulo:'Pressão (hPa)', series:[['pres','Pressão','#ba68c8']]},
  {id:'g-vib',  titulo:'Vibração (X / Y / Z)', series:[['vx','X','#ef5350'],['vy','Y','#ffee58'],['vz','Z','#42a5f5']]},
  {id:'g-mic',  titulo:'Microfone (nível)', series:[['mic','Nível','#26c6da']]},
];

const $ = id => document.getElementById(id);
let historico = [];

// ---------- montar a interface ----------
$('cards').innerHTML = CARDS.map(([k,nome,un]) =>
  `<div class="valor"><small>${nome}</small><b id="v-${k}">—</b><span>${un}</span></div>`
).join('');

$('graficos').innerHTML = GRAFICOS.map(g =>
  `<div class="grafico"><h3>${g.titulo}</h3>
   <div class="legenda">${g.series.map(s=>`<i style="background:${s[2]}"></i>${s[1]}`).join('')}</div>
   <canvas id="${g.id}"></canvas></div>`
).join('');

// ---------- desenho dos gráficos (sem bibliotecas externas) ----------
function desenhar(cv, series, hist) {
  const dpr = window.devicePixelRatio || 1;
  const w = cv.clientWidth, h = cv.clientHeight;
  cv.width = w * dpr; cv.height = h * dpr;
  const c = cv.getContext('2d');
  c.setTransform(dpr, 0, 0, dpr, 0, 0);
  c.clearRect(0, 0, w, h);

  const m = {l:50, r:10, t:8, b:20};
  const vals = [];
  series.forEach(s => hist.forEach(p => {
    if (typeof p[s[0]] === 'number') vals.push(p[s[0]]);
  }));

  c.font = '11px system-ui, sans-serif';
  if (!vals.length) {
    c.fillStyle = '#8892a6';
    c.fillText('Aguardando dados…', m.l, h / 2);
    return;
  }

  let mn = Math.min(...vals), mx = Math.max(...vals);
  if (mn === mx) { mn -= 1; mx += 1; }
  const pad = (mx - mn) * 0.1; mn -= pad; mx += pad;

  const X = i => m.l + (hist.length <= 1 ? 0 : i * (w - m.l - m.r) / (hist.length - 1));
  const Y = v => m.t + (mx - v) * (h - m.t - m.b) / (mx - mn);

  // grade + eixo Y
  c.strokeStyle = '#263049'; c.fillStyle = '#8892a6'; c.lineWidth = 1;
  for (let k = 0; k <= 4; k++) {
    const y = m.t + k * (h - m.t - m.b) / 4;
    c.beginPath(); c.moveTo(m.l, y); c.lineTo(w - m.r, y); c.stroke();
    c.fillText((mx - (mx - mn) * k / 4).toFixed(1), 4, y + 4);
  }
  // eixo X (hora)
  c.fillText(hist[0].hora, m.l, h - 5);
  const ult = hist[hist.length - 1].hora;
  c.fillText(ult, w - m.r - c.measureText(ult).width, h - 5);

  // linhas (quebra onde há valor nulo)
  series.forEach(s => {
    c.strokeStyle = s[2]; c.lineWidth = 1.8; c.beginPath();
    let abaixado = false;
    hist.forEach((p, i) => {
      const v = p[s[0]];
      if (typeof v !== 'number') { abaixado = false; return; }
      if (!abaixado) { c.moveTo(X(i), Y(v)); abaixado = true; }
      else c.lineTo(X(i), Y(v));
    });
    c.stroke();
    // ponto no último valor
    for (let i = hist.length - 1; i >= 0; i--) {
      if (typeof hist[i][s[0]] === 'number') {
        c.fillStyle = s[2]; c.beginPath();
        c.arc(X(i), Y(hist[i][s[0]]), 3, 0, 7); c.fill(); break;
      }
    }
  });
}

function redesenhar() {
  GRAFICOS.forEach(g => desenhar($(g.id), g.series, historico));
}
window.addEventListener('resize', redesenhar);

// ---------- estado ----------
function fmtBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  return (b / 1048576).toFixed(2) + ' MB';
}

async function atualizar() {
  try {
    const r = await fetch('/api/estado');
    const d = await r.json();
    historico = d.historico;

    if (d.esp32_painel_url && $('esp32-frame').dataset.url !== d.esp32_painel_url) {
      $('esp32-frame').dataset.url = d.esp32_painel_url;
      $('esp32-frame').src = d.esp32_painel_url;
      $('esp32-link').href = d.esp32_painel_url;
      $('esp32-box').style.display = 'block';
    }

    $('dot-mqtt').className = 'dot ' + (d.mqtt_conectado ? 'ok' : 'erro');
    $('txt-mqtt').textContent = d.mqtt_conectado ? 'MQTT conectado' : 'MQTT desconectado';
    $('dot-grav').className = 'dot ' + (d.salvando ? 'ok' : 'erro');
    $('txt-grav').textContent = d.salvando ? 'Gravando CSV' : 'Gravação pausada';
    $('btn-iniciar').disabled = d.salvando;
    $('btn-parar').disabled = !d.salvando;
    $('info-csv').textContent =
      `CSV: ${fmtBytes(d.csv_bytes)} · ${d.csv_linhas_total ?? d.linhas_salvas_sessao} linhas no arquivo`;

    CARDS.forEach(([k]) => {
      const v = d.ultimo ? d.ultimo[k] : null;
      $('v-' + k).textContent = (typeof v === 'number') ? v.toFixed(2) : '—';
    });
    redesenhar();
  } catch (e) {
    $('dot-mqtt').className = 'dot erro';
    $('txt-mqtt').textContent = 'Sem resposta do Raspberry';
  }
}

// ---------- terminal ----------
let ultimoLog = 0;

async function atualizarLogs() {
  try {
    const r = await fetch('/api/logs?desde=' + ultimoLog);
    const d = await r.json();
    if (!d.logs.length) { ultimoLog = d.ultimo; return; }

    const el = $('log');
    const noFim = el.scrollTop + el.clientHeight >= el.scrollHeight - 30;

    d.logs.forEach(item => {
      const linha = document.createElement('div');
      const hora = document.createElement('span');
      hora.className = 'h';
      hora.textContent = '[' + item.t + '] ';
      const texto = document.createElement('span');
      texto.textContent = item.msg;
      if (item.msg.includes('DESCARTAD') || item.msg.includes('Erro') ||
          item.msg.includes('ERRO') || item.msg.includes('Falha')) texto.className = 'w';
      linha.append(hora, texto);
      el.appendChild(linha);
      ultimoLog = item.id;
    });

    while (el.children.length > 400) el.removeChild(el.firstChild);
    if (noFim) el.scrollTop = el.scrollHeight;
  } catch (e) { /* tenta de novo no próximo ciclo */ }
}

// ---------- botões ----------
async function comando(rota) {
  try { await fetch(rota, {method:'POST'}); } catch (e) {}
  atualizar(); atualizarLogs();
}
$('btn-iniciar').onclick = () => comando('/api/salvar/iniciar');
$('btn-parar').onclick   = () => comando('/api/salvar/parar');
$('btn-apagar').onclick = async () => {
  if (!confirm('Tem certeza que deseja apagar todo o CSV? Essa ação não pode ser desfeita.')) return;
  const r = await fetch('/api/csv/apagar', {method:'POST'});
  const d = await r.json();
  if (!d.ok) alert('Não foi possível apagar o CSV: ' + (d.erro || 'erro desconhecido'));
  atualizar(); atualizarLogs();
};
$('btn-limpar').onclick  = () => { $('log').innerHTML = ''; };
$('btn-baixar').onclick  = async ev => {
  const r = await fetch('/api/download', {method:'HEAD'}).catch(() => null);
  if (r && r.status === 404) { ev.preventDefault(); alert('Ainda não existe CSV salvo.'); }
  setTimeout(atualizarLogs, 500);
};

$('rodape').innerHTML =
  'Endpoints: <code>GET /</code> <code>GET /api/estado</code> <code>GET /api/logs</code> ' +
  '<code>POST /api/salvar/iniciar</code> <code>POST /api/salvar/parar</code> ' +
  '<code>GET /api/download</code> — endereço: <code>' + location.origin + '</code>';

atualizar(); atualizarLogs();
setInterval(atualizar, 2000);
setInterval(atualizarLogs, 1000);
</script>
</body>
</html>
"""


def carregar_historico_csv():
    """Carrega leituras anteriores do CSV para os gráficos ao iniciar."""
    if not ARQUIVO_CSV.exists():
        log("[CSV] Ainda não existe histórico salvo.")
        return
    try:
        with ARQUIVO_CSV.open("r", newline="", encoding="utf-8") as f:
            leitor = csv.DictReader(f)
            carregados = []
            for linha in leitor:
                def numero(*chaves):
                    for chave in chaves:
                        valor = linha.get(chave)
                        if valor not in (None, ""):
                            try:
                                return float(valor)
                            except (TypeError, ValueError):
                                pass
                    return None
                timestamp = linha.get("timestamp", "")
                hora = timestamp[-8:] if len(timestamp) >= 8 else timestamp
                carregados.append({
                    "hora": hora,
                    "temp_amb": numero("temperatura_ambiente", "temp_amb"),
                    "umid": numero("umidade", "umid"),
                    "pres": numero("pressao", "pres"),
                    "temp_maq": numero("temperatura_analogica", "temperatura_maquina", "temp_maq"),
                    "vx": numero("vibracao_x", "vx"),
                    "vy": numero("vibracao_y", "vy"),
                    "vz": numero("vibracao_z", "vz"),
                    "mic": numero("mic_nivel", "mic"),
                })
        with estado_lock:
            historico.extend(carregados)
        log(f"[CSV] Histórico carregado para os gráficos: {len(carregados)} linhas.")
    except Exception as erro:
        log(f"[CSV] Falha ao carregar histórico: {type(erro).__name__}: {erro}")


# ============================================================
# ENDEREÇO NA REDE LOCAL
# ============================================================

def descobrir_ip_local():
    """Descobre o IP do Raspberry na rede (não envia pacotes de verdade)."""

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


# ============================================================
# MAIN
# ============================================================

def main():
    carregar_historico_csv()
    log("=" * 65)
    log("RASPBERRY PI — SMARTPREDICT")
    log("Receptor MQTT + Painel Web | Manutenção Preditiva")
    log("=" * 65)

    if not BROKER or not USUARIO or not SENHA:
        log("[ERRO] Confira MQTT_BROKER, MQTT_USUARIO e MQTT_SENHA no .env.")
        return

    cliente = criar_cliente_mqtt()

    log(f"[MQTT] Broker: {BROKER}")
    log(f"[MQTT] Porta: {PORTA}")
    log(f"[MQTT] Tópico: {TOPICO_ESP32}")
    log(f"[CSV] Destino: {ARQUIVO_CSV}")
    log(f"[CSV] Gravação ao iniciar: {'LIGADA' if salvando else 'PAUSADA'}")
    log(f"[WEB] Painel da ESP32: {ESP32_PANEL_URL or 'não configurado'}")
    if WEB_SENHA:
        log(f"[WEB] Login ativo (usuário: {WEB_USUARIO}).")
    else:
        log("[WEB] ATENÇÃO: sem senha! Defina WEB_SENHA no .env antes de abrir para a internet.")
    log("[MQTT] Conectando...")

    try:
        # connect_async + loop_start: o MQTT roda em segundo plano e
        # reconecta sozinho; a thread principal fica com o servidor web.
        cliente.connect_async(BROKER, PORTA, keepalive=60)
        cliente.loop_start()

        ip = descobrir_ip_local()
        host = socket.gethostname()

        log("")
        log("=" * 65)
        log("PAINEL WEB DISPONÍVEL — abra no navegador (mesma rede):")
        log(f"   http://{ip}:{WEB_PORTA}")
        log(f"   http://{host}.local:{WEB_PORTA}")
        log("=" * 65)
        log("Pressione CTRL+C para sair.")

        app.run(
            host="0.0.0.0",
            port=WEB_PORTA,
            threaded=True,
            use_reloader=False,
        )

    except KeyboardInterrupt:
        log("\n[SISTEMA] Encerrando...")

    except Exception as erro:
        log(f"[ERRO] {type(erro).__name__}: {erro}")

    finally:
        cliente.disconnect()
        cliente.loop_stop()


if __name__ == "__main__":
    main()


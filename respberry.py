"""
Raspberry Pi — Receptor MQTT + Painel Web
Projeto SENAI — SmartPredict

Funções:
1. Conectar ao HiveMQ Cloud via MQTT/TLS
2. Receber os lotes de leituras do ESP32-C3 (uma leitura a cada 5 s,
   enviadas juntas a cada ~1 min, porque a ESP32 dorme para economizar)
3. Ler o BME280 local via I2C a cada 5 s, numa thread própria, e casar
   cada leitura da ESP32 com a leitura do ambiente do mesmo horário
4. Salvar leituras completas em CSV para treinamento da IA
   (inclui RMS e pico da vibração)
5. Descartar leituras incompletas
6. Manter o SQLite preparado, mas desativado
7. Servir um painel web na rede local com:
   - cards com valor atual, variação e mini-gráfico de cada sensor
   - gráficos com tooltip ao passar o mouse e janela ajustável
   - botão único para iniciar / pausar a gravação do CSV
   - botões para baixar e apagar o CSV
   - terminal do Raspberry e console da ESP32 ao vivo, com filtro de avisos
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
import time
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
# Login do painel (OBRIGATÓRIO se for acessar pela internet). Senha vazia = sem login.
WEB_USUARIO = os.getenv("WEB_USUARIO", "admin").strip()
WEB_SENHA = os.getenv("WEB_SENHA", "")
# "1" = já começa gravando; "0" = começa pausado
SALVAR_AO_INICIAR = os.getenv("SALVAR_AO_INICIAR", "1").strip() != "0"

TOPICO_ESP32 = "senai/manutencao/maquina01/sensores"
TOPICO_CONSOLE = "senai/manutencao/maquina01/console"   # console (Serial) da ESP32

ARQUIVO_CSV = DIRETORIO_PROJETO / "dados_treinamento.csv"
BANCO = str(DIRETORIO_PROJETO / "dados_sensores.db")

I2C_BUS = 1
BME280_ENDERECO = 0x76

INTERVALO_BME = 5          # segundos entre leituras do BME280
BME_HIST_MAX = 720         # 1 h de leituras do BME280 guardadas
TOLERANCIA_BME_S = 20      # diferença máxima de horário para casar com a ESP32

# A ESP32 envia a cada ~1 min. Acima disto o painel avisa que os dados atrasaram.
ATRASO_AVISO_S = 150

HISTORICO_MAX = 300   # pontos mantidos em memória para os gráficos
LOGS_MAX = 500        # linhas do terminal mantidas em memória

CAMPOS_CSV = [
    "timestamp",
    "recebido_em",
    "temperatura_ambiente_c",
    "umidade_ambiente_pct",
    "pressao_ambiente_hpa",
    "temperatura_maquina",
    "vibracao_x",
    "vibracao_y",
    "vibracao_z",
    "vibracao_rms_x",
    "vibracao_rms_y",
    "vibracao_rms_z",
    "vibracao_pico",
    "mic_nivel",
]


# ============================================================
# ESTADO COMPARTILHADO (threads MQTT / BME280 <-> servidor web)
# ============================================================

estado_lock = threading.Lock()
csv_lock = threading.Lock()   # evita apagar o CSV no meio de uma gravação
parar_threads = threading.Event()

INICIO = time.monotonic()

salvando = SALVAR_AO_INICIAR
esp32_ip = None            # preenchido com o IP enviado pela ESP32
mqtt_conectado = False
linhas_salvas_sessao = 0
recebidas_total = 0
descartadas_total = 0
ultimo_recebimento = None  # time.monotonic() do último lote recebido

historico = deque(maxlen=HISTORICO_MAX)
ponto_contador = 0

bme_hist = deque(maxlen=BME_HIST_MAX)   # (time.time(), leitura) do BME280

logs = deque(maxlen=LOGS_MAX)
log_contador = 0

esp_logs = deque(maxlen=LOGS_MAX)   # console da ESP32 recebido pelo MQTT
esp_log_contador = 0
ultimo_console = None               # time.monotonic() do último bloco recebido


def log(mensagem, nivel="info"):
    """Imprime no terminal E guarda para o terminal do site.

    nivel: "info", "ok", "aviso" ou "erro" (define a cor no painel).
    """

    global log_contador

    print(mensagem, flush=True)

    with estado_lock:
        log_contador += 1
        logs.append({
            "id": log_contador,
            "t": datetime.now().strftime("%H:%M:%S"),
            "nivel": nivel,
            "msg": str(mensagem).strip("\n"),
        })


# ============================================================
# UTILITÁRIOS
# ============================================================

def valor_valido(valor):
    """Aceita números finitos; rejeita None, bool, texto e NaN."""

    if isinstance(valor, bool):
        return False

    if not isinstance(valor, (int, float)):
        return False

    return math.isfinite(valor)


def fmt(valor, casas=2):
    return f"{valor:.{casas}f}" if valor_valido(valor) else "—"


def _num(valor):
    return valor if valor_valido(valor) else None


def iso(momento):
    """time.time() -> texto ISO com fuso local."""

    return datetime.fromtimestamp(momento).astimezone().isoformat(
        timespec="seconds"
    )


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
                vibracao_z REAL,
                vibracao_rms_x REAL,
                vibracao_rms_y REAL,
                vibracao_rms_z REAL,
                vibracao_pico REAL
            )
        """)
        conexao.commit()
        log(f"[SQLITE] Banco pronto: {BANCO}", "ok")
    finally:
        conexao.close()


def salvar_sqlite(leitura):
    """Salva uma leitura completa no SQLite, se ativado."""

    ambiente = leitura["ambiente"]
    maquina = leitura["maquina"]
    vibracao = maquina.get("vibracao") or {}
    rms = maquina.get("vibracao_rms") or {}

    conexao = sqlite3.connect(BANCO)

    try:
        conexao.execute("""
            INSERT INTO leituras (
                timestamp, temperatura_ambiente, umidade, pressao,
                mic_nivel, temperatura_maquina,
                vibracao_x, vibracao_y, vibracao_z,
                vibracao_rms_x, vibracao_rms_y, vibracao_rms_z, vibracao_pico
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            rms.get("x"),
            rms.get("y"),
            rms.get("z"),
            maquina.get("vibracao_pico"),
        ))
        conexao.commit()
    finally:
        conexao.close()


# ============================================================
# BME280 — SENSOR AMBIENTE NA RASPBERRY PI
# ============================================================

# O barramento e a calibração ficam em cache. Em caso de erro, o cache
# é descartado e a próxima leitura tenta reabrir tudo. O erro só é
# avisado uma vez (e de novo quando o sensor voltar), para não encher
# o terminal a cada 5 s.
_bme = {"bus": None, "params": None, "falhando": False}


def _fechar_bme():
    if _bme["bus"] is not None:
        try:
            _bme["bus"].close()
        except Exception:
            pass
    _bme["bus"] = None
    _bme["params"] = None


def ler_bme280():
    try:
        import smbus2
        import bme280

        if _bme["bus"] is None:
            _bme["bus"] = smbus2.SMBus(I2C_BUS)
            _bme["params"] = bme280.load_calibration_params(
                _bme["bus"], BME280_ENDERECO
            )

        dados = bme280.sample(
            _bme["bus"], BME280_ENDERECO, _bme["params"]
        )

        if _bme["falhando"]:
            _bme["falhando"] = False
            log("[BME280] Sensor voltou a responder.", "ok")

        return {
            "temperatura": round(dados.temperature, 2),
            "umidade": round(dados.humidity, 2),
            "pressao": round(dados.pressure, 2),
        }

    except Exception as erro:
        if not _bme["falhando"]:
            _bme["falhando"] = True
            log(f"[BME280] Erro ao ler sensor: {erro}", "erro")

        _fechar_bme()

        return {
            "temperatura": None,
            "umidade": None,
            "pressao": None,
        }


def loop_bme280():
    """Lê o BME280 a cada INTERVALO_BME segundos e guarda com o horário."""

    while not parar_threads.is_set():
        momento = time.time()
        leitura = ler_bme280()

        with estado_lock:
            bme_hist.append((momento, leitura))

        parar_threads.wait(INTERVALO_BME)

    _fechar_bme()


def ambiente_para(momento):
    """Leitura do BME280 mais próxima do horário da leitura da ESP32."""

    with estado_lock:
        candidatos = list(bme_hist)

    melhor = None
    menor_diferenca = None

    for horario, leitura in candidatos:
        if leitura["temperatura"] is None:
            continue

        diferenca = abs(horario - momento)

        if menor_diferenca is None or diferenca < menor_diferenca:
            menor_diferenca = diferenca
            melhor = leitura

    if melhor is None or menor_diferenca > TOLERANCIA_BME_S:
        return {"temperatura": None, "umidade": None, "pressao": None}

    return dict(melhor)


# ============================================================
# MONTAR E VALIDAR LEITURA
# ============================================================

def validar_leitura(leitura):
    ambiente = leitura.get("ambiente", {})
    maquina = leitura.get("maquina", {})
    vibracao = maquina.get("vibracao")
    rms = maquina.get("vibracao_rms")

    if not isinstance(vibracao, dict):
        vibracao = {}

    if not isinstance(rms, dict):
        rms = {}

    obrigatorios = {
        "temperatura ambiente": ambiente.get("temperatura"),
        "umidade ambiente": ambiente.get("umidade"),
        "pressao ambiente": ambiente.get("pressao"),
        "temperatura da maquina": maquina.get("temperatura_analogica"),
        "vibracao X": vibracao.get("x"),
        "vibracao Y": vibracao.get("y"),
        "vibracao Z": vibracao.get("z"),
        "RMS X": rms.get("x"),
        "RMS Y": rms.get("y"),
        "RMS Z": rms.get("z"),
        "pico de vibracao": maquina.get("vibracao_pico"),
    }

    faltando = [
        nome
        for nome, valor in obrigatorios.items()
        if not valor_valido(valor)
    ]

    if faltando:
        log(
            "[CSV] Leitura DESCARTADA — sinais ausentes ou inválidos: "
            + ", ".join(faltando),
            "aviso",
        )
        return False

    return True


# ============================================================
# CSV — SALVAR PARA TREINAMENTO DA IA
# ============================================================

def preparar_csv():
    """Se o CSV existente tiver colunas antigas, renomeia e começa um novo.

    Assim as colunas novas (RMS e pico) não ficam misturadas com o
    formato antigo no mesmo arquivo.
    """

    try:
        if not ARQUIVO_CSV.exists() or ARQUIVO_CSV.stat().st_size == 0:
            return

        with ARQUIVO_CSV.open(encoding="utf-8") as arquivo:
            cabecalho = arquivo.readline().strip()

        if cabecalho == ",".join(CAMPOS_CSV):
            return

        antigo = ARQUIVO_CSV.with_name(
            f"dados_treinamento_antigo_{datetime.now():%Y%m%d_%H%M%S}.csv"
        )
        ARQUIVO_CSV.rename(antigo)
        log(
            f"[CSV] Formato antigo encontrado. Arquivo guardado como {antigo.name}; "
            "um CSV novo será criado.",
            "aviso",
        )

    except OSError as erro:
        log(f"[CSV] Erro ao verificar o CSV: {erro}", "erro")


def linha_csv(leitura):
    ambiente = leitura["ambiente"]
    maquina = leitura["maquina"]
    vibracao = maquina.get("vibracao") or {}
    rms = maquina.get("vibracao_rms") or {}

    return {
        "timestamp": leitura["timestamp"],
        "recebido_em": leitura["recebido_em"],
        "temperatura_ambiente_c": ambiente.get("temperatura"),
        "umidade_ambiente_pct": ambiente.get("umidade"),
        "pressao_ambiente_hpa": ambiente.get("pressao"),
        "temperatura_maquina": maquina.get("temperatura_analogica"),
        "vibracao_x": vibracao.get("x"),
        "vibracao_y": vibracao.get("y"),
        "vibracao_z": vibracao.get("z"),
        "vibracao_rms_x": rms.get("x"),
        "vibracao_rms_y": rms.get("y"),
        "vibracao_rms_z": rms.get("z"),
        "vibracao_pico": maquina.get("vibracao_pico"),
        "mic_nivel": maquina.get("mic_nivel"),
    }


def salvar_csv(leituras):
    """Salva várias leituras de uma vez. Retorna quantas foram salvas."""

    global linhas_salvas_sessao

    if not leituras:
        return 0

    linhas = [linha_csv(leitura) for leitura in leituras]

    try:
        with csv_lock:
            novo_arquivo = (
                not ARQUIVO_CSV.exists()
                or ARQUIVO_CSV.stat().st_size == 0
            )

            with ARQUIVO_CSV.open("a", newline="", encoding="utf-8") as arquivo:
                escritor = csv.DictWriter(arquivo, fieldnames=CAMPOS_CSV)

                if novo_arquivo:
                    escritor.writeheader()

                escritor.writerows(linhas)

        with estado_lock:
            linhas_salvas_sessao += len(linhas)
            total = linhas_salvas_sessao

        log(f"[CSV] {len(linhas)} linha(s) salva(s) — {total} nesta sessão.", "ok")
        return len(linhas)

    except Exception as erro:
        log(f"[CSV] Erro ao salvar: {erro}", "erro")
        return 0


# ============================================================
# HISTÓRICO PARA OS GRÁFICOS
# ============================================================

def registrar_historico(leitura):
    """Guarda toda leitura recebida (completa ou não) para os gráficos."""

    global ponto_contador

    ambiente = leitura["ambiente"]
    maquina = leitura["maquina"]
    vibracao = maquina.get("vibracao") or {}
    rms = maquina.get("vibracao_rms") or {}

    with estado_lock:
        ponto_contador += 1
        historico.append({
            "id": ponto_contador,
            "hora": datetime.fromtimestamp(leitura["momento"]).strftime("%H:%M:%S"),
            "temp_amb": _num(ambiente.get("temperatura")),
            "umid": _num(ambiente.get("umidade")),
            "pres": _num(ambiente.get("pressao")),
            "temp_maq": _num(maquina.get("temperatura_analogica")),
            "vx": _num(vibracao.get("x")),
            "vy": _num(vibracao.get("y")),
            "vz": _num(vibracao.get("z")),
            "rx": _num(rms.get("x")),
            "ry": _num(rms.get("y")),
            "rz": _num(rms.get("z")),
            "pico": _num(maquina.get("vibracao_pico")),
            "mic": _num(maquina.get("mic_nivel")),
        })


# ============================================================
# EXIBIR LEITURA (uma linha só — fica legível no terminal web)
# ============================================================

def imprimir_leitura(leitura):
    a = leitura["ambiente"]
    m = leitura["maquina"]
    r = m.get("vibracao_rms") or {}
    hora = datetime.fromtimestamp(leitura["momento"]).strftime("%H:%M:%S")

    log(
        f"[LEITURA {hora}] Ambiente {fmt(a['temperatura'])} °C · "
        f"{fmt(a['umidade'])} % · {fmt(a['pressao'])} hPa | "
        f"Máquina T={fmt(m.get('temperatura_analogica'))} · "
        f"RMS=({fmt(r.get('x'), 3)}, {fmt(r.get('y'), 3)}, {fmt(r.get('z'), 3)}) · "
        f"Pico={fmt(m.get('vibracao_pico'), 3)} · "
        f"Mic={fmt(m.get('mic_nivel'), 0)}"
    )


# ============================================================
# CONSOLE DA ESP32 (recebido pelo MQTT)
# ============================================================

def _nivel_esp(linha):
    """Define a cor da linha no painel a partir do texto da ESP32."""

    t = linha.lower()

    if ("[erro]" in t or "falh" in t or "nao foi possivel" in t
            or "travado" in t or "indisponivel" in t or "nao responde" in t):
        return "erro"

    if ("desconectado" in t or "nao apareceu" in t or "reduzindo" in t
            or "nao coube" in t):
        return "aviso"

    if "[ok]" in t or "conectado" in t or "enviado" in t or "funcionou" in t:
        return "ok"

    return "info"


def registrar_console_esp32(texto):
    """Quebra o bloco recebido em linhas e guarda para o painel."""

    global esp_log_contador, ultimo_console

    hora = datetime.now().strftime("%H:%M:%S")

    with estado_lock:
        ultimo_console = time.monotonic()

        for linha in texto.splitlines():
            linha = linha.rstrip()

            if not linha:
                continue

            esp_log_contador += 1
            esp_logs.append({
                "id": esp_log_contador,
                "t": hora,
                "nivel": _nivel_esp(linha),
                "msg": linha,
            })


# ============================================================
# MQTT — CALLBACKS
# ============================================================

def ao_conectar(client, userdata, flags, reason_code, properties):
    global mqtt_conectado

    if not reason_code.is_failure:
        with estado_lock:
            mqtt_conectado = True

        log("[MQTT] Conectado ao HiveMQ Cloud!", "ok")

        resultado = client.subscribe([(TOPICO_ESP32, 0), (TOPICO_CONSOLE, 0)])

        if resultado[0] == mqtt.MQTT_ERR_SUCCESS:
            log(f"[MQTT] Inscrito em: {TOPICO_ESP32}", "ok")
            log(f"[MQTT] Inscrito em: {TOPICO_CONSOLE}", "ok")
        else:
            log(f"[MQTT] Erro ao assinar tópico: {resultado[0]}", "erro")
    else:
        with estado_lock:
            mqtt_conectado = False

        log(f"[MQTT] Falha de conexão: {reason_code}", "erro")


def ao_desconectar(client, userdata, disconnect_flags, reason_code, properties):
    global mqtt_conectado

    with estado_lock:
        mqtt_conectado = False

    log(f"[MQTT] Desconectado. Motivo: {reason_code}", "aviso")


def _descartar(motivo):
    global recebidas_total, descartadas_total

    with estado_lock:
        recebidas_total += 1
        descartadas_total += 1

    log(f"[MQTT] Descartado: {motivo}", "aviso")


def atualizar_ip_esp32(ip_recebido):
    global esp32_ip

    if not isinstance(ip_recebido, str):
        return

    try:
        ipaddress.ip_address(ip_recebido)
    except ValueError:
        return

    if ip_recebido != esp32_ip:
        with estado_lock:
            esp32_ip = ip_recebido
        log(f"[MQTT] ESP32 no IP {ip_recebido}", "ok")


def montar_leitura(item, agora_esp, recebido):
    """Transforma uma leitura do lote da ESP32 numa leitura completa.

    A ESP32 não sabe a hora real: manda o relógio interno dela em cada
    leitura ("ts") e no envio ("agora"). A diferença diz há quantos
    segundos cada leitura foi feita.
    """

    global recebidas_total

    if not isinstance(item, dict):
        _descartar("leitura do lote não é um objeto.")
        return None

    vibracao = item.get("vibracao")
    rms = item.get("vibracao_rms")

    if ((vibracao is not None and not isinstance(vibracao, dict))
            or (rms is not None and not isinstance(rms, dict))):
        _descartar("formato de vibração inválido.")
        return None

    with estado_lock:
        recebidas_total += 1

    ts = item.get("ts")
    momento = recebido

    if valor_valido(agora_esp) and valor_valido(ts):
        estimado = recebido - (agora_esp - ts)

        # Proteção contra relógio estranho (reinício no meio do lote etc.)
        if recebido - 3600 <= estimado <= recebido + 5:
            momento = estimado

    return {
        "momento": momento,
        "timestamp": iso(momento),
        "recebido_em": iso(recebido),
        "ambiente": ambiente_para(momento),
        "maquina": {
            "mic_nivel": item.get("mic_nivel"),
            "temperatura_analogica": item.get("temperatura_analogica"),
            "vibracao": vibracao,
            "vibracao_rms": rms,
            "vibracao_pico": item.get("vibracao_pico"),
        },
    }


def ao_receber(client, userdata, msg):
    global descartadas_total, ultimo_recebimento

    # Console da ESP32: só guarda para o painel, não é leitura de sensor.
    if msg.topic == TOPICO_CONSOLE:
        try:
            registrar_console_esp32(msg.payload.decode("utf-8", errors="replace"))
        except Exception as erro:
            log(f"[ERRO] Console da ESP32: {type(erro).__name__}: {erro}", "erro")
        return

    try:
        dados = json.loads(msg.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        _descartar("JSON inválido.")
        return

    try:
        if not isinstance(dados, dict):
            _descartar("JSON não é um objeto.")
            return

        recebido = time.time()
        atualizar_ip_esp32(dados.get("ip"))

        lote = dados.get("lote")

        if lote is None:
            # Formato antigo: uma leitura por mensagem, sem relógio.
            itens, agora_esp = [dados], None
        elif isinstance(lote, list):
            itens, agora_esp = lote, dados.get("agora")
        else:
            _descartar("campo 'lote' inválido.")
            return

        perdidas = dados.get("perdidas")

        if valor_valido(perdidas) and perdidas > 0:
            log(
                f"[ESP32] {int(perdidas)} leitura(s) antiga(s) perdida(s): "
                "a fila encheu enquanto estava sem conexão.",
                "aviso",
            )

        with estado_lock:
            ultimo_recebimento = time.monotonic()
            gravar = salvando

        log(f"[MQTT] Lote recebido: {len(itens)} leitura(s).")

        completas = []

        for item in itens:
            leitura = montar_leitura(item, agora_esp, recebido)

            if leitura is None:
                continue

            imprimir_leitura(leitura)
            registrar_historico(leitura)

            if validar_leitura(leitura):
                completas.append(leitura)
            else:
                with estado_lock:
                    descartadas_total += 1

        if completas:
            if gravar:
                salvar_csv(completas)
            else:
                log(f"[CSV] Gravação pausada — {len(completas)} leitura(s) não salva(s).")

    except Exception as erro:
        log(f"[ERRO] {type(erro).__name__}: {erro}", "erro")


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

    # Reconexão com espera crescente (1 s até 60 s).
    cliente.reconnect_delay_set(min_delay=1, max_delay=60)

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


@app.before_request
def proteger_post():
    """Só aceita POST vindo do próprio painel (bloqueia formulários de outros sites)."""

    if request.method == "POST" and request.headers.get("X-Requested-With") != "fetch":
        return jsonify({"erro": "Requisição recusada."}), 403

    return None


@app.after_request
def sem_cache(resposta):
    if request.path.startswith("/api/"):
        resposta.headers["Cache-Control"] = "no-store"
    return resposta


@app.get("/")
def pagina_inicial():
    return Response(PAGINA_HTML, mimetype="text/html")


@app.get("/api/estado")
def api_estado():
    """Envia só os pontos novos (id > desde) para economizar banda."""

    desde = request.args.get("desde", default=0, type=int)
    agora = time.monotonic()

    with estado_lock:
        # Se o servidor reiniciou, o navegador está "à frente": manda tudo.
        reiniciado = desde > ponto_contador
        if reiniciado:
            desde = 0

        resposta = {
            "reiniciado": reiniciado,
            "historico": [p for p in historico if p["id"] > desde],
            "salvando": salvando,
            "mqtt_conectado": mqtt_conectado,
            "linhas_salvas_sessao": linhas_salvas_sessao,
            "recebidas_total": recebidas_total,
            "descartadas_total": descartadas_total,
            "segundos_desde_ultima": (
                round(agora - ultimo_recebimento)
                if ultimo_recebimento is not None else None
            ),
            "limite_atraso_s": ATRASO_AVISO_S,
            "uptime_s": round(agora - INICIO),
            "esp32_ip": esp32_ip or "",
            "segundos_desde_console": (
                round(agora - ultimo_console)
                if ultimo_console is not None else None
            ),
        }

    try:
        resposta["csv_bytes"] = (
            ARQUIVO_CSV.stat().st_size if ARQUIVO_CSV.exists() else 0
        )
    except OSError:
        resposta["csv_bytes"] = 0

    return jsonify(resposta)


@app.get("/api/logs")
def api_logs():
    desde = request.args.get("desde", default=0, type=int)

    with estado_lock:
        reiniciado = desde > log_contador
        if reiniciado:
            desde = 0

        novos = [item for item in logs if item["id"] > desde]
        ultimo = log_contador

    return jsonify({"logs": novos, "ultimo": ultimo, "reiniciado": reiniciado})


@app.get("/api/esp32/console")
def api_esp32_console():
    desde = request.args.get("desde", default=0, type=int)

    with estado_lock:
        reiniciado = desde > esp_log_contador
        if reiniciado:
            desde = 0

        novos = [item for item in esp_logs if item["id"] > desde]
        ultimo = esp_log_contador

    return jsonify({"logs": novos, "ultimo": ultimo, "reiniciado": reiniciado})


@app.post("/api/salvar/iniciar")
def api_iniciar():
    global salvando

    with estado_lock:
        salvando = True

    log("[WEB] Gravação do CSV INICIADA pelo painel.", "ok")
    return jsonify({"salvando": True})


@app.post("/api/salvar/parar")
def api_parar():
    global salvando

    with estado_lock:
        salvando = False

    log("[WEB] Gravação do CSV PAUSADA pelo painel.", "aviso")
    return jsonify({"salvando": False})


@app.get("/api/download")
def api_download():
    if not ARQUIVO_CSV.exists() or ARQUIVO_CSV.stat().st_size == 0:
        return jsonify({"erro": "Ainda não existe CSV salvo."}), 404

    nome = f"dados_treinamento_{datetime.now():%Y%m%d_%H%M%S}.csv"

    # O painel faz um HEAD antes para checar se o arquivo existe;
    # só registra no terminal o download de verdade.
    if request.method != "HEAD":
        log(f"[WEB] Download do CSV solicitado ({nome}).")

    return send_file(
        ARQUIVO_CSV,
        mimetype="text/csv",
        as_attachment=True,
        download_name=nome,
    )


@app.post("/api/csv/apagar")
def api_apagar_csv():
    """Apaga o CSV. A próxima leitura salva cria um arquivo novo com cabeçalho."""

    global linhas_salvas_sessao

    with csv_lock:
        if not ARQUIVO_CSV.exists():
            return jsonify({"erro": "Não existe CSV para apagar."}), 404

        try:
            ARQUIVO_CSV.unlink()
        except OSError as erro:
            log(f"[CSV] Erro ao apagar: {erro}", "erro")
            return jsonify({"erro": f"Não foi possível apagar: {erro}"}), 500

    with estado_lock:
        linhas_salvas_sessao = 0

    log("[WEB] CSV APAGADO pelo painel.", "aviso")
    return jsonify({"apagado": True})


# ============================================================
# PÁGINA HTML (painel)
# ============================================================

PAGINA_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#0a0e16">
<title>SmartPredict — Painel</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%23161e2e'/%3E%3Cpath d='M8 21a8 8 0 1 1 16 0' stroke='%23f5a524' stroke-width='2.6' fill='none' stroke-linecap='round'/%3E%3Cpath d='M16 21l4.5-5.5' stroke='%23e6ebf4' stroke-width='2.4' stroke-linecap='round'/%3E%3C/svg%3E">
<style>
  :root {
    --bg:#0a0e16; --surface:#111724; --surface-2:#161e2e;
    --line:#222b3d; --line-2:#2c374d;
    --text:#e6ebf4; --muted:#8a94a8; --dim:#5d6880;
    --accent:#f5a524; --ok:#22c55e; --err:#ef4444; --warn:#f59e0b;
    --radius:12px;
    --mono:ui-monospace,"JetBrains Mono","Cascadia Code","SF Mono",Consolas,monospace;
    --sans:"Inter",ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
  }
  * { box-sizing:border-box; }
  html { color-scheme:dark; }
  body {
    margin:0; background:var(--bg); color:var(--text);
    font:14px/1.5 var(--sans); -webkit-font-smoothing:antialiased;
    background-image:radial-gradient(1100px 480px at 8% -12%, rgba(245,165,36,.07), transparent 60%);
    background-attachment:fixed;
  }

  /* ---------- topo ---------- */
  .topo {
    position:sticky; top:0; z-index:10;
    display:flex; flex-wrap:wrap; gap:12px 20px; align-items:center; justify-content:space-between;
    padding:14px clamp(16px,3vw,32px);
    background:rgba(10,14,22,.82); backdrop-filter:blur(10px);
    border-bottom:1px solid var(--line);
  }
  .marca { display:flex; align-items:center; gap:12px; }
  .marca svg { width:36px; height:36px; flex:none; }
  .marca h1 { margin:0; font-size:17px; font-weight:650; letter-spacing:-.01em; }
  .marca p { margin:0; color:var(--muted); font-size:12.5px; }
  .status { display:flex; flex-wrap:wrap; gap:8px; }
  .pill {
    display:inline-flex; align-items:center; gap:8px; height:30px; padding:0 12px;
    border-radius:999px; background:var(--surface); border:1px solid var(--line);
    font-size:12.5px; color:var(--muted); white-space:nowrap;
  }
  .pill i { width:8px; height:8px; border-radius:50%; background:var(--dim); flex:none; }
  .pill.ok i { background:var(--ok); box-shadow:0 0 0 3px rgba(34,197,94,.18); }
  .pill.erro i { background:var(--err); box-shadow:0 0 0 3px rgba(239,68,68,.18); }
  .pill.aviso i { background:var(--warn); box-shadow:0 0 0 3px rgba(245,158,11,.18); }
  .pill.gravando i { background:var(--err); animation:pulso 1.4s ease-in-out infinite; }
  .pill.ok span, .pill.gravando span { color:var(--text); }
  .pill.erro span { color:#fecaca; }
  .pill.aviso span { color:#fde68a; }
  @keyframes pulso {
    0%,100% { box-shadow:0 0 0 0 rgba(239,68,68,.55); }
    50% { box-shadow:0 0 0 6px rgba(239,68,68,0); }
  }

  main {
    max-width:1360px; margin:0 auto;
    padding:20px clamp(16px,3vw,32px) 40px;
    display:flex; flex-direction:column; gap:26px;
  }

  /* ---------- barra de controle ---------- */
  .barra {
    display:flex; flex-wrap:wrap; align-items:center; gap:12px;
    padding:12px; background:var(--surface);
    border:1px solid var(--line); border-radius:var(--radius);
  }
  .btn {
    display:inline-flex; align-items:center; justify-content:center; gap:9px;
    height:40px; padding:0 16px; border-radius:9px; border:1px solid transparent;
    font:600 13.5px var(--sans); cursor:pointer; text-decoration:none;
    transition:background .15s, border-color .15s, color .15s, transform .05s;
  }
  .btn:active { transform:translateY(1px); }
  .btn:disabled { opacity:.55; cursor:progress; }
  .btn:focus-visible, .seg button:focus-visible, .mini:focus-visible, summary:focus-visible {
    outline:2px solid var(--accent); outline-offset:2px;
  }
  .btn svg { width:16px; height:16px; }
  .btn-grav { min-width:184px; background:var(--accent); color:#1b1306; }
  .btn-grav:hover { background:#ffb83d; }
  .btn-grav.parar { background:transparent; color:var(--text); border-color:var(--line-2); }
  .btn-grav.parar:hover { border-color:var(--err); color:#fecaca; background:rgba(239,68,68,.06); }
  .ico-rec { width:10px; height:10px; border-radius:50%; background:currentColor; }
  .ico-stop { width:10px; height:10px; border-radius:2px; background:var(--err); }
  .btn-sec { background:var(--surface-2); color:var(--text); border-color:var(--line-2); }
  .btn-sec:hover { border-color:#3e4b66; }
  .btn-perigo { background:transparent; color:#fca5a5; border-color:rgba(239,68,68,.35); }
  .btn-perigo:hover { background:rgba(239,68,68,.1); border-color:var(--err); color:#fecaca; }
  .metricas { display:flex; flex-wrap:wrap; gap:8px 26px; margin:0 0 0 auto; padding:0 6px; }
  .metricas div { display:flex; flex-direction:column; }
  .metricas dt { font-size:10.5px; color:var(--dim); text-transform:uppercase; letter-spacing:.07em; }
  .metricas dd { margin:0; font:600 14px var(--mono); font-variant-numeric:tabular-nums; }
  .metricas dd.alerta { color:#fcd34d; }

  /* ---------- títulos de seção ---------- */
  .rotulo {
    display:flex; align-items:baseline; gap:10px; margin:0;
    font-size:12.5px; font-weight:600; text-transform:uppercase; letter-spacing:.09em; color:var(--muted);
  }
  .rotulo small { font-size:12px; font-weight:400; text-transform:none; letter-spacing:0; color:var(--dim); }
  .grupo > .rotulo { margin-bottom:12px; }
  .titulo-linha {
    display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between;
    gap:10px; margin-bottom:12px;
  }

  /* ---------- cards ---------- */
  .cards { display:grid; grid-template-columns:repeat(auto-fill,minmax(190px,1fr)); gap:12px; }
  .card {
    position:relative; padding:14px 14px 10px; overflow:hidden;
    background:linear-gradient(180deg, var(--surface-2), var(--surface));
    border:1px solid var(--line); border-radius:var(--radius);
  }
  .card::before { content:""; position:absolute; left:0; top:12px; bottom:12px; width:3px; border-radius:0 3px 3px 0; background:var(--cor); }
  .card header { display:flex; justify-content:space-between; align-items:center; gap:8px; }
  .card-nome { font-size:12.5px; color:var(--muted); }
  .delta { font:500 11.5px var(--mono); color:var(--dim); white-space:nowrap; }
  .delta.sobe { color:#fdba74; }
  .delta.desce { color:#93c5fd; }
  .card-valor { display:flex; align-items:baseline; gap:5px; margin:4px 0 6px; }
  .card-valor b { font:600 26px/1.15 var(--mono); font-variant-numeric:tabular-nums; letter-spacing:-.02em; }
  .card-valor b.vazio { color:var(--dim); }
  .card-valor span { font-size:13px; color:var(--muted); }
  .spark { display:block; width:100%; height:32px; }

  /* ---------- gráficos ---------- */
  .seg {
    display:inline-flex; align-items:center; gap:2px; padding:3px;
    background:var(--surface); border:1px solid var(--line); border-radius:9px;
    font-size:12px; color:var(--dim);
  }
  .seg button {
    height:26px; padding:0 10px; border:0; border-radius:6px; background:transparent;
    color:var(--muted); font:500 12px var(--mono); cursor:pointer;
  }
  .seg button:hover { color:var(--text); }
  .seg button.ativo { background:var(--surface-2); color:var(--text); box-shadow:inset 0 0 0 1px var(--line-2); }
  .seg span { padding:0 8px 0 6px; }
  .graficos { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,430px),1fr)); gap:12px; }
  .grafico { padding:14px 14px 8px; background:var(--surface); border:1px solid var(--line); border-radius:var(--radius); }
  .grafico header { display:flex; flex-wrap:wrap; justify-content:space-between; align-items:center; gap:6px 12px; margin-bottom:6px; }
  .grafico h3 { margin:0; font-size:14px; font-weight:600; }
  .grafico h3 small { color:var(--dim); font-weight:400; margin-left:4px; }
  .legenda { display:flex; gap:12px; font-size:12px; color:var(--muted); }
  .legenda i { display:inline-block; width:12px; height:3px; border-radius:2px; margin-right:6px; vertical-align:middle; }
  .grafico canvas { display:block; width:100%; height:210px; cursor:crosshair; touch-action:pan-y; }

  /* ---------- painéis (ESP32 / terminal) ---------- */
  .painel { background:var(--surface); border:1px solid var(--line); border-radius:var(--radius); overflow:hidden; }
  .painel-topo {
    display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between;
    gap:8px; padding:10px 14px; border-bottom:1px solid var(--line);
  }
  .acoes { display:flex; gap:6px; align-items:center; }
  .mini {
    height:28px; padding:0 10px; border-radius:7px; cursor:pointer;
    background:var(--surface-2); color:var(--muted); border:1px solid var(--line-2);
    font:500 12px var(--sans); text-decoration:none; display:inline-flex; align-items:center;
  }
  .mini:hover { color:var(--text); }
  .mini.ativo { border-color:rgba(245,158,11,.6); color:#fcd34d; background:rgba(245,158,11,.08); }

  .consoles { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,520px),1fr)); gap:12px; }
  .terminal { background:#070a10; min-width:0; }
  .log {
    margin:0; padding:12px 14px; height:360px; overflow:auto;
    font:12px/1.6 var(--mono); color:#cbd5e1; white-space:pre-wrap; word-break:break-word;
  }
  .log .h { color:var(--dim); }
  .log .n-ok .m { color:#86efac; }
  .log .n-aviso .m { color:#fcd34d; }
  .log .n-erro .m { color:#fca5a5; }
  .log.so-avisos .ln:not(.n-aviso):not(.n-erro) { display:none; }
  .log .vazio-msg { color:var(--dim); }

  ::-webkit-scrollbar { width:10px; height:10px; }
  ::-webkit-scrollbar-thumb { background:#232c3f; border-radius:10px; border:2px solid transparent; background-clip:padding-box; }
  ::-webkit-scrollbar-track { background:transparent; }

  footer { display:flex; flex-wrap:wrap; justify-content:space-between; gap:8px; color:var(--dim); font-size:12px; }
  code { font:12px var(--mono); color:var(--muted); }

  @media (max-width:700px) {
    .metricas { margin-left:0; width:100%; justify-content:space-between; }
    .btn-grav, .btn-sec, .btn-perigo { flex:1; }
    .card-valor b { font-size:22px; }
    .grafico canvas { height:180px; }
  }
  @media (prefers-reduced-motion:reduce) {
    .pill.gravando i { animation:none; }
  }
</style>
</head>
<body>

<header class="topo">
  <div class="marca">
    <svg viewBox="0 0 32 32" fill="none" aria-hidden="true">
      <rect x=".5" y=".5" width="31" height="31" rx="8" fill="#161e2e" stroke="#2c374d"/>
      <path d="M8 21a8 8 0 1 1 16 0" stroke="#f5a524" stroke-width="2.4" stroke-linecap="round"/>
      <path d="M16 21l4.5-5.5" stroke="#e6ebf4" stroke-width="2.2" stroke-linecap="round"/>
      <circle cx="16" cy="21" r="2" fill="#e6ebf4"/>
    </svg>
    <div>
      <h1>SmartPredict</h1>
      <p>Máquina 01 · Manutenção preditiva</p>
    </div>
  </div>
  <div class="status" aria-live="polite">
    <span class="pill" id="p-mqtt"><i></i><span>MQTT…</span></span>
    <span class="pill" id="p-grav"><i></i><span>Gravação…</span></span>
    <span class="pill" id="p-dados"><i></i><span>Dados…</span></span>
  </div>
</header>

<main>
  <section class="barra">
    <button class="btn btn-grav" id="btn-grav" disabled><span class="ico-rec"></span>Carregando…</button>
    <a class="btn btn-sec" href="/api/download" id="btn-baixar">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M12 4v11m0 0l-4.5-4.5M12 15l4.5-4.5M5 20h14"/>
      </svg>
      Baixar CSV
    </a>
    <button class="btn btn-perigo" id="btn-apagar">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3"/>
      </svg>
      Apagar CSV
    </button>
    <dl class="metricas">
      <div><dt>Arquivo CSV</dt><dd id="m-csv">—</dd></div>
      <div><dt>Salvas na sessão</dt><dd id="m-salvas">—</dd></div>
      <div><dt>Recebidas</dt><dd id="m-receb">—</dd></div>
      <div><dt>Descartadas</dt><dd id="m-desc">—</dd></div>
    </dl>
  </section>

  <section class="grupo">
    <h2 class="rotulo">Ambiente <small>BME280 · Raspberry Pi</small></h2>
    <div class="cards" id="cards-amb"></div>
  </section>

  <section class="grupo">
    <h2 class="rotulo">Máquina <small>ESP32-C3</small></h2>
    <div class="cards" id="cards-maq"></div>
  </section>

  <section class="grupo">
    <div class="titulo-linha">
      <h2 class="rotulo">Histórico <small>passe o mouse para ver os valores</small></h2>
      <div class="seg" id="janela" role="group" aria-label="Quantidade de leituras exibidas">
        <button data-n="60">60</button>
        <button data-n="150">150</button>
        <button data-n="300" class="ativo">300</button>
        <span>leituras</span>
      </div>
    </div>
    <div class="graficos" id="graficos"></div>
  </section>

  <div class="consoles">
    <section class="painel terminal">
      <div class="painel-topo">
        <h2 class="rotulo">Console <small id="esp-info">ESP32-C3 · aguardando…</small></h2>
        <div class="acoes">
          <button class="mini" id="esp-filtro" aria-pressed="false">Só avisos</button>
          <button class="mini" id="esp-limpar">Limpar</button>
        </div>
      </div>
      <div id="esp-log" class="log" role="log">
        <div class="vazio-msg">Aguardando o console da ESP32 (chega junto com cada lote, a cada ~1 min)…</div>
      </div>
    </section>

    <section class="painel terminal">
      <div class="painel-topo">
        <h2 class="rotulo">Terminal <small>Raspberry Pi</small></h2>
        <div class="acoes">
          <button class="mini" id="btn-filtro" aria-pressed="false">Só avisos</button>
          <button class="mini" id="btn-limpar">Limpar</button>
        </div>
      </div>
      <div id="log" class="log" role="log"></div>
    </section>
  </div>

  <footer>
    <span id="rodape-uptime">—</span>
    <span>Painel em <code id="rodape-origem"></code></span>
  </footer>
</main>

<script>
const CORES = {
  temp_amb:'#38bdf8', umid:'#4ade80', pres:'#c084fc', temp_maq:'#fb923c',
  rx:'#f87171', ry:'#facc15', rz:'#60a5fa', pico:'#f472b6',
  vx:'#fca5a5', vy:'#fde68a', vz:'#93c5fd', mic:'#22d3ee',
};

// [grupo, chave, nome, unidade]
const CARDS = [
  ['amb','temp_amb','Temperatura','°C'],
  ['amb','umid','Umidade','%'],
  ['amb','pres','Pressão','hPa'],
  ['maq','temp_maq','Temperatura','°C'],
  ['maq','rx','Vibração RMS X','m/s²'],
  ['maq','ry','Vibração RMS Y','m/s²'],
  ['maq','rz','Vibração RMS Z','m/s²'],
  ['maq','pico','Vibração pico','m/s²'],
  ['maq','mic','Microfone',''],
];

const GRAFICOS = [
  {id:'g-temp', titulo:'Temperaturas', un:'°C', series:[['temp_amb','Ambiente'],['temp_maq','Máquina']]},
  {id:'g-rms',  titulo:'Vibração RMS e pico', un:'m/s²', series:[['rx','X'],['ry','Y'],['rz','Z'],['pico','Pico']]},
  {id:'g-vib',  titulo:'Vibração média (posição)', un:'m/s²', series:[['vx','X'],['vy','Y'],['vz','Z']]},
  {id:'g-umid', titulo:'Umidade', un:'%', series:[['umid','Umidade']]},
  {id:'g-pres', titulo:'Pressão', un:'hPa', series:[['pres','Pressão']]},
  {id:'g-mic',  titulo:'Microfone', un:'nível', series:[['mic','Nível']]},
];

const MAX_PONTOS = 300;
const M = {l:52, r:12, t:12, b:24};   // margens dos gráficos
const $ = id => document.getElementById(id);

let historico = [];
let ultimoPonto = 0;
let janelaN = 300;
let salvandoAtual = null;
let csvBytes = 0;
const hover = {};

// ---------- formatação ----------
const fmt = v => typeof v !== 'number' ? '—'
  : Math.abs(v) >= 1000 ? v.toFixed(1)
  : Math.abs(v) < 1 ? v.toFixed(3) : v.toFixed(2);

function fmtEixo(v, faixa) {
  const casas = faixa >= 50 ? 0 : faixa >= 5 ? 1 : faixa >= 0.5 ? 2 : 3;
  return v.toFixed(casas);
}

function fmtBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  return (b / 1048576).toFixed(2) + ' MB';
}

function fmtTempo(s) {
  if (s < 60) return s + ' s';
  if (s < 3600) return Math.floor(s / 60) + ' min';
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return h + ' h ' + m + ' min';
}

const janela = () => historico.slice(-janelaN);

// ---------- montar a interface ----------
CARDS.forEach(([g, k, nome, un]) => {
  $('cards-' + g).insertAdjacentHTML('beforeend', `
    <article class="card" style="--cor:${CORES[k]}">
      <header><span class="card-nome">${nome}</span><span class="delta" id="d-${k}"></span></header>
      <div class="card-valor"><b id="v-${k}" class="vazio">—</b><span>${un}</span></div>
      <canvas class="spark" id="s-${k}" aria-hidden="true"></canvas>
    </article>`);
});

$('graficos').innerHTML = GRAFICOS.map(g => `
  <article class="grafico">
    <header>
      <h3>${g.titulo}${g.un ? `<small>${g.un}</small>` : ''}</h3>
      <div class="legenda">${g.series.map(([k, n]) =>
        `<span><i style="background:${CORES[k]}"></i>${n}</span>`).join('')}</div>
    </header>
    <canvas id="${g.id}"></canvas>
  </article>`).join('');

// ---------- canvas ----------
function prepararCanvas(cv) {
  const dpr = window.devicePixelRatio || 1;
  const w = cv.clientWidth, h = cv.clientHeight;
  const W = Math.round(w * dpr), H = Math.round(h * dpr);
  if (cv.width !== W || cv.height !== H) { cv.width = W; cv.height = H; }
  const c = cv.getContext('2d');
  c.setTransform(dpr, 0, 0, dpr, 0, 0);
  c.clearRect(0, 0, w, h);
  return [c, w, h];
}

function segmentos(hist, k, X, Y) {
  const segs = []; let atual = null;
  hist.forEach((p, i) => {
    const v = p[k];
    if (typeof v !== 'number') { atual = null; return; }
    if (!atual) { atual = []; segs.push(atual); }
    atual.push([X(i), Y(v)]);
  });
  return segs;
}

function desenhar(g) {
  const cv = $(g.id);
  const hist = janela();
  const [c, w, h] = prepararCanvas(cv);
  c.font = '11px ' + getComputedStyle(document.body).fontFamily;

  const vals = [];
  g.series.forEach(([k]) => hist.forEach(p => { if (typeof p[k] === 'number') vals.push(p[k]); }));

  if (!vals.length) {
    c.fillStyle = '#5d6880'; c.textAlign = 'center';
    c.fillText('Aguardando dados…', w / 2, h / 2);
    return;
  }

  let mn = Math.min(...vals), mx = Math.max(...vals);
  if (mn === mx) { mn -= 1; mx += 1; }
  const pad = (mx - mn) * 0.12; mn -= pad; mx += pad;
  const faixa = mx - mn;
  const iw = w - M.l - M.r, ih = h - M.t - M.b;
  const X = i => M.l + (hist.length <= 1 ? iw / 2 : i * iw / (hist.length - 1));
  const Y = v => M.t + (mx - v) * ih / faixa;

  // grade + eixo Y
  c.lineWidth = 1; c.textAlign = 'right';
  for (let k = 0; k <= 4; k++) {
    const y = Math.round(M.t + k * ih / 4) + 0.5;
    c.strokeStyle = 'rgba(148,163,184,.09)';
    c.beginPath(); c.moveTo(M.l, y); c.lineTo(w - M.r, y); c.stroke();
    c.fillStyle = '#5d6880';
    c.fillText(fmtEixo(mx - faixa * k / 4, faixa), M.l - 8, y + 4);
  }

  // eixo X (hora inicial e final)
  c.fillStyle = '#5d6880';
  c.textAlign = 'left';  c.fillText(hist[0].hora, M.l, h - 6);
  c.textAlign = 'right'; c.fillText(hist[hist.length - 1].hora, w - M.r, h - 6);
  c.textAlign = 'left';

  // séries
  g.series.forEach(([k]) => {
    const cor = CORES[k];
    const segs = segmentos(hist, k, X, Y);

    if (g.series.length === 1) {   // área preenchida só em gráficos de 1 série
      const grad = c.createLinearGradient(0, M.t, 0, h - M.b);
      grad.addColorStop(0, cor + '33'); grad.addColorStop(1, cor + '00');
      c.fillStyle = grad;
      segs.forEach(s => {
        if (s.length < 2) return;
        c.beginPath(); c.moveTo(s[0][0], h - M.b);
        s.forEach(([x, y]) => c.lineTo(x, y));
        c.lineTo(s[s.length - 1][0], h - M.b); c.closePath(); c.fill();
      });
    }

    c.strokeStyle = cor; c.lineWidth = 2; c.lineJoin = 'round'; c.lineCap = 'round';
    segs.forEach(s => {
      c.beginPath();
      s.forEach(([x, y], i) => i ? c.lineTo(x, y) : c.moveTo(x, y));
      c.stroke();
    });
  });

  // hover (linha + tooltip) ou ponto final de cada série
  const hi = hover[g.id];
  if (hi != null && hist[hi]) {
    const x = X(hi);
    c.strokeStyle = 'rgba(226,232,240,.35)'; c.setLineDash([3, 3]);
    c.beginPath(); c.moveTo(x, M.t); c.lineTo(x, h - M.b); c.stroke(); c.setLineDash([]);

    const linhas = [[null, hist[hi].hora]];
    g.series.forEach(([k, n]) => {
      const v = hist[hi][k];
      if (typeof v === 'number') {
        c.fillStyle = '#0a0e16'; c.beginPath(); c.arc(x, Y(v), 5, 0, 7); c.fill();
        c.fillStyle = CORES[k];  c.beginPath(); c.arc(x, Y(v), 3.5, 0, 7); c.fill();
      }
      linhas.push([CORES[k], `${n}: ${fmt(v)}`]);
    });
    tooltip(c, x, linhas, w);
  } else {
    g.series.forEach(([k]) => {
      for (let i = hist.length - 1; i >= 0; i--) {
        const v = hist[i][k];
        if (typeof v === 'number') {
          c.fillStyle = CORES[k]; c.beginPath(); c.arc(X(i), Y(v), 3, 0, 7); c.fill();
          break;
        }
      }
    });
  }
}

function tooltip(c, x, linhas, w) {
  c.font = '12px ' + getComputedStyle(document.body).fontFamily;
  const larg = Math.max(...linhas.map(([cor, t]) => c.measureText(t).width + (cor ? 14 : 0))) + 18;
  const alt = linhas.length * 17 + 10;
  let bx = x + 12;
  if (bx + larg > w - 4) bx = x - 12 - larg;
  const by = M.t + 4;

  c.fillStyle = 'rgba(8,11,18,.94)'; c.strokeStyle = 'rgba(148,163,184,.25)';
  c.beginPath();
  if (c.roundRect) c.roundRect(bx, by, larg, alt, 7); else c.rect(bx, by, larg, alt);
  c.fill(); c.stroke();

  linhas.forEach(([cor, t], i) => {
    const ty = by + 19 + i * 17;
    let tx = bx + 9;
    if (cor) { c.fillStyle = cor; c.fillRect(tx, ty - 8, 8, 8); tx += 14; }
    c.fillStyle = cor ? '#e6ebf4' : '#8a94a8';
    c.fillText(t, tx, ty);
  });
}

function sparkline(k) {
  const cv = $('s-' + k);
  const [c, w, h] = prepararCanvas(cv);
  const pts = historico.slice(-40);
  const vals = pts.map(p => p[k]).filter(v => typeof v === 'number');
  if (vals.length < 2) return;

  let mn = Math.min(...vals), mx = Math.max(...vals);
  if (mn === mx) { mn -= 1; mx += 1; }
  const X = i => 1 + i * (w - 2) / (pts.length - 1);
  const Y = v => 3 + (mx - v) * (h - 6) / (mx - mn);

  c.strokeStyle = CORES[k]; c.globalAlpha = .85; c.lineWidth = 1.5; c.lineJoin = 'round';
  segmentos(pts, k, X, Y).forEach(s => {
    c.beginPath();
    s.forEach(([x, y], i) => i ? c.lineTo(x, y) : c.moveTo(x, y));
    c.stroke();
  });
  c.globalAlpha = 1;
}

function atualizarCards() {
  const ult = historico[historico.length - 1];
  const ant = historico[historico.length - 2];

  CARDS.forEach(([, k]) => {
    const v = ult ? ult[k] : null;
    const el = $('v-' + k);
    el.textContent = fmt(v);
    el.classList.toggle('vazio', typeof v !== 'number');

    const d = $('d-' + k);
    const p = ant ? ant[k] : null;
    if (typeof v === 'number' && typeof p === 'number') {
      const dif = v - p;
      const casas = Math.abs(v) < 1 ? 3 : 2;
      if (Math.abs(dif) < Math.pow(10, -casas) / 2) { d.textContent = '= 0'; d.className = 'delta'; }
      else {
        d.textContent = (dif > 0 ? '▲ ' : '▼ ') + Math.abs(dif).toFixed(casas);
        d.className = 'delta ' + (dif > 0 ? 'sobe' : 'desce');
      }
    } else { d.textContent = ''; d.className = 'delta'; }

    sparkline(k);
  });
}

function renderizar() {
  atualizarCards();
  GRAFICOS.forEach(desenhar);
}

let raf = 0;
window.addEventListener('resize', () => {
  cancelAnimationFrame(raf);
  raf = requestAnimationFrame(renderizar);
});

// tooltip dos gráficos
GRAFICOS.forEach(g => {
  const cv = $(g.id);
  cv.addEventListener('pointermove', e => {
    const n = janela().length;
    if (!n) return;
    const r = cv.getBoundingClientRect();
    const iw = r.width - M.l - M.r;
    const i = n <= 1 ? 0 : Math.round((e.clientX - r.left - M.l) / iw * (n - 1));
    hover[g.id] = Math.max(0, Math.min(n - 1, i));
    desenhar(g);
  });
  cv.addEventListener('pointerleave', () => { hover[g.id] = null; desenhar(g); });
});

// janela de leituras
$('janela').addEventListener('click', e => {
  const b = e.target.closest('button');
  if (!b) return;
  janelaN = Number(b.dataset.n);
  $('janela').querySelectorAll('button').forEach(x => x.classList.toggle('ativo', x === b));
  GRAFICOS.forEach(desenhar);
});

// ---------- estado ----------
function pill(id, classe, texto) {
  const p = $(id);
  p.className = 'pill ' + classe;
  p.querySelector('span').textContent = texto;
}

function atualizarBotaoGravacao(salvando) {
  if (salvando === salvandoAtual) return;
  salvandoAtual = salvando;
  const b = $('btn-grav');
  b.disabled = false;
  b.classList.toggle('parar', salvando);
  b.innerHTML = salvando
    ? '<span class="ico-stop"></span>Pausar gravação'
    : '<span class="ico-rec"></span>Iniciar gravação';
}

async function atualizar() {
  try {
    const r = await fetch('/api/estado?desde=' + ultimoPonto, {cache:'no-store'});
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();

    if (d.reiniciado) historico = [];
    if (d.historico.length) {
      historico.push(...d.historico);
      if (historico.length > MAX_PONTOS) historico = historico.slice(-MAX_PONTOS);
    }
    ultimoPonto = historico.length ? historico[historico.length - 1].id : 0;

    pill('p-mqtt', d.mqtt_conectado ? 'ok' : 'erro',
         d.mqtt_conectado ? 'MQTT conectado' : 'MQTT desconectado');
    pill('p-grav', d.salvando ? 'gravando' : '',
         d.salvando ? 'Gravando CSV' : 'Gravação pausada');

    const s = d.segundos_desde_ultima;
    pill('p-dados', s == null ? '' : s > d.limite_atraso_s ? 'aviso' : 'ok',
         s == null ? 'Sem leituras ainda' : 'Último lote há ' + fmtTempo(s));

    atualizarBotaoGravacao(d.salvando);

    csvBytes = d.csv_bytes;
    $('m-csv').textContent = fmtBytes(d.csv_bytes);
    $('btn-apagar').disabled = d.csv_bytes === 0;
    $('m-salvas').textContent = d.linhas_salvas_sessao;
    $('m-receb').textContent = d.recebidas_total;
    $('m-desc').textContent = d.descartadas_total;
    $('m-desc').classList.toggle('alerta', d.descartadas_total > 0);
    $('rodape-uptime').textContent = 'Raspberry ativo há ' + fmtTempo(d.uptime_s);

    const sc = d.segundos_desde_console;
    $('esp-info').textContent = 'ESP32-C3'
      + (d.esp32_ip ? ' · ' + d.esp32_ip : '')
      + (sc == null ? ' · aguardando…' : ' · último envio há ' + fmtTempo(sc));

    if (d.historico.length || d.reiniciado) renderizar();
  } catch (e) {
    pill('p-mqtt', 'erro', 'Sem resposta do Raspberry');
    pill('p-dados', '', 'Dados indisponíveis');
  }
}

// ---------- terminais (Raspberry e ESP32) ----------
function criarTerminal(url, elId, filtroId, limparId) {
  const el = $(elId);
  let ultimo = 0;

  $(limparId).onclick = () => { el.innerHTML = ''; };
  $(filtroId).onclick = () => {
    const ativo = el.classList.toggle('so-avisos');
    $(filtroId).classList.toggle('ativo', ativo);
    $(filtroId).setAttribute('aria-pressed', ativo);
  };

  return async function () {
  try {
    const r = await fetch(url + '?desde=' + ultimo, {cache:'no-store'});
    if (!r.ok) return;
    const d = await r.json();

    if (d.reiniciado) el.innerHTML = '';
    ultimo = d.ultimo;
    if (!d.logs.length) return;

    const aviso = el.querySelector('.vazio-msg');
    if (aviso) aviso.remove();

    const noFim = el.scrollTop + el.clientHeight >= el.scrollHeight - 30;
    const frag = document.createDocumentFragment();

    d.logs.forEach(item => {
      const linha = document.createElement('div');
      linha.className = 'ln n-' + (item.nivel || 'info');
      const hora = document.createElement('span');
      hora.className = 'h';
      hora.textContent = item.t + '  ';
      const texto = document.createElement('span');
      texto.className = 'm';
      texto.textContent = item.msg;
      linha.append(hora, texto);
      frag.appendChild(linha);
    });

    el.appendChild(frag);
    while (el.children.length > 400) el.removeChild(el.firstChild);
    if (noFim) el.scrollTop = el.scrollHeight;
  } catch (e) { /* tenta de novo no próximo ciclo */ }
  };
}

const atualizarLogs = criarTerminal('/api/logs', 'log', 'btn-filtro', 'btn-limpar');
const atualizarConsoleEsp = criarTerminal('/api/esp32/console', 'esp-log', 'esp-filtro', 'esp-limpar');

// ---------- botões ----------
$('btn-grav').onclick = async () => {
  const b = $('btn-grav');
  b.disabled = true;
  const rota = salvandoAtual ? '/api/salvar/parar' : '/api/salvar/iniciar';
  try {
    await fetch(rota, {method:'POST', headers:{'X-Requested-With':'fetch'}});
  } catch (e) {}
  salvandoAtual = null;   // força redesenhar o botão
  await atualizar();
  atualizarLogs();
};

$('btn-baixar').onclick = async ev => {
  const r = await fetch('/api/download', {method:'HEAD'}).catch(() => null);
  if (r && r.status === 404) { ev.preventDefault(); alert('Ainda não existe CSV salvo.'); }
  setTimeout(atualizarLogs, 500);
};

$('btn-apagar').onclick = async () => {
  if (!csvBytes) { alert('Não existe CSV para apagar.'); return; }
  const ok = confirm(
    `Apagar o CSV (${fmtBytes(csvBytes)})?\n\n` +
    'Isso não pode ser desfeito. Se precisar dos dados, baixe o CSV antes.'
  );
  if (!ok) return;

  const b = $('btn-apagar');
  b.disabled = true;
  try {
    const r = await fetch('/api/csv/apagar', {method:'POST', headers:{'X-Requested-With':'fetch'}});
    if (!r.ok) {
      const d = await r.json().catch(() => ({}));
      alert(d.erro || 'Não foi possível apagar o CSV.');
    }
  } catch (e) {
    alert('Sem resposta do Raspberry.');
  }
  await atualizar();
  atualizarLogs();
};

$('rodape-origem').textContent = location.origin;

renderizar();
atualizar(); atualizarLogs(); atualizarConsoleEsp();
setInterval(atualizar, 2000);
setInterval(atualizarLogs, 1000);
setInterval(atualizarConsoleEsp, 1500);
</script>
</body>
</html>
"""


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
    log("=" * 65)
    log("RASPBERRY PI — SMARTPREDICT")
    log("Receptor MQTT + Painel Web | Manutenção Preditiva")
    log("=" * 65)

    if not BROKER or not USUARIO or not SENHA:
        log("[ERRO] Confira MQTT_BROKER, MQTT_USUARIO e MQTT_SENHA no .env.", "erro")
        return

    preparar_csv()

    cliente = criar_cliente_mqtt()

    log(f"[MQTT] Broker: {BROKER}:{PORTA}")
    log(f"[MQTT] Tópico: {TOPICO_ESP32}")
    log(f"[MQTT] Console da ESP32: {TOPICO_CONSOLE}")
    log(f"[BME280] Leitura a cada {INTERVALO_BME} s")
    log(f"[CSV] Destino: {ARQUIVO_CSV}")
    log(f"[CSV] Gravação ao iniciar: {'LIGADA' if salvando else 'PAUSADA'}")

    if WEB_SENHA:
        log(f"[WEB] Login ativo (usuário: {WEB_USUARIO}).", "ok")
    else:
        log("[WEB] ATENÇÃO: sem senha! Defina WEB_SENHA no .env antes de abrir para a internet.", "aviso")

    thread_bme = threading.Thread(target=loop_bme280, name="bme280", daemon=True)
    thread_bme.start()

    log("[MQTT] Conectando...")

    try:
        # connect_async + loop_start: o MQTT roda em segundo plano e
        # reconecta sozinho; a thread principal fica com o servidor web.
        cliente.connect_async(BROKER, PORTA, keepalive=60)
        cliente.loop_start()

        ip = descobrir_ip_local()
        host = socket.gethostname()

        log("=" * 65)
        log("PAINEL WEB DISPONÍVEL — abra no navegador (mesma rede):", "ok")
        log(f"   http://{ip}:{WEB_PORTA}", "ok")
        log(f"   http://{host}.local:{WEB_PORTA}", "ok")
        log("=" * 65)
        log("Pressione CTRL+C para sair.")

        app.run(
            host="0.0.0.0",
            port=WEB_PORTA,
            threaded=True,
            use_reloader=False,
        )

    except KeyboardInterrupt:
        log("[SISTEMA] Encerrando...")

    except Exception as erro:
        log(f"[ERRO] {type(erro).__name__}: {erro}", "erro")

    finally:
        parar_threads.set()
        cliente.disconnect()
        cliente.loop_stop()
        thread_bme.join(timeout=2)


if __name__ == "__main__":
    main()

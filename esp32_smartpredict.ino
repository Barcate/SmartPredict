#include <Arduino.h>
#include <Wire.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <WebServer.h>
#include <ESPmDNS.h>
#include <PubSubClient.h>
#include <Adafruit_MPU6050.h>
#include <Adafruit_Sensor.h>
#include <OneWire.h>
#include <DallasTemperature.h>

// =====================================================
// WIFI
// =====================================================

const char* WIFI_SSID = "WIFI_IOT_CFP601";
const char* WIFI_SENHA = "iot@senai601";

// =====================================================
// HIVEMQ CLOUD
// =====================================================

const char* MQTT_BROKER =
  "f2931adfd9b1483da58557d9489517bc.s1.eu.hivemq.cloud";

const int MQTT_PORT = 8883;
const char* MQTT_USUARIO = "ESP32CNN";
const char* MQTT_SENHA = "Senha123";

const char* MQTT_TOPICO =
  "senai/manutencao/maquina01/sensores";

// =====================================================
// PAINEL WEB DA ESP32
// =====================================================
// Acesse por http://<IP da ESP32> ou http://smartpredict-esp32.local
// O IP tambem e enviado no MQTT para o Raspberry achar sozinho.

const char* NOME_MDNS = "smartpredict-esp32";

// =====================================================
// PINOS
// =====================================================

#define SDA_PIN 21
#define SCL_PIN 20
#define ONE_WIRE_BUS 10

// Microfone analogico (saida AO / OUT do modulo)
// IMPORTANTE: precisa ser um pino do ADC1. O ADC2 nao funciona
// junto com o Wi-Fi ligado, e a leitura fica travada/zerada.
// ESP32-S3: ADC1 = GPIO 1 a 10  |  ESP32-C3: GPIO 0 a 4
// ESP32 classico: GPIO 32 a 39
#define MIC_PIN 4

// =====================================================
// OBJETOS E VARIAVEIS
// =====================================================

Adafruit_MPU6050 mpu;
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature tempSensor(&oneWire);

WiFiClientSecure conexaoTLS;
PubSubClient mqtt(conexaoTLS);
WebServer servidor(80);

bool mpuOK = false;
bool tempOK = false;
bool micOK = false;
bool servidorIniciado = false;

float vibX = 0;
float vibY = 0;
float vibZ = 0;
float temperaturaMaquina = NAN;
int micNivel = -1;  // -1 = sem leitura valida

unsigned long ultimaTentativaMQTT = 0;
unsigned long ultimaTentativaWiFi = 0;
unsigned long ultimaPublicacao = 0;

const unsigned long INTERVALO_MQTT = 5000;
const unsigned long INTERVALO_RECONEXAO_WIFI = 30000;
const unsigned long JANELA_MIC_MS = 50;  // janela de amostragem do som

// =====================================================
// PAGINA HTML DO PAINEL DA ESP32
// =====================================================

const char PAGINA_HTML[] PROGMEM = R"rawliteral(<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ESP32 - Maquina 01</title>
<style>
body{margin:0;background:#0f1420;color:#e6eaf2;font-family:system-ui,sans-serif;padding:12px}
h1{font-size:16px;margin:0 0 10px}
.g{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:8px;margin-bottom:10px}
.c{background:#171e2e;border:1px solid #263049;border-radius:8px;padding:8px 10px}
.c small{display:block;color:#8892a6;font-size:11px}
.c b{font-size:19px}
canvas{width:100%;height:130px;display:block;background:#171e2e;border:1px solid #263049;border-radius:8px;margin-bottom:8px}
.l{font-size:11px;color:#8892a6;margin:6px 0 2px}
</style></head><body>
<h1>ESP32-C3 &mdash; M&aacute;quina 01 <span id="st" style="font-size:12px;color:#8892a6"></span></h1>
<div class="g" id="cards"></div>
<div class="l">Temperatura da m&aacute;quina (&deg;C)</div><canvas id="c1"></canvas>
<div class="l">Vibra&ccedil;&atilde;o X / Y / Z (m/s&sup2;) &mdash; vermelho X, amarelo Y, azul Z</div><canvas id="c2"></canvas>
<div class="l">Microfone (n&iacute;vel)</div><canvas id="c3"></canvas>
<script>
const $=i=>document.getElementById(i);
const H=[],MAX=90;
$('cards').innerHTML=[['t','Temp. m\u00e1quina'],['x','Vibra\u00e7\u00e3o X'],['y','Vibra\u00e7\u00e3o Y'],['z','Vibra\u00e7\u00e3o Z'],['m','Microfone'],['r','Wi-Fi (dBm)'],['q','MQTT'],['u','Ligada h\u00e1'],['i','IP']]
 .map(([k,n])=>`<div class="c"><small>${n}</small><b id="v-${k}">\u2014</b></div>`).join('');
const f=(v,d)=>typeof v==='number'?v.toFixed(d):'\u2014';
function tempo(s){const h=Math.floor(s/3600),m=Math.floor(s%3600/60);return h?h+'h '+m+'min':m+'min '+(s%60)+'s';}
function graf(cv,series){
  const d=window.devicePixelRatio||1,w=cv.clientWidth,h=cv.clientHeight;
  cv.width=w*d;cv.height=h*d;
  const c=cv.getContext('2d');c.setTransform(d,0,0,d,0,0);c.font='10px sans-serif';
  const vals=[];
  series.forEach(s=>H.forEach(p=>{if(typeof p[s[0]]==='number')vals.push(p[s[0]]);}));
  if(!vals.length){c.fillStyle='#8892a6';c.fillText('Aguardando dados...',8,h/2);return;}
  let a=Math.min(...vals),b=Math.max(...vals);
  if(a===b){a-=1;b+=1;}
  const pd=(b-a)*.1;a-=pd;b+=pd;
  const L=36,X=i=>L+(H.length<2?0:i*(w-L-6)/(H.length-1)),Y=v=>6+(b-v)*(h-12)/(b-a);
  c.strokeStyle='#263049';c.fillStyle='#8892a6';c.lineWidth=1;
  for(let k=0;k<=3;k++){
    const y=6+k*(h-12)/3;
    c.beginPath();c.moveTo(L,y);c.lineTo(w-6,y);c.stroke();
    c.fillText((b-(b-a)*k/3).toFixed(1),2,y+3);
  }
  series.forEach(s=>{
    c.strokeStyle=s[1];c.lineWidth=1.6;c.beginPath();
    let on=false;
    H.forEach((q,i)=>{
      const v=q[s[0]];
      if(typeof v!=='number'){on=false;return;}
      if(!on){c.moveTo(X(i),Y(v));on=true;}else c.lineTo(X(i),Y(v));
    });
    c.stroke();
  });
}
async function atualizar(){
  try{
    const r=await fetch('/api',{cache:'no-store'});
    const d=await r.json();
    const v=d.vibracao||{};
    H.push({t:d.temperatura_analogica,x:v.x,y:v.y,z:v.z,m:d.mic_nivel});
    if(H.length>MAX)H.shift();
    $('v-t').textContent=f(d.temperatura_analogica,2);
    $('v-x').textContent=f(v.x,2);
    $('v-y').textContent=f(v.y,2);
    $('v-z').textContent=f(v.z,2);
    $('v-m').textContent=f(d.mic_nivel,0);
    $('v-r').textContent=f(d.rssi,0);
    $('v-q').textContent=d.mqtt?'conectado':'desconectado';
    $('v-u').textContent=tempo(d.uptime_s||0);
    $('v-i').textContent=d.ip||'\u2014';
    $('st').textContent='\u25CF online';$('st').style.color='#43a047';
    graf($('c1'),[['t','#4fc3f7']]);
    graf($('c2'),[['x','#ef5350'],['y','#ffee58'],['z','#42a5f5']]);
    graf($('c3'),[['m','#26c6da']]);
  }catch(e){
    $('st').textContent='\u25CF sem resposta';$('st').style.color='#e53935';
  }
}
atualizar();
setInterval(atualizar,1000);
</script></body></html>)rawliteral";

// =====================================================
// WIFI
// =====================================================

const char* textoStatusWiFi(wl_status_t status) {
  switch (status) {
    case WL_IDLE_STATUS:     return "ocioso";
    case WL_NO_SSID_AVAIL:   return "rede nao encontrada";
    case WL_SCAN_COMPLETED:  return "scan concluido";
    case WL_CONNECTED:       return "conectado";
    case WL_CONNECT_FAILED:  return "falha (senha errada?)";
    case WL_CONNECTION_LOST: return "conexao perdida";
    case WL_DISCONNECTED:    return "desconectado";
    default:                 return "desconhecido";
  }
}

// Escaneia as redes, lista no Serial e procura a rede configurada.
// Se achar, devolve o canal e o BSSID do ponto com sinal mais forte.
bool escanearRedes(int32_t &canal, uint8_t bssid[6]) {
  Serial.println("[WiFi] Escaneando redes...");

  int total = WiFi.scanNetworks(false, true);

  if (total < 0) {
    Serial.printf("[WiFi] Erro no scan (%d)\n", total);
    return false;
  }

  if (total == 0) {
    Serial.println("[WiFi] Nenhuma rede encontrada.");
    return false;
  }

  Serial.printf("[WiFi] %d redes encontradas:\n", total);

  int melhor = -1;
  int32_t melhorRSSI = -1000;

  for (int i = 0; i < total; i++) {
    Serial.printf(
      "  %2d) %-32s | %4d dBm | canal %2d\n",
      i + 1,
      WiFi.SSID(i).c_str(),
      WiFi.RSSI(i),
      WiFi.channel(i)
    );

    if (WiFi.SSID(i) == WIFI_SSID && WiFi.RSSI(i) > melhorRSSI) {
      melhor = i;
      melhorRSSI = WiFi.RSSI(i);
    }
  }

  bool encontrou = false;

  if (melhor >= 0) {
    canal = WiFi.channel(melhor);
    memcpy(bssid, WiFi.BSSID(melhor), 6);
    encontrou = true;

    Serial.printf(
      "[WiFi] Rede \"%s\" encontrada: %d dBm, canal %d\n",
      WIFI_SSID, melhorRSSI, canal
    );
  } else {
    Serial.printf(
      "[WiFi] Rede \"%s\" NAO apareceu no scan.\n",
      WIFI_SSID
    );
  }

  WiFi.scanDelete();
  return encontrou;
}

bool conectarWiFi() {
  ultimaTentativaWiFi = millis();

  Serial.println("\n[WiFi] Iniciando conexao...");

  // Ordem importa: primeiro liga o modo estacao, depois desconecta
  WiFi.persistent(false);
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.disconnect();
  delay(300);

  const int MAX_TENTATIVAS = 3;

  for (int tentativa = 1; tentativa <= MAX_TENTATIVAS; tentativa++) {
    Serial.printf(
      "\n[WiFi] Tentativa %d de %d\n",
      tentativa, MAX_TENTATIVAS
    );

    // Na ultima tentativa, reduz a potencia de transmissao.
    // Resolve problema comum em placas ESP32-C3/S3 mini.
    if (tentativa == MAX_TENTATIVAS) {
      Serial.println("[WiFi] Reduzindo potencia de transmissao...");
      WiFi.setTxPower(WIFI_POWER_8_5dBm);
    }

    int32_t canal = 0;
    uint8_t bssid[6];

    if (escanearRedes(canal, bssid)) {
      WiFi.begin(WIFI_SSID, WIFI_SENHA, canal, bssid);
    } else {
      WiFi.begin(WIFI_SSID, WIFI_SENHA);
    }

    unsigned long inicio = millis();

    while (WiFi.status() != WL_CONNECTED &&
           millis() - inicio < 15000) {
      delay(500);
      Serial.print(".");
    }

    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("\n[WiFi] Conectado!");
      Serial.print("[WiFi] IP: ");
      Serial.println(WiFi.localIP());
      Serial.printf("[WiFi] Sinal: %d dBm\n", WiFi.RSSI());
      return true;
    }

    wl_status_t status = WiFi.status();
    Serial.printf(
      "\n[WiFi] Falhou. Estado: %d (%s)\n",
      status, textoStatusWiFi(status)
    );

    WiFi.disconnect();
    delay(1000);
  }

  Serial.println("[WiFi] Nao foi possivel conectar. Tentando de novo em 30 s.");
  return false;
}

// =====================================================
// MQTT
// =====================================================

void conectarMQTT() {
  if (WiFi.status() != WL_CONNECTED || mqtt.connected()) {
    return;
  }

  if (millis() - ultimaTentativaMQTT < 5000) {
    return;
  }

  ultimaTentativaMQTT = millis();

  String idCliente = "SmartPredict-ESP32-" + WiFi.macAddress();

  Serial.print("[MQTT] Conectando ao HiveMQ... ");

  if (mqtt.connect(
        idCliente.c_str(),
        MQTT_USUARIO,
        MQTT_SENHA)) {
    Serial.println("conectado!");
    Serial.print("[MQTT] Topico: ");
    Serial.println(MQTT_TOPICO);
  } else {
    Serial.print("falhou. Estado: ");
    Serial.println(mqtt.state());
  }
}

// =====================================================
// LEITURA MPU6050
// =====================================================

void lerMPU6050() {
  if (!mpuOK) {
    Serial.println("MPU6050: indisponivel");
    return;
  }

  sensors_event_t aceleracao, giro, temperaturaMPU;
  mpu.getEvent(&aceleracao, &giro, &temperaturaMPU);

  vibX = aceleracao.acceleration.x;
  vibY = aceleracao.acceleration.y;
  vibZ = aceleracao.acceleration.z;

  Serial.printf(
    "Vibracao X: %.3f | Y: %.3f | Z: %.3f m/s2\n",
    vibX, vibY, vibZ
  );
}

// =====================================================
// LEITURA DS18B20
// =====================================================

void lerTemperatura() {
  if (!tempOK) {
    Serial.println("DS18B20: indisponivel");
    return;
  }

  tempSensor.requestTemperatures();

  float temperatura = tempSensor.getTempCByIndex(0);

  if (temperatura == DEVICE_DISCONNECTED_C ||
      temperatura < -55 || temperatura > 125) {
    temperaturaMaquina = NAN;
    Serial.println("DS18B20: falha na leitura");
  } else {
    temperaturaMaquina = temperatura;
    Serial.printf(
      "Temperatura da maquina: %.2f C\n",
      temperaturaMaquina
    );
  }
}

// =====================================================
// LEITURA MICROFONE
// =====================================================
// O sinal do microfone oscila em volta de um valor medio.
// Uma leitura unica nao diz nada; o que indica o volume
// e a diferenca entre o pico maximo e minimo numa janela.

void lerMicrofone() {
  int minimo = 4095;
  int maximo = 0;
  long soma = 0;
  int amostras = 0;

  unsigned long inicio = millis();

  while (millis() - inicio < JANELA_MIC_MS) {
    int leitura = analogRead(MIC_PIN);
    if (leitura < minimo) minimo = leitura;
    if (leitura > maximo) maximo = leitura;
    soma += leitura;
    amostras++;
  }

  int media = amostras > 0 ? soma / amostras : 0;

  // Sinal grudado em 0 ou 4095 = fio solto, pino errado ou sem alimentacao
  if ((maximo == 0) || (minimo == 4095)) {
    micOK = false;
    micNivel = -1;
    Serial.printf(
      "Microfone: sinal travado em %d (verifique ligacao/pino)\n",
      media
    );
    return;
  }

  micOK = true;
  micNivel = maximo - minimo;  // pico a pico (0 a 4095)

  Serial.printf(
    "Microfone: nivel %d (min %d | max %d | media %d | %d amostras)\n",
    micNivel, minimo, maximo, media, amostras
  );
}

// =====================================================
// MONTAR JSON (usado no MQTT e no painel web)
// =====================================================
// extras = false: mensagem do MQTT (inclui o IP para o Raspberry)
// extras = true : resposta do painel web (inclui rssi, mqtt e uptime)

void montarJSON(char* destino, size_t tamanho, bool extras) {
  char tempJSON[24];

  if (isnan(temperaturaMaquina)) {
    snprintf(tempJSON, sizeof(tempJSON), "null");
  } else {
    snprintf(
      tempJSON, sizeof(tempJSON), "%.2f",
      temperaturaMaquina
    );
  }

  char micJSON[16];

  if (!micOK || micNivel < 0) {
    snprintf(micJSON, sizeof(micJSON), "null");
  } else {
    snprintf(micJSON, sizeof(micJSON), "%d", micNivel);
  }

  String ip = WiFi.localIP().toString();

  int n = snprintf(
    destino,
    tamanho,
    "{\"mic_nivel\":%s,"
    "\"temperatura_analogica\":%s,"
    "\"vibracao\":{\"x\":%.3f,\"y\":%.3f,\"z\":%.3f},"
    "\"ip\":\"%s\"",
    micJSON,
    tempJSON,
    vibX,
    vibY,
    vibZ,
    ip.c_str()
  );

  if (extras && n > 0 && (size_t)n < tamanho) {
    n += snprintf(
      destino + n,
      tamanho - n,
      ",\"rssi\":%d,\"mqtt\":%s,\"uptime_s\":%lu",
      WiFi.RSSI(),
      mqtt.connected() ? "true" : "false",
      millis() / 1000UL
    );
  }

  if (n > 0 && (size_t)n < tamanho - 1) {
    destino[n] = '}';
    destino[n + 1] = '\0';
  }
}

// =====================================================
// PUBLICAR DADOS MQTT
// =====================================================

void publicarDados() {
  if (!mqtt.connected()) {
    return;
  }

  char mensagem[320];
  montarJSON(mensagem, sizeof(mensagem), false);

  Serial.print("[MQTT] Publicando: ");
  Serial.println(mensagem);

  if (mqtt.publish(MQTT_TOPICO, mensagem)) {
    Serial.println("[MQTT] Publicacao enviada!");
  } else {
    Serial.println("[MQTT] Falha ao publicar.");
  }
}

// =====================================================
// SERVIDOR WEB DA ESP32
// =====================================================

void rotaPagina() {
  servidor.send_P(200, "text/html; charset=utf-8", PAGINA_HTML);
}

void rotaApi() {
  char json[384];
  montarJSON(json, sizeof(json), true);

  servidor.sendHeader("Access-Control-Allow-Origin", "*");
  servidor.sendHeader("Cache-Control", "no-store");
  servidor.send(200, "application/json", json);
}

void iniciarServidor() {
  if (servidorIniciado || WiFi.status() != WL_CONNECTED) {
    return;
  }

  if (MDNS.begin(NOME_MDNS)) {
    MDNS.addService("http", "tcp", 80);
    Serial.printf("[WEB] Nome na rede: http://%s.local\n", NOME_MDNS);
  } else {
    Serial.println("[WEB] mDNS nao iniciou (use o IP).");
  }

  servidor.on("/", rotaPagina);
  servidor.on("/api", rotaApi);
  servidor.onNotFound([]() {
    servidor.send(404, "text/plain", "Nao encontrado");
  });

  servidor.begin();
  servidorIniciado = true;

  Serial.printf(
    "[WEB] Painel da ESP32: http://%s\n",
    WiFi.localIP().toString().c_str()
  );
}

// =====================================================
// SETUP
// =====================================================

void setup() {
  Serial.begin(115200);
  delay(2000);

  Serial.println("\n=== SMARTPREDICT: MPU6050 + DS18B20 + MIC + MQTT + PAINEL ===");

  // Wi-Fi primeiro (antes dos sensores), igual ao codigo de scan
  conectarWiFi();

  // MPU6050
  Serial.println("\n[Testando MPU6050...]");

  Wire.begin(SDA_PIN, SCL_PIN);
  Wire.setClock(100000);

  if (mpu.begin()) {
    mpuOK = true;
    mpu.setAccelerometerRange(MPU6050_RANGE_8_G);
    mpu.setGyroRange(MPU6050_RANGE_500_DEG);
    mpu.setFilterBandwidth(MPU6050_BAND_21_HZ);
    Serial.println("[OK] MPU6050 encontrado!");
  } else {
    Serial.println("[ERRO] MPU6050 nao encontrado.");
  }

  // DS18B20
  Serial.println("\n[Testando DS18B20...]");

  tempSensor.begin();
  tempSensor.setWaitForConversion(false);

  if (tempSensor.getDeviceCount() > 0) {
    tempOK = true;
    Serial.println("[OK] DS18B20 encontrado!");
  } else {
    Serial.println("[ERRO] DS18B20 nao encontrado.");
  }

  // Microfone
  Serial.println("\n[Configurando microfone...]");

  pinMode(MIC_PIN, INPUT);
  analogReadResolution(12);                      // 0 a 4095
  analogSetPinAttenuation(MIC_PIN, ADC_11db);    // faixa ate ~3.3V

  lerMicrofone();
  if (micOK) {
    Serial.println("[OK] Microfone respondendo!");
  } else {
    Serial.println("[ERRO] Microfone sem sinal valido.");
  }

  // SOMENTE PARA TESTE INICIAL.
  // Antes de usar definitivamente, configure a CA raiz do HiveMQ.
  conexaoTLS.setInsecure();

  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  mqtt.setSocketTimeout(15);
  mqtt.setKeepAlive(60);
  mqtt.setBufferSize(512);

  iniciarServidor();
  conectarMQTT();

  Serial.println("\nIniciando leituras...");
}

// =====================================================
// LOOP
// =====================================================

void loop() {
  if (WiFi.status() != WL_CONNECTED &&
      millis() - ultimaTentativaWiFi >= INTERVALO_RECONEXAO_WIFI) {
    conectarWiFi();
  }

  // Inicia o painel assim que houver Wi-Fi (inclusive apos reconexao)
  iniciarServidor();

  conectarMQTT();

  if (mqtt.connected()) {
    mqtt.loop();
  }

  Serial.println("\n========== LEITURAS ==========");

  lerMPU6050();
  lerTemperatura();
  lerMicrofone();

  Serial.println("==============================");

  if (millis() - ultimaPublicacao >= INTERVALO_MQTT) {
    ultimaPublicacao = millis();
    publicarDados();
  }

  // Espera 500 ms, mas continua atendendo o painel web e o MQTT
  unsigned long espera = millis();

  while (millis() - espera < 500) {
    if (servidorIniciado) {
      servidor.handleClient();
    }

    if (mqtt.connected()) {
      mqtt.loop();
    }

    delay(5);
  }
}

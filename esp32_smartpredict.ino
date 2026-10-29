#include <Arduino.h>
#include <Wire.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <Adafruit_MPU6050.h>
#include <Adafruit_Sensor.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <esp_sleep.h>
#include <esp_system.h>
#include <time.h>
#include <stdarg.h>
#include <math.h>

// =====================================================
// COMO FUNCIONA (MODO ECONOMIA)
// =====================================================
// A ESP32 passa quase todo o tempo em DEEP SLEEP.
//
// A cada 5 s ela acorda, le os sensores SEM ligar o Wi-Fi
// (~0,4 s acordada), guarda a leitura na memoria RTC (que
// sobrevive ao sono) e volta a dormir.
//
// A cada 12 leituras (1 min) ela liga o Wi-Fi, envia todas as
// leituras de uma vez pelo MQTT (HiveMQ), envia o console e
// desliga o Wi-Fi de novo.
//
// Se o Wi-Fi falhar, as leituras ficam guardadas (ate 36) e
// as tentativas ficam mais espacadas para nao gastar bateria.
//
// GRAVAR CODIGO NOVO: enquanto dorme, a ESP32 some da USB.
// Segure o botao BOOT, aperte e solte RESET, solte BOOT e
// clique em Carregar na Arduino IDE.

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

// Mensagens do console (o mesmo texto do Monitor Serial)
const char* MQTT_TOPICO_CONSOLE =
  "senai/manutencao/maquina01/console";

// =====================================================
// TEMPOS (ajuste aqui)
// =====================================================

const uint32_t INTERVALO_LEITURA_MS = 5000;  // uma leitura a cada 5 s
const uint8_t AMOSTRAS_POR_ENVIO = 12;       // 12 x 5 s = envio a cada 1 min
                                             // (24 = a cada 2 min: gasta ainda menos)
const uint8_t MAX_AMOSTRAS = 36;             // fila maxima se o Wi-Fi cair
const uint8_t MAX_MULTIPLICADOR_ESPERA = 10; // apos falhas: tenta no maximo a cada 10 min

const uint32_t TIMEOUT_WIFI_RAPIDO_MS = 10000;  // conexao direta (canal/BSSID guardados)
const uint32_t TIMEOUT_WIFI_SCAN_MS = 15000;    // conexao depois de escanear

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
// SENSORES
// =====================================================

const uint32_t JANELA_MIC_MS = 50;     // janela de amostragem do som

// Vibracao: 200 amostras a 1 kHz (0,2 s). Com isso da para
// calcular o RMS (intensidade da vibracao) e o pico, que dizem
// muito mais sobre a maquina do que uma leitura unica.
const uint16_t AMOSTRAS_VIB = 200;
const uint32_t PERIODO_VIB_US = 1000;

// DS18B20 em 10 bits: precisao de 0,25 C e conversao de ~190 ms
// (em 12 bits sao 750 ms com a CPU acordada).
const uint8_t RESOLUCAO_DS18B20 = 10;

// MPU6050 (acesso direto aos registradores para acordar e
// dormir rapido, sem o reset de ~200 ms da biblioteca)
const uint8_t MPU_ADDR = 0x68;
const uint8_t MPU_SMPLRT_DIV = 0x19;
const uint8_t MPU_CONFIG = 0x1A;
const uint8_t MPU_ACCEL_CONFIG = 0x1C;
const uint8_t MPU_ACCEL_XOUT_H = 0x3B;
const uint8_t MPU_PWR_MGMT_1 = 0x6B;
const uint8_t MPU_PWR_MGMT_2 = 0x6C;

const uint8_t MPU_ACORDADO = 0x08;   // relogio interno, sensor de temperatura desligado
const uint8_t MPU_DORMINDO = 0x48;   // idem + modo sleep (~5 uA)
const float MPU_LSB_POR_G = 4096.0f; // faixa +-8 g
const float GRAVIDADE = 9.80665f;

// =====================================================
// LEITURAS GUARDADAS (memoria RTC: sobrevive ao deep sleep)
// =====================================================

const int16_t SEM_TEMPERATURA = INT16_MIN;

struct Amostra {
  uint32_t ts;      // relogio interno da ESP32 (o Raspberry corrige para a hora real)
  int16_t temp100;  // temperatura x 100 (SEM_TEMPERATURA = sem leitura)
  int16_t mic;      // nivel pico a pico (-1 = sem leitura)
  bool vibOK;
  float vx, vy, vz; // media (posicao / gravidade), m/s2
  float rx, ry, rz; // RMS da vibracao, m/s2
  float pico;       // maior desvio em relacao a media, m/s2
};

RTC_DATA_ATTR Amostra amostras[MAX_AMOSTRAS];
RTC_DATA_ATTR uint8_t nAmostras = 0;
RTC_DATA_ATTR uint8_t amostrasDesdeTentativa = 0;
RTC_DATA_ATTR uint8_t falhasSeguidas = 0;
RTC_DATA_ATTR uint32_t perdidas = 0;   // leituras descartadas por fila cheia
RTC_DATA_ATTR uint32_t ciclo = 0;

// Wi-Fi: guarda canal e roteador para reconectar sem escanear
RTC_DATA_ATTR bool wifiCacheOK = false;
RTC_DATA_ATTR int32_t wifiCanal = 0;
RTC_DATA_ATTR uint8_t wifiBSSID[6];
RTC_DATA_ATTR bool txReduzida = false;

// =====================================================
// CONSOLE REMOTO
// =====================================================
// Funciona igual ao Serial (print, println, printf): escreve no
// Monitor Serial normalmente e guarda uma copia na memoria RTC.
// A copia e enviada pelo MQTT junto com o lote e depois limpa.
// Se ficar cheio, descarta as mensagens mais antigas.

const size_t CONSOLE_MAX = 2400;
RTC_DATA_ATTR char consoleBuf[CONSOLE_MAX + 1];
RTC_DATA_ATTR uint16_t consoleTam = 0;

class ConsoleRemoto : public Print {
 public:
  size_t write(uint8_t c) override {
    Serial.write(c);
    guardar(c);
    return 1;
  }

  size_t write(const uint8_t* dados, size_t tamanhoDados) override {
    Serial.write(dados, tamanhoDados);
    for (size_t i = 0; i < tamanhoDados; i++) {
      guardar(dados[i]);
    }
    return tamanhoDados;
  }

  bool vazio() const { return consoleTam == 0; }

  const char* texto() {
    consoleBuf[consoleTam] = '\0';
    return consoleBuf;
  }

  size_t tamanho() const { return consoleTam; }

  void limpar() { consoleTam = 0; }

 private:
  void guardar(uint8_t c) {
    if (c == '\r') return;

    if (consoleTam >= CONSOLE_MAX) {
      // Cheio: descarta a metade mais antiga, cortando numa quebra de linha.
      size_t corte = CONSOLE_MAX / 2;
      while (corte < consoleTam && consoleBuf[corte] != '\n') corte++;
      if (corte < consoleTam) corte++;

      memmove(consoleBuf, consoleBuf + corte, consoleTam - corte);
      consoleTam -= corte;
    }

    consoleBuf[consoleTam++] = (char)c;
  }
};

ConsoleRemoto Console;

// =====================================================
// OBJETOS
// =====================================================

Adafruit_MPU6050 mpu;
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature tempSensor(&oneWire);

WiFiClientSecure conexaoTLS;
PubSubClient mqtt(conexaoTLS);

// Lote JSON (36 leituras cabem com folga)
const size_t TAM_LOTE_JSON = 8192;
const uint16_t TAM_BUFFER_MQTT = 8448;
char loteJSON[TAM_LOTE_JSON];

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

// Escaneia as redes, lista no console e procura a rede configurada.
// Se achar, devolve o canal e o BSSID do ponto com sinal mais forte.
bool escanearRedes(int32_t &canal, uint8_t bssid[6], int32_t &rssi) {
  Console.println("[WiFi] Escaneando redes...");

  int total = WiFi.scanNetworks(false, true);

  if (total < 0) {
    Console.printf("[WiFi] Erro no scan (%d)\n", total);
    return false;
  }

  if (total == 0) {
    Console.println("[WiFi] Nenhuma rede encontrada.");
    return false;
  }

  Console.printf("[WiFi] %d redes encontradas:\n", total);

  int melhor = -1;
  int32_t melhorRSSI = -1000;

  for (int i = 0; i < total; i++) {
    Console.printf(
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
    rssi = melhorRSSI;
    encontrou = true;

    Console.printf(
      "[WiFi] Rede \"%s\" encontrada: %d dBm, canal %d\n",
      WIFI_SSID, melhorRSSI, canal
    );
  } else {
    Console.printf(
      "[WiFi] Rede \"%s\" NAO apareceu no scan.\n",
      WIFI_SSID
    );
  }

  WiFi.scanDelete();
  return encontrou;
}

// Espera conectar; se conseguir, guarda canal e BSSID para a proxima vez.
bool esperarWiFi(uint32_t limiteMs) {
  unsigned long inicio = millis();

  while (WiFi.status() != WL_CONNECTED &&
         millis() - inicio < limiteMs) {
    delay(100);
  }

  if (WiFi.status() == WL_CONNECTED) {
    wifiCanal = WiFi.channel();
    memcpy(wifiBSSID, WiFi.BSSID(), 6);
    wifiCacheOK = true;

    Console.printf(
      "[WiFi] Conectado em %lu ms | IP %s | %d dBm\n",
      millis() - inicio,
      WiFi.localIP().toString().c_str(),
      WiFi.RSSI()
    );
    return true;
  }

  wl_status_t status = WiFi.status();
  Console.printf(
    "[WiFi] Falhou. Estado: %d (%s)\n",
    status, textoStatusWiFi(status)
  );
  return false;
}

// Desliga e religa o radio por completo. Sem isso, se a tentativa
// anterior ainda estiver "conectando", a ESP32 recusa a nova
// configuracao ("sta is connecting, cannot set config").
void prepararRadio(bool potenciaReduzida) {
  WiFi.disconnect(true);
  WiFi.mode(WIFI_OFF);
  delay(100);

  WiFi.persistent(false);
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(true);  // modem sleep enquanto conectado

  if (potenciaReduzida) {
    // Resolve problema comum em placas ESP32-C3/S3 mini
    // (antena ruim que nao conecta na potencia maxima).
    WiFi.setTxPower(WIFI_POWER_8_5dBm);
  }
}

bool tentarConectar(bool potenciaReduzida, bool usarRoteador,
                    int32_t canal, const uint8_t* bssid, uint32_t limiteMs) {
  prepararRadio(potenciaReduzida);

  Console.printf("[WiFi] Tentando com potencia %s...\n",
                 potenciaReduzida ? "reduzida" : "normal");

  if (usarRoteador) {
    WiFi.begin(WIFI_SSID, WIFI_SENHA, canal, bssid);
  } else {
    WiFi.begin(WIFI_SSID, WIFI_SENHA);
  }

  if (esperarWiFi(limiteMs)) {
    if (potenciaReduzida != txReduzida) {
      txReduzida = potenciaReduzida;
      Console.printf("[WiFi] Potencia %s funcionou; vou usar ela nas proximas vezes.\n",
                     potenciaReduzida ? "reduzida" : "normal");
    }
    return true;
  }

  return false;
}

bool conectarWiFi() {
  // 1) Caminho rapido: canal e roteador ja conhecidos (sem scan),
  //    com a potencia que funcionou da ultima vez.
  if (wifiCacheOK) {
    Console.printf("[WiFi] Conectando direto (canal %ld)...\n", (long)wifiCanal);

    if (tentarConectar(txReduzida, true, wifiCanal, wifiBSSID, TIMEOUT_WIFI_RAPIDO_MS)) {
      return true;
    }

    Console.println("[WiFi] Conexao direta falhou; vou escanear.");
    wifiCacheOK = false;
  }

  // 2) Escaneia para achar o ponto com melhor sinal
  prepararRadio(txReduzida);

  int32_t canal = 0;
  uint8_t bssid[6];
  int32_t rssi = -1000;
  bool achou = escanearRedes(canal, bssid, rssi);

  if (achou && rssi < -80) {
    Console.printf("[WiFi] AVISO: sinal muito fraco (%ld dBm). "
                   "Aproxime a ESP32 do roteador (ideal: acima de -75 dBm).\n",
                   (long)rssi);
  }

  // 3) Primeiro com a potencia que funcionou da ultima vez,
  //    depois com a outra.
  if (tentarConectar(txReduzida, achou, canal, bssid, TIMEOUT_WIFI_SCAN_MS)) {
    return true;
  }

  if (tentarConectar(!txReduzida, achou, canal, bssid, TIMEOUT_WIFI_SCAN_MS)) {
    return true;
  }

  Console.println("[WiFi] Nao foi possivel conectar.");
  return false;
}

void desligarWiFi() {
  WiFi.disconnect(true);
  WiFi.mode(WIFI_OFF);
}

// =====================================================
// MPU6050 — VIBRACAO
// =====================================================

bool mpuEscrever(uint8_t registrador, uint8_t valor) {
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(registrador);
  Wire.write(valor);
  return Wire.endTransmission() == 0;
}

bool mpuLerAceleracao(int16_t valores[3]) {
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(MPU_ACCEL_XOUT_H);

  if (Wire.endTransmission(false) != 0) {
    return false;
  }

  if (Wire.requestFrom((uint8_t)MPU_ADDR, (uint8_t)6) != 6) {
    return false;
  }

  for (int k = 0; k < 3; k++) {
    uint8_t alto = Wire.read();
    uint8_t baixo = Wire.read();
    valores[k] = (int16_t)((alto << 8) | baixo);
  }

  return true;
}

// Acorda o MPU, coleta 200 amostras a 1 kHz, calcula media,
// RMS e pico e coloca o MPU para dormir de novo.
bool lerVibracao(Amostra &a) {
  if (!mpuEscrever(MPU_PWR_MGMT_1, MPU_ACORDADO)) {
    Console.println("MPU6050: nao responde");
    return false;
  }

  // Reaplica a configuracao a cada leitura: se o MPU perder
  // energia por um instante, ele volta configurado certo.
  mpuEscrever(MPU_PWR_MGMT_2, 0x07);    // giroscopio em standby (nao e usado)
  mpuEscrever(MPU_SMPLRT_DIV, 0x00);    // 1 kHz
  mpuEscrever(MPU_CONFIG, 0x01);        // filtro passa-baixa de 184 Hz
  mpuEscrever(MPU_ACCEL_CONFIG, 0x10);  // faixa +-8 g

  delay(30);  // acelerometro estabiliza depois de sair do sono

  double soma[3] = {0, 0, 0};
  double somaQuadrados[3] = {0, 0, 0};
  float minimo[3] = {1e9f, 1e9f, 1e9f};
  float maximo[3] = {-1e9f, -1e9f, -1e9f};
  uint16_t validas = 0;

  unsigned long proxima = micros();

  for (uint16_t i = 0; i < AMOSTRAS_VIB; i++) {
    while ((long)(micros() - proxima) < 0) {
      // espera o proximo instante (1 ms)
    }
    proxima += PERIODO_VIB_US;

    int16_t bruto[3];
    if (!mpuLerAceleracao(bruto)) {
      continue;
    }

    for (int k = 0; k < 3; k++) {
      float v = bruto[k] / MPU_LSB_POR_G * GRAVIDADE;
      soma[k] += v;
      somaQuadrados[k] += (double)v * v;
      if (v < minimo[k]) minimo[k] = v;
      if (v > maximo[k]) maximo[k] = v;
    }
    validas++;
  }

  mpuEscrever(MPU_PWR_MGMT_1, MPU_DORMINDO);

  if (validas < AMOSTRAS_VIB / 2) {
    Console.printf("MPU6050: leitura falhou (%u de %u amostras)\n",
                   validas, AMOSTRAS_VIB);
    return false;
  }

  float media[3];
  float rms[3];
  float pico = 0;

  for (int k = 0; k < 3; k++) {
    media[k] = soma[k] / validas;
    double variancia = somaQuadrados[k] / validas - (double)media[k] * media[k];
    rms[k] = variancia > 0 ? sqrt(variancia) : 0;

    float desvio = max(maximo[k] - media[k], media[k] - minimo[k]);
    if (desvio > pico) pico = desvio;
  }

  a.vx = media[0];
  a.vy = media[1];
  a.vz = media[2];
  a.rx = rms[0];
  a.ry = rms[1];
  a.rz = rms[2];
  a.pico = pico;
  return true;
}

// =====================================================
// DS18B20 — TEMPERATURA DA MAQUINA
// =====================================================
// A conversao (~190 ms) roda em paralelo com a leitura da
// vibracao e do microfone, para a ESP32 dormir mais cedo.

bool iniciarTemperatura() {
  tempSensor.begin();

  if (tempSensor.getDeviceCount() == 0) {
    Console.println("DS18B20: indisponivel");
    return false;
  }

  if (tempSensor.getResolution() != RESOLUCAO_DS18B20) {
    tempSensor.setResolution(RESOLUCAO_DS18B20);
  }

  tempSensor.setWaitForConversion(false);
  tempSensor.requestTemperatures();
  return true;
}

void terminarTemperatura(Amostra &a, unsigned long inicio) {
  unsigned long limite =
    tempSensor.millisToWaitForConversion(RESOLUCAO_DS18B20) + 50;

  while (!tempSensor.isConversionComplete() &&
         millis() - inicio < limite) {
    delay(5);
  }

  float temperatura = tempSensor.getTempCByIndex(0);

  if (temperatura == DEVICE_DISCONNECTED_C ||
      temperatura < -55 || temperatura > 125) {
    a.temp100 = SEM_TEMPERATURA;
    Console.println("DS18B20: falha na leitura");
  } else {
    a.temp100 = (int16_t)lroundf(temperatura * 100.0f);
  }
}

// =====================================================
// MICROFONE
// =====================================================
// O sinal do microfone oscila em volta de um valor medio.
// Uma leitura unica nao diz nada; o que indica o volume
// e a diferenca entre o pico maximo e minimo numa janela.

bool lerMicrofone(Amostra &a, bool detalhar) {
  pinMode(MIC_PIN, INPUT);
  analogReadResolution(12);                    // 0 a 4095
  analogSetPinAttenuation(MIC_PIN, ADC_11db);  // faixa ate ~3.3V

  int minimo = 4095;
  int maximo = 0;
  long soma = 0;
  int amostrasMic = 0;

  unsigned long inicio = millis();

  while (millis() - inicio < JANELA_MIC_MS) {
    int leitura = analogRead(MIC_PIN);
    if (leitura < minimo) minimo = leitura;
    if (leitura > maximo) maximo = leitura;
    soma += leitura;
    amostrasMic++;
  }

  int media = amostrasMic > 0 ? soma / amostrasMic : 0;

  // Sinal grudado em 0 ou 4095 = fio solto, pino errado ou sem alimentacao
  if ((maximo == 0) || (minimo == 4095)) {
    a.mic = -1;
    Console.printf(
      "Microfone: sinal travado em %d (verifique ligacao/pino)\n",
      media
    );
    return false;
  }

  a.mic = maximo - minimo;  // pico a pico (0 a 4095)

  if (detalhar) {
    Console.printf(
      "Microfone: nivel %d (min %d | max %d | media %d | %d amostras)\n",
      a.mic, minimo, maximo, media, amostrasMic
    );
  }

  return true;
}

// =====================================================
// COLETA DE UMA LEITURA
// =====================================================

void guardarAmostra(const Amostra &a) {
  if (nAmostras >= MAX_AMOSTRAS) {
    // Fila cheia (sem Wi-Fi ha muito tempo): descarta a mais antiga
    memmove(&amostras[0], &amostras[1], sizeof(Amostra) * (MAX_AMOSTRAS - 1));
    nAmostras = MAX_AMOSTRAS - 1;
    perdidas++;
  }

  amostras[nAmostras++] = a;

  if (amostrasDesdeTentativa < 250) {
    amostrasDesdeTentativa++;
  }
}

void imprimirResumo(const Amostra &a) {
  char temp[16];
  char mic[8];

  if (a.temp100 == SEM_TEMPERATURA) {
    snprintf(temp, sizeof(temp), "temp --");
  } else {
    snprintf(temp, sizeof(temp), "%.2f C", a.temp100 / 100.0f);
  }

  if (a.mic < 0) {
    snprintf(mic, sizeof(mic), "--");
  } else {
    snprintf(mic, sizeof(mic), "%d", a.mic);
  }

  if (a.vibOK) {
    Console.printf(
      "[#%lu] RMS %.3f/%.3f/%.3f pico %.3f | %s | mic %s | fila %u\n",
      (unsigned long)ciclo, a.rx, a.ry, a.rz, a.pico,
      temp, mic, nAmostras
    );
  } else {
    Console.printf(
      "[#%lu] vibracao -- | %s | mic %s | fila %u\n",
      (unsigned long)ciclo, temp, mic, nAmostras
    );
  }
}

void coletarAmostra(bool detalhar) {
  Amostra a;
  memset(&a, 0, sizeof(a));
  a.ts = (uint32_t)time(nullptr);
  a.temp100 = SEM_TEMPERATURA;
  a.mic = -1;

  unsigned long inicioTemp = millis();
  bool temTemperatura = iniciarTemperatura();  // conversao comeca agora

  a.vibOK = lerVibracao(a);                    // ~0,23 s
  lerMicrofone(a, detalhar);                   // 0,05 s

  if (temTemperatura) {
    terminarTemperatura(a, inicioTemp);        // ja quase pronta
  }

  guardarAmostra(a);
  imprimirResumo(a);
}

// =====================================================
// MONTAR LOTE JSON
// =====================================================
// Formato:
// {"ip":"...","agora":<relogio ESP>,"perdidas":N,"lote":[
//   {"ts":..,"mic_nivel":..,"temperatura_analogica":..,
//    "vibracao":{"x":..,"y":..,"z":..},
//    "vibracao_rms":{"x":..,"y":..,"z":..},
//    "vibracao_pico":..}, ...]}
// "agora" permite ao Raspberry converter cada "ts" para a hora real.

bool anexar(char* destino, size_t tamanho, size_t &pos, const char* formato, ...) {
  if (pos >= tamanho) return false;

  va_list args;
  va_start(args, formato);
  int n = vsnprintf(destino + pos, tamanho - pos, formato, args);
  va_end(args);

  if (n < 0 || (size_t)n >= tamanho - pos) {
    pos = tamanho;
    return false;
  }

  pos += n;
  return true;
}

size_t montarLote(char* destino, size_t tamanho, const char* ip, uint32_t agora) {
  size_t pos = 0;

  bool ok = anexar(
    destino, tamanho, pos,
    "{\"ip\":\"%s\",\"agora\":%lu,\"perdidas\":%lu,\"lote\":[",
    ip, (unsigned long)agora, (unsigned long)perdidas
  );

  for (uint8_t i = 0; ok && i < nAmostras; i++) {
    const Amostra &a = amostras[i];

    char temp[16];
    char mic[8];

    if (a.temp100 == SEM_TEMPERATURA) {
      snprintf(temp, sizeof(temp), "null");
    } else {
      snprintf(temp, sizeof(temp), "%.2f", a.temp100 / 100.0f);
    }

    if (a.mic < 0) {
      snprintf(mic, sizeof(mic), "null");
    } else {
      snprintf(mic, sizeof(mic), "%d", a.mic);
    }

    ok = anexar(
      destino, tamanho, pos,
      "%s{\"ts\":%lu,\"mic_nivel\":%s,\"temperatura_analogica\":%s,",
      i ? "," : "", (unsigned long)a.ts, mic, temp
    );

    if (!ok) break;

    if (a.vibOK) {
      ok = anexar(
        destino, tamanho, pos,
        "\"vibracao\":{\"x\":%.3f,\"y\":%.3f,\"z\":%.3f},"
        "\"vibracao_rms\":{\"x\":%.4f,\"y\":%.4f,\"z\":%.4f},"
        "\"vibracao_pico\":%.4f}",
        a.vx, a.vy, a.vz, a.rx, a.ry, a.rz, a.pico
      );
    } else {
      ok = anexar(
        destino, tamanho, pos,
        "\"vibracao\":null,\"vibracao_rms\":null,\"vibracao_pico\":null}"
      );
    }
  }

  if (ok) {
    ok = anexar(destino, tamanho, pos, "]}");
  }

  return ok ? pos : 0;
}

// =====================================================
// ENVIO PELO MQTT
// =====================================================

// Envia tudo o que foi impresso desde o ultimo envio.
// Nao imprime nada aqui dentro (senao a mensagem iria para o
// proprio buffer que esta sendo enviado).
void publicarConsole() {
  if (Console.vazio() || !mqtt.connected()) {
    return;
  }

  if (mqtt.publish(MQTT_TOPICO_CONSOLE,
                   (const uint8_t*)Console.texto(),
                   Console.tamanho(), false)) {
    Console.limpar();
  }
}

bool publicarTudo() {
  // Mantido como no codigo original.
  // setInsecure() nao valida o certificado do broker; para producao,
  // configure a CA raiz do HiveMQ antes de expor o dispositivo.
  conexaoTLS.setInsecure();

  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  mqtt.setSocketTimeout(10);
  mqtt.setKeepAlive(30);
  mqtt.setBufferSize(TAM_BUFFER_MQTT);

  String idCliente = "SmartPredict-ESP32-" + WiFi.macAddress();

  Console.print("[MQTT] Conectando ao HiveMQ... ");
  unsigned long inicio = millis();

  if (!mqtt.connect(idCliente.c_str(), MQTT_USUARIO, MQTT_SENHA)) {
    Console.printf("falhou. Estado: %d\n", mqtt.state());
    return false;
  }

  Console.printf("conectado (%lu ms).\n", millis() - inicio);

  bool ok = false;
  size_t tamanho = montarLote(
    loteJSON, sizeof(loteJSON),
    WiFi.localIP().toString().c_str(),
    (uint32_t)time(nullptr)
  );

  if (tamanho == 0) {
    Console.println("[MQTT] Lote nao coube no buffer; envio cancelado.");
  } else if (mqtt.publish(MQTT_TOPICO, (const uint8_t*)loteJSON, tamanho, false)) {
    Console.printf("[MQTT] Lote enviado: %u leituras (%u bytes).\n",
                   nAmostras, (unsigned)tamanho);
    nAmostras = 0;
    perdidas = 0;
    ok = true;
  } else {
    Console.println("[MQTT] Falha ao publicar o lote.");
  }

  publicarConsole();

  // Da tempo dos pacotes sairem antes de fechar a conexao.
  mqtt.loop();
  delay(50);
  mqtt.disconnect();
  conexaoTLS.stop();

  return ok;
}

// Decide se ja e hora de tentar enviar.
// Depois de falhas, espera 2x, 4x, 8x... (no maximo 10x) mais.
bool horaDeEnviar() {
  uint8_t multiplicador = 1;

  for (uint8_t i = 0; i < falhasSeguidas && multiplicador < MAX_MULTIPLICADOR_ESPERA; i++) {
    multiplicador *= 2;
  }

  if (multiplicador > MAX_MULTIPLICADOR_ESPERA) {
    multiplicador = MAX_MULTIPLICADOR_ESPERA;
  }

  return amostrasDesdeTentativa >= AMOSTRAS_POR_ENVIO * multiplicador;
}

void enviarLote() {
  amostrasDesdeTentativa = 0;

  // CPU mais rapida so durante o envio: o TLS termina antes e o
  // radio (a parte que mais gasta) fica ligado por menos tempo.
  setCpuFrequencyMhz(160);

  bool ok = false;

  if (conectarWiFi()) {
    ok = publicarTudo();
  }

  desligarWiFi();
  setCpuFrequencyMhz(80);

  if (ok) {
    falhasSeguidas = 0;
  } else {
    if (falhasSeguidas < 10) falhasSeguidas++;
    Console.printf("[ENVIO] Falhou (%u seguidas). %u leituras guardadas.\n",
                   falhasSeguidas, nAmostras);
  }
}

// =====================================================
// PRIMEIRA INICIALIZACAO (ligou na tomada / RESET)
// =====================================================

void primeiraInicializacao() {
  delay(2000);  // tempo para abrir o Monitor Serial

  nAmostras = 0;
  amostrasDesdeTentativa = 0;
  falhasSeguidas = 0;
  perdidas = 0;
  ciclo = 0;
  wifiCacheOK = false;
  txReduzida = false;
  Console.limpar();

  Console.println("\n=== SMARTPREDICT: MPU6050 + DS18B20 + MIC + MQTT ===");
  Console.println("Modo economia: deep sleep entre leituras.");
  Console.printf("Leitura a cada %lu s, envio a cada %u leituras.\n",
                 (unsigned long)(INTERVALO_LEITURA_MS / 1000), AMOSTRAS_POR_ENVIO);
  Console.println("Para gravar codigo novo: segure BOOT, aperte RESET, solte BOOT.");

  // MPU6050
  Console.println("\n[Testando MPU6050...]");

  if (mpu.begin()) {
    mpu.setAccelerometerRange(MPU6050_RANGE_8_G);
    mpu.setGyroRange(MPU6050_RANGE_500_DEG);
    mpu.setFilterBandwidth(MPU6050_BAND_184_HZ);
    Wire.setClock(400000);
    mpuEscrever(MPU_PWR_MGMT_2, 0x07);
    mpuEscrever(MPU_PWR_MGMT_1, MPU_DORMINDO);
    Console.println("[OK] MPU6050 encontrado!");
  } else {
    Wire.setClock(400000);
    Console.println("[ERRO] MPU6050 nao encontrado.");
  }

  // DS18B20
  Console.println("\n[Testando DS18B20...]");

  tempSensor.begin();

  if (tempSensor.getDeviceCount() > 0) {
    tempSensor.setResolution(RESOLUCAO_DS18B20);
    Console.printf("[OK] DS18B20 encontrado! (%u bits)\n", RESOLUCAO_DS18B20);
  } else {
    Console.println("[ERRO] DS18B20 nao encontrado.");
  }

  // Microfone
  Console.println("\n[Configurando microfone...]");

  Amostra teste;
  memset(&teste, 0, sizeof(teste));

  if (lerMicrofone(teste, true)) {
    Console.println("[OK] Microfone respondendo!");
  } else {
    Console.println("[ERRO] Microfone sem sinal valido.");
  }

  Console.println("\nIniciando leituras...");
}

// =====================================================
// DORMIR ATE A PROXIMA LEITURA
// =====================================================

void dormir() {
  uint32_t acordado = millis();
  uint32_t espera = 100;

  if (acordado + 100 < INTERVALO_LEITURA_MS) {
    espera = INTERVALO_LEITURA_MS - acordado;
  }

  esp_sleep_enable_timer_wakeup((uint64_t)espera * 1000ULL);
  esp_deep_sleep_start();
}

// =====================================================
// SETUP (roda a cada vez que a ESP32 acorda)
// =====================================================

void setup() {
  setCpuFrequencyMhz(80);  // suficiente para ler os sensores (economiza bateria)

  Serial.begin(115200);

  Wire.begin(SDA_PIN, SCL_PIN);
  Wire.setClock(400000);   // I2C rapido: menos tempo acordada

  bool acordouDoSono = (esp_reset_reason() == ESP_RST_DEEPSLEEP);

  if (!acordouDoSono) {
    primeiraInicializacao();
  }

  ciclo++;
  coletarAmostra(!acordouDoSono);

  // Na primeira vez envia logo, para conferir que tudo funciona.
  if (!acordouDoSono || horaDeEnviar()) {
    enviarLote();
  }

  dormir();
}

// =====================================================
// LOOP
// =====================================================
// Nunca e executado: a ESP32 dorme no fim do setup() e,
// ao acordar, comeca de novo pelo setup().

void loop() {
}

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <inttypes.h>
#include <stdlib.h>
#include <math.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"

#include "esp_log.h"
#include "esp_system.h"
#include "esp_mac.h"
#include "esp_timer.h"
#include "nvs_flash.h"

#include "esp_bt.h"
#include "esp_bt_main.h"
#include "esp_gap_ble_api.h"

#include "esp_wifi.h"
#include "esp_netif.h"
#include "esp_event.h"
#include "esp_http_server.h"

#include "lwip/sockets.h"
#include "lwip/inet.h"
#include "lwip/netdb.h"

#include "mqtt_client.h"

/* ================= SAFE PRINTS ================= */
#define SAFE_PRINT(...)                 \
do {                                   \
    xSemaphoreTake(console_mutex, portMAX_DELAY); \
    printf(__VA_ARGS__);               \
    xSemaphoreGive(console_mutex);     \
} while(0)

#define SAFE_VPRINTF(fmt, args)         \
do {                                   \
    xSemaphoreTake(console_mutex, portMAX_DELAY); \
    vprintf(fmt, args);                \
    xSemaphoreGive(console_mutex);     \
} while(0)

/* ================= CONFIG ================= */
#define WIFI_SSID      "ZTE_2.4G_ehTuuE"
#define WIFI_PASSWORD  "jcmolina1961"
#define WINDOW_SIZE_MS 1000
#define MQTT_BROKER_URI "mqtt://192.168.1.5:1883"

#define SYNC_PORT 5000
#define SYNC_TIMEOUT_MS 5000

static const uint8_t TARGET_UUID[16] = {
    0xAB,0x90,0x78,0x56,0x34,0x12,0x34,0x12,
    0x34,0x12,0x34,0x12,0x78,0x56,0x34,0x12
};

/* ================= GLOBALS ================= */
static SemaphoreHandle_t console_mutex;
static bool process_scan_results = true;
static bool found_beacon = false;
static uint8_t beacon_mac[6];
static uint8_t esp32_mac[6];

#define RSSI_BUF_SIZE 128
static int rssi_buf[RSSI_BUF_SIZE];
static int rssi_buf_count = 0;

static int rssi_sum = 0;
static int last_avg_rssi = -999;
static int last_median_rssi = -999;
static float last_std_rssi = 0.0f;

static float ema_rssi = 0;
static float last_ema_rssi = 0;
static int delta_rssi = 0;
#define EMA_ALPHA 0.4f

static EventGroupHandle_t wifi_event_group;
#define WIFI_CONNECTED_BIT BIT0

static esp_mqtt_client_handle_t mqtt_client;
static bool mqtt_connected = false;

static int64_t window_counter = 1;
static int64_t last_sync_time = 0;
static bool maestro_activo = false;

static int64_t last_published_window = 0;

static int udp_socket = -1;
static TaskHandle_t sync_task_handle = NULL;

/* ================= UI ================= */
static int line_count = 0;
static bool display_initialized = false;

static void clear_screen(void) {
    SAFE_PRINT("\033[2J\033[H");
    line_count = 0;
}

static void print_line(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    SAFE_PRINT("\033[2K\r");
    SAFE_VPRINTF(fmt, args);
    SAFE_PRINT("\n");
    va_end(args);
    line_count++;
    fflush(stdout);
}

static void draw_layout(void) {
    clear_screen();
    SAFE_PRINT("==============================================\n");
    SAFE_PRINT("         ESP32 BLE ANCHOR (SLAVE UDP)\n");
    SAFE_PRINT("==============================================\n");
    line_count += 3;

    print_line(" ESP32 MAC : --:--:--:--:--:--");
    print_line(" BEACON MAC: --:--:--:--:--:--");
    print_line(" RSSI AVG  : --- dBm");
    print_line(" RSSI MED  : --- dBm");
    print_line(" RSSI DELTA: --- dB");
    print_line(" IP        : ...");
    print_line(" MQTT      : Disconnected");
    print_line(" WINDOW    : 0");
    print_line(" MAESTRO   : ...");
    line_count++;
    display_initialized = true;
}

static void update_display_line(int line, const char *fmt, ...) {
    if (!display_initialized) return;
    int target = 3 + line;
    SAFE_PRINT("\033[%d;0H\033[2K\r", target);

    va_list args;
    va_start(args, fmt);
    SAFE_VPRINTF(fmt, args);
    va_end(args);

    SAFE_PRINT("\033[%d;0H", line_count);
    fflush(stdout);
}

static void refresh_display(void) {
    static bool mac_shown = false;
    if (!mac_shown) {
        update_display_line(0, " ESP32 MAC : %02X:%02X:%02X:%02X:%02X:%02X",
                            esp32_mac[0], esp32_mac[1], esp32_mac[2],
                            esp32_mac[3], esp32_mac[4], esp32_mac[5]);
        mac_shown = true;
    }

    update_display_line(6, " MQTT      : %s", mqtt_connected ? "Connected" : "Disconnected");
    update_display_line(7, " WINDOW    : %lld", window_counter);
    update_display_line(8, " MAESTRO   : %s", maestro_activo ? "OK" : "TIMEOUT");

    if (!process_scan_results) {
        update_display_line(1, " BEACON MAC: --:--:--:--:--:--");
        update_display_line(2, " RSSI AVG  : PAUSED");
        update_display_line(3, " RSSI MED  : --- dBm");
        update_display_line(4, " RSSI DELTA: --- dB");
        return;
    }

    if (found_beacon) {
        update_display_line(1, " BEACON MAC: %02X:%02X:%02X:%02X:%02X:%02X",
                            beacon_mac[0], beacon_mac[1], beacon_mac[2],
                            beacon_mac[3], beacon_mac[4], beacon_mac[5]);
        update_display_line(2, " RSSI AVG  : %d dBm", last_avg_rssi);
        update_display_line(3, " RSSI MED  : %d dBm", last_median_rssi);
        update_display_line(4, " RSSI DELTA: %d dB", delta_rssi);
    } else {
        update_display_line(1, " BEACON MAC: --:--:--:--:--:--");
        update_display_line(2, " RSSI AVG  : --- dBm");
        update_display_line(3, " RSSI MED  : --- dBm");
        update_display_line(4, " RSSI DELTA: --- dB");
    }
}

/* ================= HELPERS ================= */
static int cmp_int(const void *a, const void *b) {
    return (*(int*)a - *(int*)b);
}

static int compute_median(int *data, int n) {
    qsort(data, n, sizeof(int), cmp_int);
    if (n % 2 == 0)
        return (data[n/2 - 1] + data[n/2]) / 2;
    else
        return data[n/2];
}

/* ================= BLE ================= */
static void gap_event_handler(esp_gap_ble_cb_event_t event,
                              esp_ble_gap_cb_param_t *param) {

    if (!process_scan_results) return;
    if (event != ESP_GAP_BLE_SCAN_RESULT_EVT) return;
    if (param->scan_rst.search_evt != ESP_GAP_SEARCH_INQ_RES_EVT) return;

    uint8_t *adv = param->scan_rst.ble_adv;
    uint8_t len = param->scan_rst.adv_data_len;
    uint8_t pos = 0;
    bool uuid_found = false;

    while (pos < len) {
        uint8_t l = adv[pos];
        if (l == 0 || pos + l >= len) break;
        if (l >= 17 && memcmp(&adv[pos + 2], TARGET_UUID, 16) == 0) {
            uuid_found = true;
            break;
        }
        pos += l + 1;
    }

    if (!uuid_found) return;

    memcpy(beacon_mac, param->scan_rst.bda, 6);
    found_beacon = true;

    int rssi = param->scan_rst.rssi;

    if (rssi_buf_count < RSSI_BUF_SIZE) {
        rssi_buf[rssi_buf_count++] = rssi;
    }

    rssi_sum += rssi;
}

/* ================= WIFI ================= */
static void wifi_event_handler(void* arg, esp_event_base_t base,
                               int32_t id, void* data) {
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) esp_wifi_connect();
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) esp_wifi_connect();
    if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        xEventGroupSetBits(wifi_event_group, WIFI_CONNECTED_BIT);
        ip_event_got_ip_t* event = (ip_event_got_ip_t*) data;
        update_display_line(5, " IP        : " IPSTR, IP2STR(&event->ip_info.ip));
    }
}

/* ================= HTTP ================= */
static esp_err_t http_get_handler(httpd_req_t *req) {
    char buf[64];
    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK) {
        char val[8];
        if (httpd_query_key_value(buf, "scan", val, sizeof(val)) == ESP_OK) {
            if (!strcmp(val, "on")) {
                process_scan_results = true;
                SAFE_PRINT("▶️ Escaneo REANUDADO\n");
            }
            if (!strcmp(val, "off")) {
                process_scan_results = false;
                SAFE_PRINT("⏸️ Escaneo PAUSADO\n");
            }
            refresh_display();
        }
    }

    char resp[512];
    snprintf(resp, sizeof(resp),
             "{\"scanning\":%s,\"rssi_avg\":%d,\"rssi_med\":%d,\"rssi_delta\":%d,\"window\":%lld,\"mqtt\":%s,\"maestro\":%s}",
             process_scan_results ? "true" : "false",
             last_avg_rssi, last_median_rssi, delta_rssi,
             window_counter,
             mqtt_connected ? "true" : "false",
             maestro_activo ? "activo" : "timeout");

    httpd_resp_send(req, resp, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static httpd_uri_t uri = {
    .uri = "/",
    .method = HTTP_GET,
    .handler = http_get_handler
};

static void http_server_task(void* pvParameters) {
    httpd_config_t http_cfg = HTTPD_DEFAULT_CONFIG();
    httpd_handle_t server = NULL;
    if (httpd_start(&server, &http_cfg) == ESP_OK) {
        httpd_register_uri_handler(server, &uri);
    }
    vTaskDelete(NULL);
}

/* ================= MQTT ================= */
static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data) {
    esp_mqtt_event_handle_t event = event_data;
    
    switch (event->event_id) {
        case MQTT_EVENT_CONNECTED:
            mqtt_connected = true;
            refresh_display();
            break;
        case MQTT_EVENT_DISCONNECTED:
            mqtt_connected = false;
            refresh_display();
            break;
        case MQTT_EVENT_PUBLISHED:
            break;
        case MQTT_EVENT_ERROR:
            SAFE_PRINT("MQTT Error\n");
            mqtt_connected = false;
            refresh_display();
            break;
        default:
            break;
    }
}

/* ================= SINCRONIZACIÓN UDP ================= */
static void sync_task(void *pvParameters) {
    SAFE_PRINT("📡 Iniciando sincronización UDP como ESCLAVO...\n");
    
    xEventGroupWaitBits(wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
    
    udp_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (udp_socket < 0) {
        SAFE_PRINT("❌ Error creando socket UDP\n");
        vTaskDelete(NULL);
        return;
    }

    int broadcast_enable = 1;
    setsockopt(udp_socket, SOL_SOCKET, SO_BROADCAST, &broadcast_enable, sizeof(broadcast_enable));

    struct sockaddr_in local_addr;
    local_addr.sin_family = AF_INET;
    local_addr.sin_port = htons(SYNC_PORT);
    local_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(udp_socket, (struct sockaddr *)&local_addr, sizeof(local_addr)) < 0) {
        SAFE_PRINT("❌ Error en bind UDP\n");
        close(udp_socket);
        vTaskDelete(NULL);
        return;
    }

    struct timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 100000;
    setsockopt(udp_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    char buffer[32];
    struct sockaddr_in sender_addr;
    socklen_t sender_len = sizeof(sender_addr);

    last_sync_time = esp_timer_get_time() / 1000;
    maestro_activo = false;

    while (1) {
        memset(buffer, 0, sizeof(buffer));
        int received = recvfrom(udp_socket, buffer, sizeof(buffer) - 1, 0,
                               (struct sockaddr *)&sender_addr, &sender_len);

        if (received > 0) {
            buffer[received] = '\0';
            if (strncmp(buffer, "SYNC:", 5) == 0) {
                int64_t sync_window = atoll(buffer + 5);
                window_counter = sync_window;
                last_sync_time = esp_timer_get_time() / 1000;
                maestro_activo = true;
                refresh_display();
            }
        }

        int64_t now = esp_timer_get_time() / 1000;
        if (maestro_activo && (now - last_sync_time > SYNC_TIMEOUT_MS)) {
            maestro_activo = false;
            refresh_display();
        }

        vTaskDelay(pdMS_TO_TICKS(100));
    }
}

/* ================= MAIN ================= */
void app_main(void) {
    nvs_flash_init();
    esp_log_level_set("*", ESP_LOG_ERROR);
    esp_read_mac(esp32_mac, ESP_MAC_WIFI_STA);

    console_mutex = xSemaphoreCreateMutex();
    draw_layout();

    wifi_event_group = xEventGroupCreate();
    esp_netif_init();
    esp_event_loop_create_default();

    esp_netif_t *sta_netif = esp_netif_create_default_wifi_sta();

    ESP_ERROR_CHECK(esp_netif_dhcpc_stop(sta_netif));

    esp_netif_ip_info_t ip_info = {0};
    ip_info.ip.addr = ipaddr_addr("192.168.1.43");
    ip_info.gw.addr = ipaddr_addr("192.168.1.1");
    ip_info.netmask.addr = ipaddr_addr("255.255.255.0");
    ESP_ERROR_CHECK(esp_netif_set_ip_info(sta_netif, &ip_info));

    esp_netif_dns_info_t dns = {0};
    dns.ip.u_addr.ip4.addr = ipaddr_addr("8.8.8.8");
    dns.ip.type = IPADDR_TYPE_V4;
    ESP_ERROR_CHECK(esp_netif_set_dns_info(sta_netif, ESP_NETIF_DNS_MAIN, &dns));

    wifi_init_config_t cfg_wifi = WIFI_INIT_CONFIG_DEFAULT();
    esp_wifi_init(&cfg_wifi);

    esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID,
                                       wifi_event_handler, NULL, NULL);
    esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP,
                                       wifi_event_handler, NULL, NULL);

    wifi_config_t sta_cfg = {
        .sta = {
            .ssid = WIFI_SSID,
            .password = WIFI_PASSWORD
        }
    };

    esp_wifi_set_mode(WIFI_MODE_STA);
    esp_wifi_set_config(WIFI_IF_STA, &sta_cfg);
    esp_wifi_start();

    xEventGroupWaitBits(wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);

    xTaskCreate(sync_task, "sync_task", 4096, NULL, 5, &sync_task_handle);

    esp_mqtt_client_config_t mqtt_cfg = {
        .broker = {
            .address = {
                .uri = MQTT_BROKER_URI,
            }
        }
    };

    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);

    esp_bt_controller_config_t bt_cfg = BT_CONTROLLER_INIT_CONFIG_DEFAULT();
    bt_cfg.mode = ESP_BT_MODE_BLE;
    esp_bt_controller_init(&bt_cfg);
    esp_bt_controller_enable(ESP_BT_MODE_BLE);
    esp_bluedroid_init();
    esp_bluedroid_enable();
    esp_ble_gap_register_callback(gap_event_handler);

    esp_ble_scan_params_t scan_params = {
        .scan_type = BLE_SCAN_TYPE_ACTIVE,
        .own_addr_type = BLE_ADDR_TYPE_PUBLIC,
        .scan_filter_policy = BLE_SCAN_FILTER_ALLOW_ALL,
        .scan_interval = 0x30,
        .scan_window = 0x20,
        .scan_duplicate = BLE_SCAN_DUPLICATE_DISABLE
    };

    esp_ble_gap_set_scan_params(&scan_params);
    esp_ble_gap_start_scanning(0);

    xTaskCreate(http_server_task, "http_server_task", 4096, NULL, 5, NULL);

    int64_t last_window_time = esp_timer_get_time() / 1000;

    while (1) {
        int64_t now = esp_timer_get_time() / 1000;

        if (now - last_window_time >= WINDOW_SIZE_MS) {
            if (rssi_buf_count > 0) {

                int avg = rssi_sum / rssi_buf_count;
                last_avg_rssi = avg;

                int temp[RSSI_BUF_SIZE];
                memcpy(temp, rssi_buf, rssi_buf_count * sizeof(int));
                last_median_rssi = compute_median(temp, rssi_buf_count);

                float variance = 0;
                for (int i = 0; i < rssi_buf_count; i++) {
                    float diff = rssi_buf[i] - avg;
                    variance += diff * diff;
                }
                variance /= rssi_buf_count;
                last_std_rssi = sqrtf(variance);

                if (ema_rssi == 0)
                    ema_rssi = avg;
                else
                    ema_rssi = EMA_ALPHA * avg + (1 - EMA_ALPHA) * ema_rssi;

                delta_rssi = (int)(ema_rssi - last_ema_rssi);
                last_ema_rssi = ema_rssi;

                if (window_counter > last_published_window) {
                    last_published_window = window_counter;

                    if (mqtt_client != NULL && mqtt_connected) {
                        char payload[128];
                        char topic[32];

                        snprintf(topic, sizeof(topic), "ips/anchor/C");

                        snprintf(payload, sizeof(payload),
                                 "{\"window_id\":%lld,\"avg\":%d,\"median\":%d,\"delta\":%d,\"std\":%.2f}",
                                 window_counter,
                                 last_avg_rssi,
                                 last_median_rssi,
                                 delta_rssi,
                                 last_std_rssi);

                        esp_mqtt_client_publish(mqtt_client, topic, payload, 0, 1, 0);
                    }
                }

                refresh_display();
            }

            rssi_sum = 0;
            rssi_buf_count = 0;
            found_beacon = false;

            last_window_time += WINDOW_SIZE_MS;
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

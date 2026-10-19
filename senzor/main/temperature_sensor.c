/*
 * SPDX-FileCopyrightText: 2024-2026 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: LicenseRef-Included
 *
 * Zigbee HA_temperature_sensor Example
 *
 * This example code is in the Public Domain (or CC0 licensed, at your option.)
 *
 * Unless required by applicable law or agreed to in writing, this
 * software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

#include "esp_check.h"
#include "esp_err.h"
#include "esp_log.h"
#include "nvs_flash.h"

#include "imu_sensor_driver.h"
#include "switch_driver.h"
#include "alarm_timer.h"

#include "esp_zigbee.h"
#include "ezbee/zha.h"

#include "temperature_sensor.h"

#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"
#include <string.h>
#include "zb_stream_proto.h"
#include "ezbee/zcl/cluster/custom.h"

static const char *TAG = "TEMPERATURE_SENSOR";

#define ESP_TEMP_SENSOR_MIN_VALUE       (-10) /* Sensor min measured value (degree Celsius) */
#define ESP_TEMP_SENSOR_MAX_VALUE       (80)  /* Sensor max measured value (degree Celsius) */
#define ESP_TEMP_SENSOR_UPDATE_INTERVAL (0.1f)   /* Sensor update interval (seconds) */

#define STREAM_TX_INTERVAL_MS  500          /* target time between packets */
#define SAMPLE_RATE_HZ_DEFAULT 100.0f        /* adjustable sampling frequency */

static int16_t zb_temperature_to_s16(float temp) { return (int16_t)(temp * 100); }

typedef struct { int64_t ts; uint32_t seq; uint32_t period_us; int16_t val; } sample_t;

static QueueHandle_t   s_sample_q;
static volatile uint32_t s_period_us;
static volatile uint32_t s_dropped;

/* Runs in the esp_timer task: must never block. */
static void sample_cb(float temperature, int64_t ts, uint32_t seq)
{
    sample_t s = { ts, seq, s_period_us, zb_temperature_to_s16(temperature) };
    if (xQueueSend(s_sample_q, &s, 0) != pdTRUE) {
        s_dropped++;
    }
}

static void stream_flush(stream_batch_t *b)
{
    if (b->count == 0) return;
    uint8_t buf[1 + sizeof(stream_batch_t)];
    size_t  len = STREAM_HDR_LEN + 2 * b->count;
    buf[0] = (uint8_t)len;                  /* ZCL octet string = length prefix + data */
    memcpy(&buf[1], b, len);

    ezb_zcl_report_attr_cmd_t cmd = {
        .cmd_ctrl = {
            .fc.direction       = EZB_ZCL_CMD_DIRECTION_TO_CLI,
            .dst_addr.addr_mode = EZB_ADDR_MODE_NONE,       /* uses the binding */
            .src_ep             = ESP_ZIGBEE_HA_TEMPERATURE_SENSOR_EP_ID,
            .cluster_id         = STREAM_CLUSTER_ID,
        },
        .payload = { .attr_id = STREAM_ATTR_ID },
    };
    esp_zigbee_lock_acquire(portMAX_DELAY);
    ezb_zcl_set_attr_value(ESP_ZIGBEE_HA_TEMPERATURE_SENSOR_EP_ID, STREAM_CLUSTER_ID, EZB_ZCL_CLUSTER_SERVER,
                           STREAM_ATTR_ID, EZB_ZCL_STD_MANUF_CODE, buf, false);
    ezb_err_t ret = ezb_zcl_report_attr_cmd_req(&cmd);
    esp_zigbee_lock_release();
    if (ret != EZB_ERR_NONE) ESP_LOGW(TAG, "stream report failed 0x%04x", ret);
    b->count = 0;
}

static uint8_t batch_for_rate(float hz)
{
    int n = (int)(hz * STREAM_TX_INTERVAL_MS / 1000.0f);
    return n < 1 ? 1 : (n > STREAM_BATCH_MAX ? STREAM_BATCH_MAX : n);
}

static void stream_tx_task(void *arg)
{
    stream_batch_t b = { .ver = STREAM_PROTO_VER };
    sample_t s;
    for (;;) {
        if (xQueueReceive(s_sample_q, &s, pdMS_TO_TICKS(2 * STREAM_TX_INTERVAL_MS)) != pdTRUE) {
            stream_flush(&b);               /* timeout: don't leave a partial batch sitting */
            continue;
        }
        bool gap    = b.count && (s.seq != b.first_seq + b.count);
        bool period = b.count && (s.period_us != b.period_us);
        if (gap || period) stream_flush(&b);        /* never mix timing in one batch */
        if (b.count == 0) {
            b.first_seq = s.seq; b.first_ts_us = (uint64_t)s.ts; b.period_us = s.period_us;
        }
        b.val[b.count++] = s.val;
        if (b.count >= batch_for_rate(1e6f / s.period_us)) stream_flush(&b);
    }
}


static void button_event_handler(switch_driver_handle_t handle)
{
    ESP_RETURN_ON_FALSE(handle != SWITCH_INV_HANDLE, , TAG, "Invalid switch handle");

    ezb_err_t ret = EZB_ERR_NONE;
    ezb_zcl_report_attr_cmd_t report_attr_cmd = {
        .cmd_ctrl =
            {
                .fc.direction       = EZB_ZCL_CMD_DIRECTION_TO_CLI,
                .dst_addr.addr_mode = EZB_ADDR_MODE_NONE,
                .src_ep             = ESP_ZIGBEE_HA_TEMPERATURE_SENSOR_EP_ID,
                .cluster_id         = EZB_ZCL_CLUSTER_ID_TEMPERATURE_MEASUREMENT,
            },
        .payload =
            {
                .attr_id = EZB_ZCL_ATTR_TEMPERATURE_MEASUREMENT_MEASURED_VALUE_ID,
            },
    };
    esp_zigbee_lock_acquire(portMAX_DELAY);
    ret = ezb_zcl_report_attr_cmd_req(&report_attr_cmd);
    esp_zigbee_lock_release();
    if (ret == EZB_ERR_NONE) {
        ESP_LOGI(TAG, "Sent ZCL Report Attribute request");
    } else {
        ESP_LOGE(TAG, "Failed to send ZCL Report Attribute request with error(0x%04x)", ret);
    }
}

static esp_err_t deferred_driver_init(void)
{
    static bool is_inited = false;

    ESP_RETURN_ON_FALSE(!is_inited, ESP_OK, TAG, "Deferred driver already initialized");

    temperature_sensor_config_t temp_sensor_config =
        TEMPERATURE_SENSOR_CONFIG_DEFAULT(ESP_TEMP_SENSOR_MIN_VALUE, ESP_TEMP_SENSOR_MAX_VALUE);

    s_sample_q  = xQueueCreate(256, sizeof(sample_t));
    ESP_RETURN_ON_FALSE(s_sample_q != NULL, ESP_ERR_NO_MEM, TAG, "Failed to create sample queue");
    s_period_us = (uint32_t)(1e6f / SAMPLE_RATE_HZ_DEFAULT);
    xTaskCreate(stream_tx_task, "stream_tx", 4096, NULL, 6, NULL);

    ESP_RETURN_ON_ERROR(temp_sensor_driver_init(&temp_sensor_config, 1.0f / SAMPLE_RATE_HZ_DEFAULT, sample_cb),
                        TAG, "Failed to initialize temperature sensor");

    switch_driver_config_t config = {
        .gpio_num = CONFIG_GPIO_BOOT_ON_DEVKIT,
        .event_cb = button_event_handler,
    };
    ESP_RETURN_ON_FALSE(switch_driver_init(&config) != SWITCH_INV_HANDLE, ESP_FAIL, TAG, "Failed to initialize switch driver");
    is_inited = true;

    return is_inited ? ESP_OK : ESP_FAIL;
}

static void esp_zigbee_alarm_bdb_commissioning(alarm_timer_arg_t arg)
{
    esp_zigbee_lock_acquire(portMAX_DELAY);
    (void)ezb_bdb_start_top_level_commissioning(arg);
    esp_zigbee_lock_release();
}

static bool esp_zigbee_app_signal_handler(const ezb_app_signal_t *app_signal)
{
    ezb_app_signal_type_t signal_type = ezb_app_signal_get_type(app_signal);

    switch (signal_type) {
    case EZB_ZDO_SIGNAL_SKIP_STARTUP:
        ESP_LOGI(TAG, "Initialize Zigbee stack");
        ezb_bdb_start_top_level_commissioning(EZB_BDB_MODE_INITIALIZATION);
        break;
    case EZB_BDB_SIGNAL_DEVICE_FIRST_START:
    case EZB_BDB_SIGNAL_DEVICE_REBOOT: {
        ezb_bdb_comm_status_t status = *((ezb_bdb_comm_status_t *)ezb_app_signal_get_params(app_signal));
        if (status == EZB_BDB_STATUS_SUCCESS) {
            ESP_LOGI(TAG, "Deferred driver initialization %s", deferred_driver_init() ? "failed" : "successful");
            ESP_LOGI(TAG, "Device started up in%s factory-reset mode", ezb_bdb_is_factory_new() ? "" : " non");
            if (ezb_bdb_is_factory_new()) {
                ezb_bdb_start_top_level_commissioning(EZB_BDB_MODE_NETWORK_STEERING);
            } else {
                ESP_LOGI(TAG, "Device reboot");
            }
        } else {
            ESP_LOGW(TAG, "%s failed with status(0x%02x), please retry", ezb_app_signal_to_string(signal_type), status);
            alarm_timer_schedule(esp_zigbee_alarm_bdb_commissioning, EZB_BDB_MODE_INITIALIZATION, 1000);
        }
    } break;
    case EZB_BDB_SIGNAL_STEERING: {
        ezb_bdb_comm_status_t status = *((ezb_bdb_comm_status_t *)ezb_app_signal_get_params(app_signal));
        if (status == EZB_BDB_STATUS_SUCCESS) {
            ezb_extpanid_t extended_pan_id;
            ezb_nwk_get_extended_panid(&extended_pan_id);
            ESP_LOGI(TAG, "Joined network successfully: PAN ID(0x%04hx, EXT: 0x%llx), Channel(%d), Short Address(0x%04hx)",
                     ezb_nwk_get_panid(), extended_pan_id.u64, ezb_nwk_get_current_channel(), ezb_nwk_get_short_address());
        } else {
            ESP_LOGW(TAG, "Failed to join network with status(0x%02x)", status);
            alarm_timer_schedule(esp_zigbee_alarm_bdb_commissioning, EZB_BDB_MODE_NETWORK_STEERING, 1000);
        }
    } break;
    case EZB_ZDO_SIGNAL_DEVICE_ANNCE: {
        const ezb_zdo_signal_device_annce_params_t *dev_annce_params = ezb_app_signal_get_params(app_signal);
        ESP_LOGI(TAG, "New device commissioned or rejoined (short: 0x%04hx)", dev_annce_params->short_addr);
    } break;
    case EZB_ZDO_SIGNAL_LEAVE: {
        const ezb_zdo_signal_leave_params_t *leave_params = ezb_app_signal_get_params(app_signal);
        ESP_LOGI(TAG, "Left network successfully with type(0x%02x)", leave_params->leave_type);
    } break;
    case EZB_NWK_SIGNAL_PERMIT_JOIN_STATUS: {
        uint8_t duration = *(uint8_t *)ezb_app_signal_get_params(app_signal);
        if (duration) {
            ESP_LOGI(TAG, "Network(0x%04hx) is open for %d seconds", ezb_nwk_get_panid(), duration);
        } else {
            ESP_LOGW(TAG, "Network(0x%04hx) closed, devices joining not allowed.", ezb_nwk_get_panid());
        }
    } break;
    default:
        ESP_LOGI(TAG, "Zigbee APP Signal: %s(type: 0x%02x)", ezb_app_signal_to_string(signal_type), signal_type);
        break;
    }
    return true;
}

static void esp_zigbee_zcl_core_action_handler(ezb_zcl_core_action_callback_id_t callback_id, void *message)
{
    switch (callback_id) {
    case EZB_ZCL_CORE_DEFAULT_RSP_CB_ID: {
        ezb_zcl_cmd_default_rsp_message_t *default_rsp = (ezb_zcl_cmd_default_rsp_message_t *)message;
        ESP_LOGI(TAG, "Received ZCL Default Response: status(0x%02x)", default_rsp->in.status_code);
    } break;
    default:
        ESP_LOGW(TAG, "ZCL Core Action: ID(0x%04lx)", callback_id);
        break;
    }
}

static void stream_cluster_init(uint8_t ep_id)   { (void)ep_id; }
static void stream_cluster_deinit(uint8_t ep_id) { (void)ep_id; }

esp_err_t esp_zigbee_create_zha_temperature_sensor_device(void)
{
    ezb_af_device_desc_t                dev_desc               = ezb_af_create_device_desc();
    ezb_zha_temperature_sensor_config_t temperature_sensor_cfg = EZB_ZHA_TEMPERATURE_SENSOR_CONFIG();
    temperature_sensor_cfg.temp_meas_cfg.min_measured_value    = zb_temperature_to_s16(ESP_TEMP_SENSOR_MIN_VALUE);
    temperature_sensor_cfg.temp_meas_cfg.max_measured_value    = zb_temperature_to_s16(ESP_TEMP_SENSOR_MAX_VALUE);

    ezb_af_ep_desc_t ep_desc =
        ezb_zha_create_temperature_sensor(ESP_ZIGBEE_HA_TEMPERATURE_SENSOR_EP_ID, &temperature_sensor_cfg);
    ezb_zcl_cluster_desc_t basic_desc = {0};

    basic_desc = ezb_af_endpoint_get_cluster_desc(ep_desc, EZB_ZCL_CLUSTER_ID_BASIC, EZB_ZCL_CLUSTER_SERVER);
    ezb_zcl_basic_cluster_desc_add_attr(basic_desc, EZB_ZCL_ATTR_BASIC_MANUFACTURER_NAME_ID, (void *)ESP_MANUFACTURER_NAME);
    ezb_zcl_basic_cluster_desc_add_attr(basic_desc, EZB_ZCL_ATTR_BASIC_MODEL_IDENTIFIER_ID, (void *)ESP_MODEL_IDENTIFIER);
        /* Custom stream cluster: octet-string attribute carrying one sample batch */
    static const ezb_zcl_custom_cluster_config_t stream_cfg = {
        .cluster_id  = STREAM_CLUSTER_ID,
        .init_func   = stream_cluster_init,
        .deinit_func = stream_cluster_deinit,
    };
    /* For octet strings the first byte is the length; initialise it to the maximum size */
    static uint8_t stream_init_val[1 + sizeof(stream_batch_t)] = {sizeof(stream_batch_t)};

    ezb_zcl_cluster_desc_t stream_desc = ezb_zcl_custom_create_cluster_desc(&stream_cfg, EZB_ZCL_CLUSTER_SERVER);
    ESP_ERROR_CHECK(ezb_zcl_custom_cluster_desc_add_attr(stream_desc, STREAM_ATTR_ID,
                                                         EZB_ZCL_ATTR_TYPE_OCTSTR,
                                                         EZB_ZCL_ATTR_ACCESS_READ_ONLY | EZB_ZCL_ATTR_ACCESS_REPORTING,
                                                         stream_init_val));
    ESP_ERROR_CHECK(ezb_af_endpoint_add_cluster_desc(ep_desc, stream_desc));

    ESP_ERROR_CHECK(ezb_af_device_add_endpoint_desc(dev_desc, ep_desc));
    ESP_ERROR_CHECK(ezb_af_device_desc_register(dev_desc));

    ezb_zcl_core_action_handler_register(esp_zigbee_zcl_core_action_handler);

    return ESP_OK;
}

esp_err_t esp_zigbee_setup_commissioning(void)
{
    ezb_aps_secur_enable_distributed_security(false);
    ESP_ERROR_CHECK(ezb_bdb_set_primary_channel_set(ESP_ZIGBEE_PRIMARY_CHANNEL_MASK));
    ESP_ERROR_CHECK(ezb_bdb_set_secondary_channel_set(ESP_ZIGBEE_SECONDARY_CHANNEL_MASK));
    ESP_ERROR_CHECK(ezb_app_signal_add_handler(esp_zigbee_app_signal_handler));

    return ESP_OK;
}

static void esp_zigbee_stack_main_task(void *pvParameters)
{
    esp_zigbee_config_t config = ESP_ZIGBEE_DEFAULT_CONFIG();

    ESP_ERROR_CHECK(esp_zigbee_init(&config));

    ESP_ERROR_CHECK(esp_zigbee_setup_commissioning());

    ESP_ERROR_CHECK(esp_zigbee_create_zha_temperature_sensor_device());

    ESP_ERROR_CHECK(esp_zigbee_start(false));

    esp_zigbee_launch_mainloop();

    esp_zigbee_deinit();

    vTaskDelete(NULL);
}

void app_main(void)
{
    ESP_ERROR_CHECK(nvs_flash_init());
    ESP_LOGI(TAG, "Start ESP Zigbee Stack");
    xTaskCreate(esp_zigbee_stack_main_task, "Zigbee_main", 4096, NULL, 5, NULL);
}

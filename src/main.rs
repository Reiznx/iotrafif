// Firmware ESP32 (esp-idf-svc) — publish data dummy suhu & kelembapan ke HiveMQ.
// Target: `cargo build --release` di project yang di-scaffold dengan `esp-idf-template`
// (https://github.com/esp-rs/esp-idf-template), board ESP32 apa saja.
//
// Sebelum flash, isi WIFI_SSID / WIFI_PASS / MQTT_* di bawah, atau lebih baik
// pindahkan ke `sdkconfig.defaults` / `cfg.toml` pakai `toml_cfg` supaya tidak
// ikut ter-commit ke git.

use embedded_svc::http::client::Client;
use embedded_svc::mqtt::client::{EventPayload, QoS};
use esp_idf_svc::eventloop::EspSystemEventLoop;
use esp_idf_svc::hal::peripherals::Peripherals;
use esp_idf_svc::http::client::{Configuration as HttpConfiguration, EspHttpConnection};
use esp_idf_svc::mqtt::client::{EspMqttClient, MqttClientConfiguration};
use esp_idf_svc::nvs::EspDefaultNvsPartition;
use esp_idf_svc::ota::EspOta;
use esp_idf_svc::wifi::{AuthMethod, BlockingWifi, ClientConfiguration, Configuration, EspWifi};
use log::{error, info, warn};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

// ---- Ganti sesuai jaringan & broker kamu ----
const WIFI_SSID: &str = "Arm Robot";
const WIFI_PASS: &str = "terserah";

const MQTT_HOST: &str = "6891b601aebc42e8b39bea05b4e34777.s1.eu.hivemq.cloud";
const MQTT_PORT: u16 = 8883;
const MQTT_USER: &str = "iotrafif";
const MQTT_PASS: &str = "16022006"; // rotate password lama sebelum dipakai lagi
const MQTT_TOPIC: &str = "sample_enose";
const DEVICE_ID: &str = "esp32-enose-01";
const FIRMWARE_VERSION: &str = env!("CARGO_PKG_VERSION");
const OTA_COMMAND_TOPIC: &str = "devices/esp32-enose-01/ota/command";
const OTA_STATUS_TOPIC: &str = "devices/esp32-enose-01/ota/status";
// ----------------------------------------------

#[derive(Debug, Deserialize)]
struct OtaCommand {
    command: String,
    device_id: String,
    version: String,
    url: String,
    size: usize,
    sha256: String,
}

fn main() -> anyhow::Result<()> {
    esp_idf_svc::sys::link_patches();
    esp_idf_svc::log::EspLogger::initialize_default();

    // Tandai slot OTA berjalan sebagai valid agar rollback tidak terpicu.
    match EspOta::new() {
        Ok(mut ota) => {
            if let Err(e) = ota.mark_running_slot_valid() {
                error!("OTA rollback state could not be confirmed: {e:?}");
            }
        }
        Err(e) => {
            warn!("OTA metadata unavailable (no OTA partition?): {e:?}");
        }
    }

    let peripherals = Peripherals::take()?;
    let sys_loop = EspSystemEventLoop::take()?;
    let nvs = EspDefaultNvsPartition::take()?;

    let mut wifi = BlockingWifi::wrap(
        EspWifi::new(peripherals.modem, sys_loop.clone(), Some(nvs))?,
        sys_loop,
    )?;

    // Retry WiFi connection up to 5 kali sebelum menyerah.
    connect_wifi_with_retry(&mut wifi, 5)?;
    let ip_info = wifi.wifi().sta_netif().get_ip_info()?;
    info!("WiFi tersambung, IP: {:?}", ip_info);

    let broker_url = format!("mqtts://{MQTT_USER}:{MQTT_PASS}@{MQTT_HOST}:{MQTT_PORT}");
    let mqtt_config = MqttClientConfiguration {
        client_id: Some(DEVICE_ID),
        crt_bundle_attach: Some(esp_idf_svc::sys::esp_crt_bundle_attach),
        ..Default::default()
    };

    let (ota_sender, ota_receiver) = mpsc::channel::<Vec<u8>>();
    let mqtt_connected = Arc::new(AtomicBool::new(false));
    let connected_cb = mqtt_connected.clone();

    let mut client = EspMqttClient::new_cb(&broker_url, &mqtt_config, move |event| {
        match event.payload() {
            EventPayload::Connected(_) => {
                info!("Terhubung ke broker HiveMQ");
                connected_cb.store(true, Ordering::SeqCst);
            }
            EventPayload::Received { topic: Some(topic), data, .. }
                if topic == OTA_COMMAND_TOPIC =>
            {
                if let Err(e) = ota_sender.send(data.to_vec()) {
                    error!("Gagal mengantrikan perintah OTA: {e:?}");
                }
            }
            EventPayload::Disconnected => {
                info!("Terputus dari broker MQTT");
                connected_cb.store(false, Ordering::SeqCst);
            }
            EventPayload::Error(e) => error!("Event MQTT error: {e:?}"),
            _ => {}
        }
    })?;

    // Tunggu koneksi MQTT terbentuk (TLS handshake ~2–3 detik).
    info!("Menunggu koneksi MQTT ke broker...");
    while !mqtt_connected.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(100));
    }

    client.subscribe(OTA_COMMAND_TOPIC, QoS::AtLeastOnce)?;
    info!("Berhasil subscribe ke topic '{OTA_COMMAND_TOPIC}'");
    info!("Mulai kirim data dummy ke topic '{MQTT_TOPIC}'...");

    // Simulasi pembacaan sensor tanpa hardware sungguhan, memakai gelombang
    // sinus supaya nilainya bergerak halus dan realistis untuk demo.
    let mut t: f32 = 0.0;
    let mut subscribed = true;
    loop {
        if !mqtt_connected.load(Ordering::SeqCst) {
            subscribed = false;
            warn!("MQTT terputus, menunggu koneksi kembali...");
            std::thread::sleep(Duration::from_millis(1000));
            continue;
        }

        if !subscribed {
            match client.subscribe(OTA_COMMAND_TOPIC, QoS::AtLeastOnce) {
                Ok(_) => {
                    info!("Subscribe ulang ke '{OTA_COMMAND_TOPIC}' berhasil");
                    subscribed = true;
                }
                Err(e) => {
                    error!("Gagal subscribe ulang: {e:?}");
                    std::thread::sleep(Duration::from_millis(1000));
                    continue;
                }
            }
        }

        // Tangani perintah OTA jika ada.
        if let Ok(command) = ota_receiver.try_recv() {
            let status = match handle_ota_command(&command) {
                Ok(()) => {
                    info!("OTA selesai; perangkat akan reboot");
                    "completed"
                }
                Err(e) => {
                    error!("OTA gagal: {e:#}");
                    "failed"
                }
            };
            let ota_payload = format!(
                r#"{{"device_id":"{DEVICE_ID}","status":"{status}","firmware_version":"{FIRMWARE_VERSION}"}}"#
            );
            let _ = client.publish(OTA_STATUS_TOPIC, QoS::AtLeastOnce, false, ota_payload.as_bytes());
            if status == "completed" {
                unsafe { esp_idf_svc::sys::esp_restart(); }
            }
        }

        let suhu = 27.0_f32 + 3.5 * t.sin();
        let kelembapan = 65.0_f32 + 10.0 * (t * 0.7).cos();
        let kondisi = if suhu > 30.0 { "Panas" } else { "Normal" };

        let payload = format!(
            r#"{{"device_id":"{DEVICE_ID}","firmware_version":"{FIRMWARE_VERSION}","suhu":{suhu:.2},"kelembapan":{kelembapan:.2},"status":"{kondisi}","network_ip":"{ip}","network_gateway":"{gw}","network_mask":"{mask}","wifi_connected":true}}"#,
            ip   = ip_info.ip,
            gw   = ip_info.subnet.gateway,
            mask = ip_info.subnet.mask,
        );

        match client.publish(MQTT_TOPIC, QoS::AtMostOnce, false, payload.as_bytes()) {
            Ok(_) => info!("Terkirim: {payload}"),
            Err(e) => error!("Gagal publish: {e:?}"),
        }

        t += 0.3;
        std::thread::sleep(Duration::from_secs(5));
    }
}

/// Sambungkan ke WiFi dengan retry otomatis.
fn connect_wifi_with_retry(
    wifi: &mut BlockingWifi<EspWifi<'static>>,
    max_retries: u32,
) -> anyhow::Result<()> {
    let config = Configuration::Client(ClientConfiguration {
        ssid: WIFI_SSID.try_into().unwrap(),
        password: WIFI_PASS.try_into().unwrap(),
        auth_method: AuthMethod::WPA2Personal,
        ..Default::default()
    });
    wifi.set_configuration(&config)?;
    wifi.start()?;

    for attempt in 1..=max_retries {
        info!("WiFi percobaan {attempt}/{max_retries}...");
        match wifi.connect() {
            Ok(_) => {}
            Err(e) => {
                warn!("WiFi connect gagal (attempt {attempt}): {e:?}");
                std::thread::sleep(Duration::from_secs(2));
                continue;
            }
        }
        match wifi.wait_netif_up() {
            Ok(_) => return Ok(()),
            Err(e) => {
                warn!("wait_netif_up gagal (attempt {attempt}): {e:?}");
                // Disconnect sebelum retry agar state bersih.
                let _ = wifi.disconnect();
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    }
    anyhow::bail!("WiFi gagal tersambung setelah {max_retries} percobaan")
}

fn handle_ota_command(payload: &[u8]) -> anyhow::Result<()> {
    let command: OtaCommand = serde_json::from_slice(payload)?;
    anyhow::ensure!(command.command == "ota_update", "unsupported OTA command");
    anyhow::ensure!(command.device_id == DEVICE_ID, "OTA command targets another device");
    anyhow::ensure!(command.url.starts_with("https://"), "OTA URL must use HTTPS");
    anyhow::ensure!(command.size > 0 && command.size <= 8 * 1024 * 1024, "invalid OTA size");
    anyhow::ensure!(command.sha256.len() == 64, "invalid SHA-256 digest");
    info!("Downloading firmware {} ({} bytes)", command.version, command.size);

    let http_config = HttpConfiguration {
        crt_bundle_attach: Some(esp_idf_svc::sys::esp_crt_bundle_attach),
        timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    };
    let connection = EspHttpConnection::new(&http_config)?;
    let mut http_client = Client::wrap(connection);
    let request = http_client.get(&command.url)?;
    let mut response = request.submit()?;
    anyhow::ensure!(
        response.status() == 200,
        "firmware server returned HTTP {}",
        response.status()
    );

    let mut ota = EspOta::new()?;
    let mut update = ota.initiate_update_with_known_size(command.size)?;
    let mut hasher = Sha256::new();
    // Buffer pada heap, bukan stack, agar tidak memakan stack frame besar.
    let mut buffer = vec![0u8; 4096];
    let mut received = 0usize;
    loop {
        let count = response.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        received = received.saturating_add(count);
        anyhow::ensure!(received <= command.size, "firmware exceeds declared size");
        hasher.update(&buffer[..count]);
        update.write(&buffer[..count])?;
    }
    anyhow::ensure!(received == command.size, "firmware size mismatch");
    let digest = format!("{:x}", hasher.finalize());
    anyhow::ensure!(
        digest.eq_ignore_ascii_case(&command.sha256),
        "firmware SHA-256 mismatch"
    );
    update.complete()?;
    Ok(())
}
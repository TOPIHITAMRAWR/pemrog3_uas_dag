# Panduan Menjalankan Proyek Pipeline Cuaca BMKG (Apache Airflow)

Dokumen ini memuat panduan lengkap untuk mempersiapkan environment, mengonfigurasi, dan menjalankan pipeline data prakiraan cuaca BMKG berbasis **Apache Airflow**.

---

## 1. Arsitektur Proyek

Proyek ini terdiri dari dua DAG yang saling terhubung:
1. **DAG 1: `bmkg_weather_etl`**
   - **Tujuan**: Mengambil data prakiraan cuaca resmi dari API publik BMKG untuk 31 kelurahan representatif (1 kelurahan per kecamatan) di Kota Surabaya.
   - **Alur Task**: `check_api` $\rightarrow$ `extract` (dinamis per lokasi via `.expand()`) $\rightarrow$ `validate_raw` $\rightarrow$ `transform` $\rightarrow$ `load` $\rightarrow$ `trigger_analytics`.
   - **Output**: File raw JSON di `data/raw/`, CSV di `data/processed/`, dan data tersimpan ke tabel `weather_forecast` pada `data/bmkg_weather.db`.

2. **DAG 2: `bmkg_weather_analytics`**
   - **Tujuan**: Menganalisis data prakiraan cuaca terkini secara otomatis setelah ETL selesai.
   - **Alur Task (Paralel)**:
     - `detect_extreme_weather`: Mendeteksi indikasi cuaca ekstrem (curah hujan $> 20.0$ mm, kecepatan angin $> 30.0$ km/jam, suhu $\ge 35.0$ Â°C) dan menyimpannya ke tabel `weather_alerts`.
     - `build_daily_summary`: Menghitung agregasi harian per wilayah (rata-rata suhu, kelembapan, curah hujan total, dan kecepatan angin) ke tabel `weather_daily_summary`.

---

## 2. Struktur Direktori

```text
proyek_airflow/
â”œâ”€â”€ config/
â”‚   â””â”€â”€ locations.json          # Daftar kode wilayah adm4 BMKG target
â”œâ”€â”€ dags/
â”‚   â”œâ”€â”€ bmkg_weather_etl.py     # DAG 1: Ekstraksi, Transformasi, dan Pemuatan Data
â”‚   â””â”€â”€ bmkg_weather_analytics.py # DAG 2: Analitik cuaca ekstrem & ringkasan harian
â”œâ”€â”€ data/
â”‚   â”œâ”€â”€ raw/                    # Arsip response mentah JSON per batch waktu
â”‚   â”œâ”€â”€ processed/              # File CSV hasil transformasi
â”‚   â””â”€â”€ bmkg_weather.db         # Database SQLite utama
â”œâ”€â”€ PANDUAN.md                  # Dokumentasi panduan instalasi dan eksekusi
â””â”€â”€ .gitignore
```

---

## 3. Prasyarat Sistem

- **Python**: Versi `3.10` atau `3.11` direkomendasikan.
- **Sistem Operasi**: Linux, macOS, atau Windows (melalui **WSL2** sangat disarankan untuk kestabilan scheduler Airflow).
- **SQLite3**: Terintegrasi langsung dengan Python.

---

## 4. Persiapan Lingkungan (Setup Environment)

### Langkah 1: Buat dan Aktifkan Virtual Environment

Buka terminal di root folder proyek `proyek_airflow`:

- **Linux / macOS / WSL (Rekomendasi)**:
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```

- **Windows PowerShell**:
  ```powershell
  python -m venv venv
  .\venv\Scripts\Activate.ps1
  ```

---

### Langkah 2: Install Dependensi

Install Apache Airflow beserta library pendukung yang dibutuhkan proyek:

```bash
# 1. Install library pendukung ETL & Analytics
pip install pandas requests pendulum

# 2. Install Apache Airflow (versi 2.8+ / 2.9+)
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

> **Alternatif instalasi cepat**:
> ```bash
> pip install "apache-airflow>=2.8.0" pandas requests pendulum
> ```

---

### Langkah 3: Konfigurasi Environment Variable `AIRFLOW_HOME`

Pastikan Airflow membaca DAG dan konfigurasi langsung dari folder proyek ini:

- **Linux / macOS / WSL**:
  ```bash
  export AIRFLOW_HOME="$(pwd)"
  ```

- **Windows PowerShell**:
  ```powershell
  $env:AIRFLOW_HOME = (Get-Location).Path
  ```

---

## 5. Inisialisasi Database Airflow

Jalankan inisialisasi metadata database Airflow:

```bash
airflow db migrate
```

Buat akun pengguna administrator untuk mengakses Web UI Airflow:

```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname BMKG \
    --role Admin \
    --email admin@example.com \
    --password admin
```

---

## 6. Menjalankan Apache Airflow

### Opsi A: Mode Standalone (Paling Praktis untuk Development)

Mode ini menjalankan scheduler, webserver, dan triggerer sekaligus dalam satu terminal:

```bash
airflow standalone
```

### Opsi B: Menjalankan Komponen Terpisah

Jika ingin memantau log scheduler dan webserver secara terpisah, buka dua terminal:

- **Terminal 1 (Scheduler & Triggerer)**:
  ```bash
  airflow scheduler
  ```
- **Terminal 2 (Webserver)**:
  ```bash
  airflow webserver --port 8080
  ```

Setelah berjalan, buka antarmuka Web UI di browser Anda:
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Username**: `admin`
- **Password**: `admin` (atau password yang Anda buat di Langkah 5)

---

## 7. Menjalankan Pipeline & Pengujian

### 1. Verifikasi Pendeteksian DAG

Pastikan kedua DAG terdaftar tanpa error sintaks:

```bash
airflow dags list
```

Pastikan output memuat:
- `bmkg_weather_etl`
- `bmkg_weather_analytics`

---

### 2. Jalankan Pipeline (Trigger DAG)

Jalankan DAG pertama (`bmkg_weather_etl`):

- **Melalui Web UI**:
  1. Cari DAG `bmkg_weather_etl`.
  2. Aktifkan toggle toggle DAG menjadi **Active** (warna biru).
  3. Klik tombol **Trigger DAG** (ikon tombol Play di sebelah kanan).

- **Melalui Terminal (CLI)**:
  ```bash
  airflow dags unpause bmkg_weather_etl
  airflow dags unpause bmkg_weather_analytics
  airflow dags trigger bmkg_weather_etl
  ```

---

### 3. Alur Eksekusi Otomatis

1. `bmkg_weather_etl` akan mengekstrak data 31 kelurahan (31 kecamatan) dari API BMKG secara paralel.
2. Melakukan validasi struktur JSON dan transformasi tanggal lokal ke format WIB.
3. Menyimpan hasil olahan ke tabel `weather_forecast`.
4. Task terakhir `trigger_analytics` akan memicu eksekusi `bmkg_weather_analytics` secara otomatis.
5. `bmkg_weather_analytics` mengeksekusi dua task paralel:
   - `detect_extreme_weather`
   - `build_daily_summary`

---

## 8. Verifikasi Data Hasil Eksekusi

Setelah pipeline selesai (status kedua DAG hijau/`success`), periksa hasil data pada database SQLite:

### Periksa Database via Terminal / Python

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/bmkg_weather.db')
cur = conn.cursor()

print('=== TOTAL DATA PRAKIRAAN (weather_forecast) ===')
print('Jumlah baris:', cur.execute('SELECT COUNT(*) FROM weather_forecast').fetchone()[0])

print('\n=== TOTAL PERINGATAN CUACA EKSTREM (weather_alerts) ===')
print('Jumlah peringatan:', cur.execute('SELECT COUNT(*) FROM weather_alerts').fetchone()[0])

print('\n=== CONTOH RINGKASAN HARIAN (weather_daily_summary) ===')
rows = cur.execute('SELECT location_code, forecast_date_local, avg_temp_c, max_temp_c, min_temp_c, avg_humidity_pct, total_precip_mm, avg_wind_speed FROM weather_daily_summary LIMIT 5').fetchall()
for r in rows:
    print(r)
"
```

### Tabel yang Terbentuk di `data/bmkg_weather.db`:

1. **`weather_forecast`**: Data mentah terstandarisasi per slot waktu prakiraan.
2. **`weather_alerts`**: Peringatan kondisi cuaca ekstrem per slot waktu (`PRIMARY KEY (location_code, forecast_utc, alert_type)`).
3. **`weather_daily_summary`**: Ringkasan rata-rata metrik cuaca harian (`PRIMARY KEY (location_code, forecast_date_local)`).

---

## 9. Pengujian Mandiri Task Tertentu (CLI Task Test)

Anda dapat menguji task tertentu secara terisolasi tanpa perlu menjalankan keseluruhan DAG run:

```bash
# Uji task check_api pada DAG ETL
airflow tasks test bmkg_weather_etl check_api 2026-11-22

# Uji task deteksi cuaca ekstrem pada DAG Analytics
airflow tasks test bmkg_weather_analytics detect_extreme_weather 2026-11-22

# Uji task agregasi harian pada DAG Analytics
airflow tasks test bmkg_weather_analytics build_daily_summary 2026-11-22
```

---

## 10. Catatan & Best Practices

1. **Idempotensi**:
   - Kedua DAG dirancang aman dijalankan berulang kali kapan saja tanpa risiko duplikasi data berkat penggunaan klausa `INSERT OR REPLACE` dengan Primary Key komposit.
2. **Penanganan Error / Offline Data**:
   - Jika koneksi internet terputus atau tabel database belum terbentuk, fungsi analytics akan menangani kondisi tersebut secara aman tanpa membuat worker crash.
3. **Pengaturan Penjadwalan (Scheduling)**:
   - Saat ini kedua DAG disetel `schedule=None` untuk mode manual/triggered testing.
   - Jika ingin dijadwalkan otomatis harian (misal jam 10:00 dan 22:00 WIB), ubah parameter `schedule` pada DAG 1 (`bmkg_weather_etl`) menjadi cron expression: `"0 10,22 * * *"`.


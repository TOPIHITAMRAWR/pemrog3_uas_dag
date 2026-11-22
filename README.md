# Pipeline ETL dan Analitik Prakiraan Cuaca BMKG (Apache Airflow)

Repositori ini berisi implementasi data pipeline berbasis **Apache Airflow** untuk mengekstrak, memvalidasi, mentransformasi, menyimpan, dan menganalisis data prakiraan cuaca resmi dari API Publik Badan Meteorologi, Klimatologi, dan Geofisika (BMKG) di seluruh kecamatan Kota Surabaya.

Proyek ini dikembangkan untuk memenuhi tugas akhir mata kuliah **Pemrograman 3** dengan fokus utama pada orkestrasi alur kerja ETL menggunakan `PythonOperator` dan TaskFlow API.

---

## Arsitektur Pipeline

Sistem terdiri dari dua Directed Acyclic Graph (DAG) yang terhubung secara otomatis melalui `TriggerDagRunOperator`:

### 1. DAG Utama: `bmkg_weather_etl`
Bertanggung jawab menjalankan proses Extract, Transform, dan Load (ETL) secara bertahap dan idempoten:
- **`check_api`**: Memeriksa ketersediaan layanan endpoint API BMKG sebelum proses ekstraksi dimulai.
- **`extract` (Dynamic Task Mapping)**: Mengekstrak data JSON prakiraan cuaca untuk 31 kelurahan representatif menggunakan `.expand()` dengan batas konkurensi (`max_active_tis_per_dag=3`) serta jeda permintaan untuk menghindari pembatasan trafik (*rate limit*).
- **`validate_raw`**: Memvalidasi kelengkapan struktur JSON hasil ekstraksi dan memastikan seluruh atribut wajib (`utc_datetime`, `local_datetime`, `t`, `hu`) tersedia.
- **`transform`**: Meratakan struktur JSON bersarang (*flattening*), mengonversi zona waktu UTC ke waktu lokal WIB (`Asia/Jakarta`), menstandarkan tipe data numerik, dan mengekspor hasil bersih ke format CSV.
- **`load`**: Menyimpan data hasil transformasi ke tabel `weather_forecast` pada database SQLite (`data/bmkg_weather.db`) menggunakan klausa `INSERT OR REPLACE` berbasis kunci komposit `(location_code, forecast_utc, analysis_utc)`.
- **`trigger_analytics`**: Memicu eksekusi DAG kedua secara otomatis setelah pemuatan data selesai.

### 2. DAG Analitik: `bmkg_weather_analytics`
Berjalan otomatis setelah proses ETL selesai dengan dua task paralel:
- **`detect_extreme_weather`**: Memindai data prakiraan terbaru untuk mendeteksi potensi cuaca ekstrem (curah hujan di atas 20.0 mm, kecepatan angin di atas 30.0 km/jam, atau suhu mencapai 35.0 derajat Celsius ke atas) dan menyimpannya ke tabel `weather_alerts`.
- **`build_daily_summary`**: Menghitung agregasi statistik cuaca harian per lokasi (suhu rata-rata, suhu maksimum, suhu minimum, rata-rata kelembapan, total curah hujan, dan rata-rata kecepatan angin) ke tabel `weather_daily_summary`.

---

## Struktur Direktori

```text
pemrog3_uas_dag/
├── config/
│   └── locations.json            # Konfigurasi 31 kode wilayah adm4 Kota Surabaya
├── dags/
│   ├── bmkg_weather_etl.py       # DAG 1: Pipeline utama ETL prakiraan cuaca
│   └── bmkg_weather_analytics.py # DAG 2: Deteksi cuaca ekstrem dan ringkasan harian
├── data/
│   ├── raw/                      # Arsip response mentah JSON per waktu eksekusi
│   ├── processed/              # Hasil transformasi data dalam format CSV
│   └── bmkg_weather.db           # Database SQLite utama
├── PANDUAN.md                    # Panduan teknis instalasi dan pengujian
└── README.md                     # Dokumentasi utama proyek
```

---

## Cakupan Wilayah Ekstraksi (31 Kecamatan Kota Surabaya)

Pipeline ini mencakup seluruh **31 kecamatan** di Kota Surabaya dengan mengambil satu kelurahan representatif pada masing-masing kecamatan berdasarkan kode wilayah administrasi Kemendagri tingkat IV (`adm4`):

| No | Kecamatan | Kelurahan Representatif | Kode Wilayah (`adm4`) |
| :- | :--- | :--- | :--- |
| 1 | Karang Pilang | Karang Pilang | `35.78.01.1001` |
| 2 | Wonocolo | Sidosermo | `35.78.02.1001` |
| 3 | Rungkut | Kalirungkut | `35.78.03.1001` |
| 4 | Wonokromo | Wonokromo | `35.78.04.1001` |
| 5 | Tegalsari | Tegalsari | `35.78.05.1001` |
| 6 | Sawahan | Petemon | `35.78.06.1001` |
| 7 | Genteng | Embong Kaliasin | `35.78.07.1001` |
| 8 | Gubeng | Gubeng | `35.78.08.1001` |
| 9 | Sukolilo | Keputih | `35.78.09.1001` |
| 10 | Tambaksari | Tambaksari | `35.78.10.1001` |
| 11 | Simokerto | Simokerto | `35.78.11.1001` |
| 12 | Pabean Cantian | Bongkaran | `35.78.12.1001` |
| 13 | Bubutan | Alun-alun Contong | `35.78.13.1001` |
| 14 | Tandes | Tandes | `35.78.14.1002` |
| 15 | Krembangan | Krembangan Selatan | `35.78.15.1001` |
| 16 | Semampir | Ampel | `35.78.16.1001` |
| 17 | Kenjeran | Tanah Kali Kedinding | `35.78.17.1001` |
| 18 | Lakarsantri | Bangkingan | `35.78.18.1001` |
| 19 | Benowo | Kandangan | `35.78.19.1001` |
| 20 | Wiyung | Wiyung | `35.78.20.1001` |
| 21 | Dukuh Pakis | Dukuh Pakis | `35.78.21.1001` |
| 22 | Gayungan | Gayungan | `35.78.22.1001` |
| 23 | Jambangan | Jambangan | `35.78.23.1001` |
| 24 | Tenggilis Mejoyo | Kutisari | `35.78.24.1001` |
| 25 | Gunung Anyar | Gunung Anyar | `35.78.25.1001` |
| 26 | Mulyorejo | Mulyorejo | `35.78.26.1001` |
| 27 | Sukomanunggal | Sukomanunggal | `35.78.27.1001` |
| 28 | Asem Rowo | Asem Rowo | `35.78.28.1001` |
| 29 | Bulak | Kedung Cowek | `35.78.29.1001` |
| 30 | Pakal | Pakal | `35.78.30.1001` |
| 31 | Sambikerep | Made | `35.78.31.1002` |

---

## Panduan Singkat Menjalankan Proyek

Langkah instalasi lengkap tersedia pada berkas [`PANDUAN.md`](PANDUAN.md). Secara ringkas, jalankan perintah berikut pada lingkungan Linux/WSL:

```bash
# 1. Aktifkan virtual environment dan instal dependensi
python3 -m venv venv
source venv/bin/activate
pip install "apache-airflow>=2.8.0" pandas requests pendulum

# 2. Arahkan AIRFLOW_HOME ke direktori proyek dan migrasi database
export AIRFLOW_HOME="$(pwd)"
airflow db migrate

# 3. Buat akun administrator dan jalankan layanan Airflow
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname BMKG \
    --role Admin \
    --email admin@example.com \
    --password admin

airflow standalone
```

Setelah layanan aktif, buka `http://localhost:8080`, aktifkan kedua DAG (`bmkg_weather_etl` dan `bmkg_weather_analytics`), lalu jalankan trigger pada `bmkg_weather_etl`.

---

## Sumber Data

Data prakiraan cuaca bersumber dari layanan data terbuka resmi BMKG melalui endpoint `https://api.bmkg.go.id/publik/prakiraan-cuaca`.

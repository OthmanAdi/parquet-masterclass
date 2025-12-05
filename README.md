# Parquet Masterclass - Big Data Profis

**Modul 3: Big-Data Analyst Experte**
**Morphos GmbH**

---

## Was ist das hier?

Eine 90-minuetige Live-Session zum Thema **Apache Parquet** - das Datenformat der Big-Data Profis.

Nach dieser Session weisst du:
- Warum Parquet 10-100x schneller ist als CSV
- Wie du CSV zu Parquet konvertierst
- Wie du partitionierte Daten erstellst
- Warum Netflix, Uber und alle grossen Firmen Parquet nutzen

---

## Setup (5 Minuten)

### Schritt 1: Repository klonen

```bash
git clone https://github.com/DEIN-USERNAME/parquet-masterclass.git
cd parquet-masterclass
```

### Schritt 2: Virtual Environment erstellen

```bash
python -m venv .venv
```

### Schritt 3: Virtual Environment aktivieren

**Windows:**
```bash
.venv\Scripts\activate
```

**Mac/Linux:**
```bash
source .venv/bin/activate
```

### Schritt 4: Dependencies installieren

```bash
pip install -r requirements.txt
```

### Schritt 5: Dataset herunterladen

1. Gehe zu: https://www.kaggle.com/datasets/yasserh/nyc-taxi-trip-duration
2. Klicke auf "Download"
3. Entpacke die ZIP-Datei
4. Kopiere `train.csv` in den `data/` Ordner
5. Benenne um zu `nyc_taxi.csv`

**Oder mit Kaggle CLI:**
```bash
kaggle datasets download -d yasserh/nyc-taxi-trip-duration
unzip nyc-taxi-trip-duration.zip -d data/
mv data/train.csv data/nyc_taxi.csv
```

### Schritt 6: Java pruefen

PySpark braucht Java 11 oder 17. Pruefe:

```bash
java -version
```

Falls nicht installiert: https://adoptium.net/temurin/releases/?version=17

---

## Ordnerstruktur

```
parquet-masterclass/
├── README.md                    <- Du bist hier
├── requirements.txt             <- Python Dependencies
├── data/
│   └── nyc_taxi.csv            <- Dataset (manuell hinzufuegen)
└── Tag8_Parquet_Live.py        <- Die komplette Live-Session
```

---

## Los geht's!

```bash
python Tag8_Parquet_Live.py
```

---

## Dataset Info

**NYC Taxi Trip Duration**
- Quelle: Kaggle
- Groesse: ~64 MB (1.4 Millionen Fahrten)
- Zeitraum: New York City Taxi Fahrten
- Spalten: pickup_datetime, dropoff_datetime, passenger_count, trip_duration, GPS-Koordinaten

---

## Probleme?

1. **Java nicht gefunden:** Installiere Java 17 von https://adoptium.net
2. **JAVA_HOME nicht gesetzt:** Das Script setzt es automatisch
3. **Dataset fehlt:** Lade es von Kaggle herunter (Link oben)

---

**Viel Erfolg!**

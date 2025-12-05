# ============================================================
#
#   PARQUET MASTERCLASS - LIVE SESSION
#
#   Modul 3: Big-Data Analyst Experte
#   Morphos GmbH - Weiterbildung KI und Data-Analyst
#
#   Dauer: 90 Minuten
#   Dataset: NYC Taxi Trip Duration (1.4 Mio Zeilen)
#
# ============================================================
#
#   ANLEITUNG FUER DEN DOZENTEN:
#
#   - Alles was mit "# SAGEN:" beginnt = das sagst du laut
#   - Alles was mit "# ZEIGEN:" beginnt = zeige auf den Code
#   - Alles was mit "# WARTEN:" beginnt = warte auf Reaktion
#   - Fuehre den Code BLOCK fuer BLOCK aus
#   - Lass die Studenten jeden Block selbst ausfuehren
#
# ============================================================

# ============================================================
#   TEIL 0: SETUP (5 Minuten)
# ============================================================

# SAGEN:
# "Guten Morgen! Heute lernen wir etwas, das euch zu echten
#  Big-Data Profis macht. Wir lernen PARQUET."
#
# "Parquet ist das Datenformat, das Netflix, Uber, Spotify
#  und fast jede grosse Tech-Firma verwendet."
#
# "Warum? Weil es CSV ZERSTOERT. 10x schneller, 10x kleiner."
#
# "Aber erstmal: Habt ihr alle das Repo geklont?"
#
# WARTEN: Alle bestaetigen

# SAGEN:
# "Gut. Dann aktiviert eure Virtual Environment und
#  fuehrt den ersten Block aus. Wir machen das zusammen."

import os
import sys
import time

# HADOOP FIX FOR WINDOWS (winutils.exe)
os.environ['HADOOP_HOME'] = r'C:\hadoop'

# Java 17 Pfad setzen (Windows)
# ZEIGEN: "Diese Zeile sagt Python wo Java ist.
#          Ohne Java funktioniert Spark nicht."
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'

# Python Pfade setzen
python_exec = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exec
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

# PySpark importieren
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, round as spark_round,
    year, month, dayofmonth, hour,
    lag, rank, dense_rank, row_number
)
from pyspark.sql.window import Window

# SAGEN:
# "Jetzt starten wir Spark. Das dauert ein paar Sekunden."

print("=" * 70)
print("   PARQUET MASTERCLASS")
print("   Das Geheimnis der Big-Data Profis")
print("=" * 70)

spark = SparkSession.builder \
    .appName("Parquet-Masterclass") \
    .config("spark.driver.memory", "4g") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\nSpark laeuft! Version:", spark.version)
print("-" * 70)

# WARTEN: Alle haben Spark gestartet

# ============================================================
#   TEIL 1: WAS IST PARQUET? (10 Minuten)
# ============================================================

# SAGEN:
# "Bevor wir Code schreiben, muessen wir verstehen
#  WARUM Parquet so verdammt schnell ist."
#
# "Stellt euch eine Excel-Tabelle vor mit 1 Million Zeilen
#  und 20 Spalten. Ihr wollt nur die Summe EINER Spalte."
#
# "Was macht CSV?"

print("\n" + "=" * 70)
print("   TEIL 1: WARUM IST PARQUET SCHNELLER?")
print("=" * 70)

print("""
SAGEN: "Schaut auf diese Darstellung."

CSV SPEICHERT ZEILE FUER ZEILE (Row Storage):
============================================

Zeile 1: | ID | Name   | Stadt   | Umsatz |
Zeile 2: | ID | Name   | Stadt   | Umsatz |
Zeile 3: | ID | Name   | Stadt   | Umsatz |
... 1 Million mal ...

Query: SELECT SUM(Umsatz) FROM tabelle

Problem: CSV muss JEDE Zeile lesen!
         Auch ID, Name, Stadt - obwohl wir nur Umsatz brauchen.
         = 4 Spalten x 1 Mio Zeilen = 4 Millionen Werte gelesen


PARQUET SPEICHERT SPALTE FUER SPALTE (Column Storage):
=====================================================

Spalte ID:     | 1 | 2 | 3 | 4 | 5 | ... |
Spalte Name:   | A | B | C | D | E | ... |
Spalte Stadt:  | X | Y | Z | W | V | ... |
Spalte Umsatz: |100|200|300|400|500| ... |

Query: SELECT SUM(Umsatz) FROM tabelle

Parquet liest NUR die Umsatz-Spalte!
= 1 Spalte x 1 Mio Zeilen = 1 Million Werte gelesen

ERGEBNIS: 4x weniger Daten = 4x schneller!
""")

# SAGEN:
# "Versteht ihr den Unterschied?"
#
# "CSV: Liest ALLES, auch was es nicht braucht."
# "Parquet: Liest NUR was es braucht."
#
# "Bei 20 Spalten und ihr braucht nur 2?
#  Parquet ist 10x schneller."
#
# "Das ist der EINZIGE Grund warum alle grossen Firmen
#  Parquet nutzen. Es ist einfach schneller."

# WARTEN: Fragen beantworten

print("""
ZUSAETZLICHE VORTEILE:
======================

1. KOMPRESSION
   - CSV: Keine (100 MB bleibt 100 MB)
   - Parquet: Automatisch komprimiert (100 MB wird 15 MB)

2. DATENTYPEN
   - CSV: Alles ist Text (Spark muss raten)
   - Parquet: Echte Typen gespeichert (int, double, date)

3. SCHEMA
   - CSV: Spalteninfos muessen geraten werden
   - Parquet: Schema ist in der Datei gespeichert

ZUSAMMENFASSUNG:
Parquet = Schneller + Kleiner + Sicherer
""")

input("\n>>> Druecke ENTER um fortzufahren...")

# ============================================================
#   TEIL 2: CSV LADEN MIT ZEITMESSUNG (10 Minuten)
# ============================================================

# SAGEN:
# "Jetzt wird es praktisch. Wir laden unser Dataset."
#
# "Das sind 1.4 Millionen NYC Taxi Fahrten."
# "64 MB CSV-Datei."
#
# "Und wir messen wie lange das dauert."

print("\n" + "=" * 70)
print("   TEIL 2: CSV LADEN - WIE LANGE DAUERT DAS?")
print("=" * 70)

csv_pfad = "./data/nyc_taxi.csv"

# ZEIGEN: "Hier messen wir die Zeit mit time.time()"
print("\nLade CSV-Datei...")
print(f"Pfad: {csv_pfad}")

# Zeit messen: START
start_zeit = time.time()

# CSV laden
df_csv = spark.read.csv(csv_pfad, header=True, inferSchema=True)

# WICHTIG: count() ist eine ACTION - erst dann wird wirklich geladen
anzahl_zeilen = df_csv.count()

# Zeit messen: ENDE
csv_ladezeit = time.time() - start_zeit

print(f"\n{'='*50}")
print(f"CSV GELADEN!")
print(f"{'='*50}")
print(f"Anzahl Zeilen:  {anzahl_zeilen:,}")
print(f"Ladezeit:       {csv_ladezeit:.2f} Sekunden")
print(f"{'='*50}")

# SAGEN:
# "Schaut euch die Ladezeit an. Merkt euch diese Zahl."
# "Wir werden sie gleich mit Parquet vergleichen."

# WARTEN: Alle haben die Zeit notiert

# ZEIGEN: Schema anzeigen
print("\nCSV Schema (von Spark geraten):")
df_csv.printSchema()

# SAGEN:
# "Schaut auf die Datentypen. Spark hat sie GERATEN."
# "inferSchema=True heisst: Spark schaut sich die Daten an
#  und versucht zu erraten ob es int, double, string ist."
#
# "Das kostet Zeit und kann Fehler verursachen."
# "Bei Parquet gibt es dieses Problem NICHT."

# Erste Zeilen zeigen
print("\nErste 5 Zeilen:")
df_csv.show(5)

# SAGEN:
# "Das sind also unsere Taxi-Daten."
# "pickup_datetime = wann die Fahrt startete"
# "trip_duration = wie lange in Sekunden"
# "passenger_count = wie viele Passagiere"
# "Die GPS Koordinaten zeigen wo in NYC"

input("\n>>> Druecke ENTER um fortzufahren...")

# ============================================================
#   TEIL 3: ALS PARQUET SPEICHERN (10 Minuten)
# ============================================================

# SAGEN:
# "Jetzt konvertieren wir diese CSV zu Parquet."
# "Und das Beste? Es ist EINE Zeile Code."

print("\n" + "=" * 70)
print("   TEIL 3: CSV ZU PARQUET KONVERTIEREN")
print("=" * 70)

parquet_pfad = "./data/nyc_taxi.parquet"

print("\nSpeichere als Parquet...")
print(f"Ziel: {parquet_pfad}")

# Zeit messen
start_zeit = time.time()

# ZEIGEN: "Das ist die magische Zeile"
# SAGEN: "write = speichern, mode overwrite = ueberschreiben falls existiert,
#         parquet = das Format"

df_csv.write.mode("overwrite").parquet(parquet_pfad)

speicher_zeit = time.time() - start_zeit

print(f"\n{'='*50}")
print(f"PARQUET GESPEICHERT!")
print(f"{'='*50}")
print(f"Speicherzeit: {speicher_zeit:.2f} Sekunden")
print(f"{'='*50}")

# SAGEN:
# "Das war's. Eine Zeile Code."
# "Eure 64 MB CSV ist jetzt ein Parquet."
# "Aber wie gross ist die Parquet-Datei?"

# Dateigroessen vergleichen
print("\n" + "-" * 50)
print("DATEIGROESSEN-VERGLEICH:")
print("-" * 50)

# CSV Groesse
csv_groesse = os.path.getsize(csv_pfad)
csv_mb = csv_groesse / (1024 * 1024)

# Parquet Groesse (Ordner mit mehreren Dateien)
parquet_groesse = 0
for root, dirs, files in os.walk(parquet_pfad):
    for file in files:
        file_path = os.path.join(root, file)
        parquet_groesse += os.path.getsize(file_path)
parquet_mb = parquet_groesse / (1024 * 1024)

# Berechnung
ersparnis_prozent = (1 - parquet_mb / csv_mb) * 100
faktor = csv_mb / parquet_mb

print(f"\nCSV Groesse:      {csv_mb:.1f} MB")
print(f"Parquet Groesse:  {parquet_mb:.1f} MB")
print(f"\nErsparnis:        {ersparnis_prozent:.0f}%")
print(f"Parquet ist       {faktor:.1f}x kleiner!")

# SAGEN:
# "Schaut euch das an!"
# "Von XX MB auf XX MB."
# "Das ist XX% weniger Speicherplatz."
#
# "Und das OHNE Qualitaetsverlust. Alle Daten sind noch da."
# "Parquet komprimiert automatisch."
#
# "Stellt euch vor: Statt 1 TB Daten auf der Festplatte
#  nur noch 200 GB. Das spart richtig Geld in der Cloud!"

# WARTEN: Reaktionen abwarten, ggf. Fragen

# Ordnerstruktur zeigen
print("\n" + "-" * 50)
print("PARQUET ORDNERSTRUKTUR:")
print("-" * 50)
print(f"\n{parquet_pfad}/")
for root, dirs, files in os.walk(parquet_pfad):
    for file in files:
        file_size = os.path.getsize(os.path.join(root, file))
        print(f"   {file} ({file_size / 1024:.0f} KB)")

# SAGEN:
# "Parquet speichert nicht EINE Datei, sondern einen ORDNER."
# "Darin sind mehrere Part-Dateien."
# "Warum? Damit Spark die Daten parallel verarbeiten kann."
# "Jeder Worker kann einen Part lesen."

input("\n>>> Druecke ENTER um fortzufahren...")

# ============================================================
#   TEIL 4: PARQUET LADEN - DER SPEED TEST (10 Minuten)
# ============================================================

# SAGEN:
# "Jetzt kommt der Moment der Wahrheit."
# "Wir laden die GLEICHEN Daten als Parquet."
# "Und messen wieder die Zeit."
#
# "Wer wettet dass Parquet schneller ist?"

print("\n" + "=" * 70)
print("   TEIL 4: PARQUET LADEN - SPEED TEST!")
print("=" * 70)

print("\nLade Parquet-Datei...")

# Zeit messen
start_zeit = time.time()

# ZEIGEN: "Fast identisch - nur .parquet statt .csv"
df_parquet = spark.read.parquet(parquet_pfad)

# Action erzwingen
anzahl_parquet = df_parquet.count()

# Zeit messen
parquet_ladezeit = time.time() - start_zeit

print(f"\n{'='*50}")
print(f"PARQUET GELADEN!")
print(f"{'='*50}")
print(f"Anzahl Zeilen:  {anzahl_parquet:,}")
print(f"Ladezeit:       {parquet_ladezeit:.2f} Sekunden")
print(f"{'='*50}")

# Vergleich
print("\n" + "=" * 50)
print("   DER GROSSE VERGLEICH")
print("=" * 50)
print(f"\nCSV Ladezeit:     {csv_ladezeit:.2f} Sekunden")
print(f"Parquet Ladezeit: {parquet_ladezeit:.2f} Sekunden")

speedup = csv_ladezeit / parquet_ladezeit
print(f"\n>>> PARQUET IST {speedup:.1f}x SCHNELLER! <<<")

# SAGEN:
# "BOOM! Seht ihr das?"
# "CSV: X Sekunden. Parquet: X Sekunden."
# "Das ist Xx schneller!"
#
# "Bei 1.4 Millionen Zeilen."
# "Stellt euch vor bei 100 Millionen Zeilen."
# "Oder bei 1 Milliarde."
#
# "Das ist der Unterschied zwischen 'ich warte 10 Minuten'
#  und 'ich hab das Ergebnis in 1 Minute'."

# WARTEN: Mind-Blown Moment geniessen

# Schema vergleichen
print("\n" + "-" * 50)
print("SCHEMA VERGLEICH:")
print("-" * 50)

print("\nParquet Schema (GESPEICHERT, nicht geraten!):")
df_parquet.printSchema()

# SAGEN:
# "Schaut auf die Datentypen."
# "Parquet hat das Schema GESPEICHERT."
# "Kein Raten mehr. Kein inferSchema."
# "Die Typen sind 100% korrekt."

input("\n>>> Druecke ENTER um fortzufahren...")

# ============================================================
#   TEIL 5: SQL QUERIES AUF PARQUET (15 Minuten)
# ============================================================

# SAGEN:
# "Jetzt zeigen wir euch, dass alles was ihr schon kennt
#  auch auf Parquet funktioniert."
#
# "Wir machen SQL Queries - genau wie gestern."
# "GROUP BY, Aggregationen, alles das Gleiche."

print("\n" + "=" * 70)
print("   TEIL 5: SQL QUERIES - WAS IHR SCHON KENNT!")
print("=" * 70)

# Als View registrieren
df_parquet.createOrReplaceTempView("taxi")

print("\nView 'taxi' erstellt. Jetzt koennen wir SQL nutzen!")

# ----- QUERY 1: Einfache Aggregation -----

# SAGEN:
# "Query 1: Wie viele Fahrten pro Passagieranzahl?"

print("\n" + "-" * 50)
print("QUERY 1: Fahrten pro Passagieranzahl")
print("-" * 50)

start_zeit = time.time()

query1 = spark.sql("""
    SELECT
        passenger_count AS passagiere,
        COUNT(*) AS anzahl_fahrten,
        ROUND(AVG(trip_duration) / 60, 1) AS durchschnitt_minuten
    FROM taxi
    WHERE passenger_count > 0
    GROUP BY passenger_count
    ORDER BY passenger_count
""")

query1.show()

q1_zeit = time.time() - start_zeit
print(f"Query Zeit: {q1_zeit:.2f} Sekunden")

# SAGEN:
# "Seht ihr? 1 Passagier ist am haeufigsten."
# "Durchschnittliche Fahrt ist X Minuten."
# "Die Query hat X Sekunden gedauert."
#
# "Das ist Standard SQL - nichts Neues."

# ----- QUERY 2: Datum-Funktionen -----

# SAGEN:
# "Query 2: Jetzt nutzen wir Datum-Funktionen."
# "YEAR, MONTH, HOUR - kennt ihr schon."

print("\n" + "-" * 50)
print("QUERY 2: Fahrten pro Monat")
print("-" * 50)

start_zeit = time.time()

query2 = spark.sql("""
    SELECT
        YEAR(pickup_datetime) AS jahr,
        MONTH(pickup_datetime) AS monat,
        COUNT(*) AS anzahl_fahrten,
        ROUND(SUM(trip_duration) / 3600, 0) AS gesamt_stunden
    FROM taxi
    GROUP BY YEAR(pickup_datetime), MONTH(pickup_datetime)
    ORDER BY jahr, monat
""")

query2.show()

q2_zeit = time.time() - start_zeit
print(f"Query Zeit: {q2_zeit:.2f} Sekunden")

# SAGEN:
# "YEAR() und MONTH() extrahieren Jahr und Monat."
# "trip_duration / 3600 konvertiert Sekunden zu Stunden."
# "Ihr seht: die meisten Fahrten waren im Monat X."

# ----- QUERY 3: Top Stunden -----

# SAGEN:
# "Query 3: Zu welcher Uhrzeit fahren die meisten Taxis?"

print("\n" + "-" * 50)
print("QUERY 3: Fahrten pro Stunde (Top 10)")
print("-" * 50)

start_zeit = time.time()

query3 = spark.sql("""
    SELECT
        HOUR(pickup_datetime) AS stunde,
        COUNT(*) AS anzahl_fahrten,
        ROUND(AVG(trip_duration) / 60, 1) AS avg_minuten
    FROM taxi
    GROUP BY HOUR(pickup_datetime)
    ORDER BY anzahl_fahrten DESC
    LIMIT 10
""")

query3.show()

q3_zeit = time.time() - start_zeit
print(f"Query Zeit: {q3_zeit:.2f} Sekunden")

# SAGEN:
# "HOUR() extrahiert die Stunde aus dem Datetime."
# "Die meisten Fahrten sind um X Uhr."
# "Macht Sinn - das ist Feierabend / Rushhour."

input("\n>>> Druecke ENTER um fortzufahren...")

# ============================================================
#   TEIL 6: WINDOW FUNCTIONS AUF PARQUET (15 Minuten)
# ============================================================

# SAGEN:
# "Jetzt kommen die Window Functions."
# "LAG, RANK - alles was ihr gestern gelernt habt."
# "Funktioniert genauso auf Parquet."

print("\n" + "=" * 70)
print("   TEIL 6: WINDOW FUNCTIONS - EUER WISSEN ANWENDEN!")
print("=" * 70)

# ----- QUERY 4: LAG - Vergleich mit Vormonat -----

# SAGEN:
# "Query 4: Wie hat sich die Anzahl Fahrten pro Monat entwickelt?"
# "Wir nutzen LAG um mit dem Vormonat zu vergleichen."
# "Das kennt ihr von gestern!"

print("\n" + "-" * 50)
print("QUERY 4: Monatliches Wachstum mit LAG()")
print("-" * 50)

start_zeit = time.time()

query4 = spark.sql("""
    WITH monatlich AS (
        SELECT
            YEAR(pickup_datetime) AS jahr,
            MONTH(pickup_datetime) AS monat,
            COUNT(*) AS fahrten
        FROM taxi
        GROUP BY YEAR(pickup_datetime), MONTH(pickup_datetime)
    )
    SELECT
        jahr,
        monat,
        fahrten,
        LAG(fahrten) OVER (ORDER BY jahr, monat) AS vormonat,
        fahrten - LAG(fahrten) OVER (ORDER BY jahr, monat) AS differenz,
        ROUND(
            (fahrten - LAG(fahrten) OVER (ORDER BY jahr, monat)) * 100.0
            / LAG(fahrten) OVER (ORDER BY jahr, monat), 1
        ) AS wachstum_pct
    FROM monatlich
    ORDER BY jahr, monat
""")

query4.show()

q4_zeit = time.time() - start_zeit
print(f"Query Zeit: {q4_zeit:.2f} Sekunden")

# SAGEN:
# "CTE mit WITH - kennt ihr."
# "LAG(fahrten) holt die Fahrten vom VORMONAT."
# "Differenz und Prozent - genau wie gestern."
#
# "Seht ihr? Im Monat X gab es XX% mehr Fahrten."

# ----- QUERY 5: RANK - Top Tage -----

# SAGEN:
# "Query 5: Welche Tage hatten die meisten Fahrten?"
# "Wir nutzen RANK um sie zu ranken."

print("\n" + "-" * 50)
print("QUERY 5: Top 10 Tage nach Fahrten (RANK)")
print("-" * 50)

start_zeit = time.time()

query5 = spark.sql("""
    WITH taeglich AS (
        SELECT
            DATE(pickup_datetime) AS tag,
            COUNT(*) AS fahrten,
            ROUND(AVG(trip_duration) / 60, 1) AS avg_minuten
        FROM taxi
        GROUP BY DATE(pickup_datetime)
    )
    SELECT
        tag,
        fahrten,
        avg_minuten,
        RANK() OVER (ORDER BY fahrten DESC) AS rang
    FROM taeglich
    ORDER BY rang
    LIMIT 10
""")

query5.show()

q5_zeit = time.time() - start_zeit
print(f"Query Zeit: {q5_zeit:.2f} Sekunden")

# SAGEN:
# "DATE() macht aus datetime nur das Datum."
# "RANK() sortiert nach Fahrten absteigend."
# "Der Tag mit den meisten Fahrten war der X."
#
# "Interessant: War das vielleicht ein Feiertag?
#  Silvester? Ein Grossevent?"

# ----- QUERY 6: PARTITION BY -----

# SAGEN:
# "Query 6: Jetzt kombinieren wir RANK mit PARTITION BY."
# "Wir ranken die Tage INNERHALB jedes Monats."

print("\n" + "-" * 50)
print("QUERY 6: Beste 3 Tage PRO MONAT (PARTITION BY)")
print("-" * 50)

start_zeit = time.time()

query6 = spark.sql("""
    WITH taeglich AS (
        SELECT
            MONTH(pickup_datetime) AS monat,
            DATE(pickup_datetime) AS tag,
            COUNT(*) AS fahrten
        FROM taxi
        GROUP BY MONTH(pickup_datetime), DATE(pickup_datetime)
    ),
    mit_rang AS (
        SELECT
            monat,
            tag,
            fahrten,
            RANK() OVER (PARTITION BY monat ORDER BY fahrten DESC) AS rang
        FROM taeglich
    )
    SELECT * FROM mit_rang
    WHERE rang <= 3
    ORDER BY monat, rang
""")

query6.show(20)

q6_zeit = time.time() - start_zeit
print(f"Query Zeit: {q6_zeit:.2f} Sekunden")

# SAGEN:
# "PARTITION BY monat bedeutet:"
# "Der Rang startet in JEDEM Monat neu bei 1."
#
# "Monat 1: Rang 1, 2, 3"
# "Monat 2: Rang 1, 2, 3 (neu gezaehlt!)"
#
# "Das ist extrem nuetzlich fuer Reports."
# "'Zeig mir die Top 3 Tage pro Monat'"

input("\n>>> Druecke ENTER um fortzufahren...")

# ============================================================
#   TEIL 7: PERFORMANCE BATTLE - CSV vs PARQUET (10 Minuten)
# ============================================================

# SAGEN:
# "Jetzt machen wir einen fairen Vergleich."
# "Die GLEICHE Query auf CSV und Parquet."
# "Welches Format gewinnt?"

print("\n" + "=" * 70)
print("   TEIL 7: PERFORMANCE BATTLE - CSV vs PARQUET!")
print("=" * 70)

# Views erstellen
df_csv.createOrReplaceTempView("taxi_csv")
df_parquet.createOrReplaceTempView("taxi_parquet")

# Die Test-Query
test_query = """
    SELECT
        MONTH(pickup_datetime) AS monat,
        COUNT(*) AS fahrten,
        ROUND(AVG(trip_duration) / 60, 1) AS avg_minuten,
        ROUND(AVG(passenger_count), 2) AS avg_passagiere
    FROM {tabelle}
    GROUP BY MONTH(pickup_datetime)
    ORDER BY monat
"""

print("\nTest-Query:")
print("-" * 50)
print(test_query.format(tabelle="[CSV oder PARQUET]"))
print("-" * 50)

# ----- TEST 1: CSV -----

# SAGEN:
# "Zuerst CSV. Los geht's!"

print("\n>>> TEST 1: Query auf CSV...")

start_zeit = time.time()

result_csv = spark.sql(test_query.format(tabelle="taxi_csv"))
result_csv.collect()  # Action erzwingen

csv_query_zeit = time.time() - start_zeit

print(f"CSV Query Zeit: {csv_query_zeit:.2f} Sekunden")

# ----- TEST 2: PARQUET -----

# SAGEN:
# "Jetzt Parquet. Die gleiche Query."

print("\n>>> TEST 2: Query auf PARQUET...")

start_zeit = time.time()

result_parquet = spark.sql(test_query.format(tabelle="taxi_parquet"))
result_parquet.collect()  # Action erzwingen

parquet_query_zeit = time.time() - start_zeit

print(f"Parquet Query Zeit: {parquet_query_zeit:.2f} Sekunden")

# ----- ERGEBNIS -----

print("\n" + "=" * 50)
print("   ERGEBNIS DES BATTLES:")
print("=" * 50)
print(f"\nCSV:     {csv_query_zeit:.2f} Sekunden")
print(f"Parquet: {parquet_query_zeit:.2f} Sekunden")

if parquet_query_zeit < csv_query_zeit:
    speedup = csv_query_zeit / parquet_query_zeit
    print(f"\n>>> PARQUET GEWINNT! {speedup:.1f}x SCHNELLER! <<<")
else:
    print("\n>>> Unentschieden (bei kleinen Daten normal)")

# SAGEN:
# "Seht ihr den Unterschied?"
# "Bei groesseren Daten waere der Unterschied noch krasser."
# "CSV: X Sekunden. Parquet: X Sekunden."
#
# "Stellt euch vor ihr habt 100 Queries am Tag."
# "Mit CSV: wartet ihr X Minuten."
# "Mit Parquet: wartet ihr nur X Minuten."
#
# "Zeit ist Geld. Parquet spart beides."

# Ergebnis zeigen
print("\nDas Ergebnis (identisch bei beiden):")
result_parquet.show()

input("\n>>> Druecke ENTER um fortzufahren...")

# ============================================================
#   TEIL 8: PARTITIONIERTES PARQUET (10 Minuten)
# ============================================================

# SAGEN:
# "Jetzt lernen wir den ULTIMATIVEN Trick."
# "Partitioniertes Parquet."
#
# "Stellt euch vor, ihr speichert Daten nach Monat getrennt."
# "Query fuer Januar? Spark liest NUR den Januar-Ordner."
# "Die anderen 11 Monate werden ignoriert."

print("\n" + "=" * 70)
print("   TEIL 8: PARTITIONIERTES PARQUET - DER ULTIMATIVE TRICK")
print("=" * 70)

print("""
WAS IST PARTITIONIERUNG?
========================

NORMAL (eine grosse Datei):
    taxi.parquet/
    └── part-00000.parquet (alles drin)

PARTITIONIERT (nach Monat aufgeteilt):
    taxi_partitioned.parquet/
    ├── monat=1/
    │   └── data.parquet (nur Januar)
    ├── monat=2/
    │   └── data.parquet (nur Februar)
    ├── monat=3/
    │   └── data.parquet (nur Maerz)
    ... usw ...

Query: WHERE monat = 3
Spark liest NUR den Ordner 'monat=3'!
= VIEL schneller!
""")

# SAGEN:
# "Versteht ihr das Prinzip?"
# "Die Daten werden physisch in Ordner aufgeteilt."
# "Spark ist schlau genug, nur den relevanten Ordner zu lesen."
# "Das nennt man PARTITION PRUNING."

# ZEIGEN: Partitioniert speichern

print("\n" + "-" * 50)
print("PARTITIONIERT SPEICHERN:")
print("-" * 50)

# Monat-Spalte hinzufuegen
df_mit_monat = df_parquet.withColumn(
    "monat",
    month(col("pickup_datetime"))
)

partitioned_pfad = "./data/taxi_partitioned.parquet"

print(f"\nSpeichere partitioniert nach Monat...")
print(f"Ziel: {partitioned_pfad}")

start_zeit = time.time()

# ZEIGEN: "partitionBy('monat') ist der Schluessel"
df_mit_monat.write \
    .mode("overwrite") \
    .partitionBy("monat") \
    .parquet(partitioned_pfad)

part_speicher_zeit = time.time() - start_zeit

print(f"Speicherzeit: {part_speicher_zeit:.2f} Sekunden")

# Ordnerstruktur zeigen
print("\nOrdnerstruktur:")
print(f"{partitioned_pfad}/")
for item in sorted(os.listdir(partitioned_pfad)):
    item_path = os.path.join(partitioned_pfad, item)
    if os.path.isdir(item_path):
        # Groesse des Ordners berechnen
        folder_size = sum(
            os.path.getsize(os.path.join(item_path, f))
            for f in os.listdir(item_path)
            if os.path.isfile(os.path.join(item_path, f))
        )
        print(f"   {item}/ ({folder_size / 1024:.0f} KB)")

# SAGEN:
# "Seht ihr? Ordner fuer jeden Monat!"
# "monat=1, monat=2, monat=3..."
# "Jeder Ordner enthaelt nur die Daten fuer diesen Monat."

# Query auf partitionierten Daten
print("\n" + "-" * 50)
print("QUERY AUF PARTITIONIERTE DATEN:")
print("-" * 50)

# Laden
df_part = spark.read.parquet(partitioned_pfad)
df_part.createOrReplaceTempView("taxi_partitioned")

# SAGEN:
# "Jetzt machen wir eine Query nur fuer Monat 3."
# "Spark sollte nur den monat=3 Ordner lesen."

print("\nQuery: Nur Monat 3 (Maerz)")

start_zeit = time.time()

result_part = spark.sql("""
    SELECT
        monat,
        COUNT(*) AS fahrten,
        ROUND(AVG(trip_duration) / 60, 1) AS avg_minuten
    FROM taxi_partitioned
    WHERE monat = 3
    GROUP BY monat
""")

result_part.show()

part_query_zeit = time.time() - start_zeit

print(f"Query Zeit (partitioniert): {part_query_zeit:.2f} Sekunden")

# SAGEN:
# "Extrem schnell!"
# "Spark hat nur 1/12 der Daten gelesen."
# "Die anderen 11 Monate wurden komplett ignoriert."
#
# "Das ist PARTITION PRUNING in Aktion."
# "Bei grossen Datenmengen spart das MINUTEN pro Query."

input("\n>>> Druecke ENTER um fortzufahren...")

# ============================================================
#   ZUSAMMENFASSUNG
# ============================================================

print("\n" + "=" * 70)
print("   ZUSAMMENFASSUNG: WAS HABT IHR HEUTE GELERNT?")
print("=" * 70)

print("""
1. PARQUET vs CSV
   ===============
   - CSV: Row Storage, liest ALLES
   - Parquet: Column Storage, liest NUR was noetig ist
   - Parquet ist 3-100x schneller (je nach Query)
   - Parquet ist 60-90% kleiner (Kompression)

2. KONVERTIERUNG (1 Zeile Code!)
   =============================
   df.write.parquet("pfad")           # Speichern
   spark.read.parquet("pfad")         # Laden

3. EURE SQL-SKILLS FUNKTIONIEREN
   ==============================
   - GROUP BY, ORDER BY, WHERE
   - YEAR(), MONTH(), HOUR(), DATE()
   - LAG(), RANK(), PARTITION BY
   - CTEs mit WITH
   - Alles funktioniert auf Parquet!

4. PARTITIONIERUNG
   ================
   df.write.partitionBy("spalte").parquet("pfad")

   - Daten in Ordner aufgeteilt
   - Query liest nur relevante Ordner
   - PARTITION PRUNING = massive Beschleunigung

5. WANN PARQUET NUTZEN?
   =====================
   - Daten > 100.000 Zeilen
   - Wiederholte Analysen
   - Production / Data Engineering
   - Immer wenn Performance wichtig ist

6. WANN CSV NUTZEN?
   =================
   - Kleine Daten
   - Export fuer Excel
   - Menschenlesbares Format noetig
   - Schneller Datenaustausch
""")

print("\n" + "=" * 70)
print("   PERFORMANCE-ZUSAMMENFASSUNG EURER SESSION:")
print("=" * 70)

print(f"""
DATEIGROESSEN:
   CSV:     {csv_mb:.1f} MB
   Parquet: {parquet_mb:.1f} MB
   Ersparnis: {ersparnis_prozent:.0f}%

LADEZEITEN:
   CSV:     {csv_ladezeit:.2f} Sekunden
   Parquet: {parquet_ladezeit:.2f} Sekunden
   Speedup: {csv_ladezeit / parquet_ladezeit:.1f}x

QUERY-ZEITEN:
   CSV:     {csv_query_zeit:.2f} Sekunden
   Parquet: {parquet_query_zeit:.2f} Sekunden
   Speedup: {csv_query_zeit / parquet_query_zeit:.1f}x
""")

# SAGEN:
# "Das sind EURE Zahlen von heute."
# "Ihr habt selbst gesehen wie viel schneller Parquet ist."
#
# "Ab jetzt: Wenn ihr grosse Daten habt, nutzt Parquet."
# "Euer zukuenftiger Chef wird euch dafuer lieben."
#
# "Fragen?"

# WARTEN: Fragen beantworten

print("\n" + "=" * 70)
print("   CHEAT SHEET - ZUM MITNEHMEN")
print("=" * 70)

print("""
# CSV zu Parquet konvertieren
df = spark.read.csv("daten.csv", header=True, inferSchema=True)
df.write.parquet("daten.parquet")

# Parquet laden
df = spark.read.parquet("daten.parquet")

# Parquet ueberschreiben
df.write.mode("overwrite").parquet("daten.parquet")

# Partitioniert speichern (nach Jahr und Monat)
df.write.partitionBy("jahr", "monat").parquet("daten_partitioned.parquet")

# Partitionierte Daten laden
df = spark.read.parquet("daten_partitioned.parquet")
# WHERE-Filter nutzt automatisch Partition Pruning!
""")

print("\n" + "=" * 70)
print("   SESSION BEENDET - IHR SEID JETZT PARQUET PROFIS!")
print("=" * 70)

# Spark beenden
spark.stop()

print("\nSpark Session beendet.")
print("Bis zum naechsten Mal!")

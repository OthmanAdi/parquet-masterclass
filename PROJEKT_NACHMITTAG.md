# Business Intelligence Report - NYC Taxi Analyse

**Modul 3: Big-Data Analyst Experte**
**Nachmittags-Projekt: 13:15 - 16:00 Uhr**

---

## Dein Szenario

Du bist **Data Analyst** bei einer grossen Taxi-Firma in New York. Dein Chef hat dir folgende Aufgabe gegeben:

> "Ich brauche einen Report ueber unsere Taxi-Performance. Analysiere die Daten und finde heraus: Wann fahren die meisten Taxis? Wie entwickelt sich unser Geschaeft? Welche Tage sind die besten?"

Du hast **1.4 Millionen Fahrt-Datensaetze** als Parquet-Datei. Nutze dein SQL-Wissen um die Fragen zu beantworten.

---

## Setup

Falls noch nicht geschehen, fuege diesen Code in deine `main.py` ein:

```python
import time
import sys
import os

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = os.environ.get('PATH', '') + r';C:\hadoop\bin'
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'

python_exec = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exec
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BI-Report") \
    .config("spark.driver.memory", "4g") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Parquet laden
df = spark.read.parquet("./data/nyc.parquet")
df.createOrReplaceTempView("taxi")

print("Daten geladen! Anzahl Zeilen:", df.count())
```

---

## Aufgabe 1: Basis-Statistiken (20 Minuten)

### Deine Aufgabe

Dein Chef fragt: **"Wie viele Fahrten haben wir? Wie lange dauert eine durchschnittliche Fahrt? Wie viele Passagiere haben wir insgesamt befoerdert?"**

Schreibe eine SQL Query die folgendes berechnet:
- Gesamtanzahl der Fahrten
- Durchschnittliche Fahrtdauer in Minuten (ROUND auf 1 Nachkommastelle)
- Gesamte Fahrtdauer in Stunden
- Durchschnittliche Passagieranzahl (ROUND auf 2 Nachkommastellen)
- Gesamtanzahl befoerderter Passagiere

### Hinweis

- `trip_duration` ist in Sekunden gespeichert
- Sekunden zu Minuten: teile durch 60
- Sekunden zu Stunden: teile durch 3600
- Nutze: `COUNT`, `SUM`, `AVG`, `ROUND`

### Erwartetes Ergebnis

Eine Zeile mit 5 Spalten: anzahl_fahrten, avg_minuten, gesamt_stunden, avg_passagiere, gesamt_passagiere

<details>
<summary>Loesung anzeigen</summary>

```python
aufgabe1 = spark.sql("""
    SELECT
        COUNT(*) AS anzahl_fahrten,
        ROUND(AVG(trip_duration) / 60, 1) AS avg_minuten,
        ROUND(SUM(trip_duration) / 3600, 0) AS gesamt_stunden,
        ROUND(AVG(passenger_count), 2) AS avg_passagiere,
        SUM(passenger_count) AS gesamt_passagiere
    FROM taxi
""")

print("AUFGABE 1: Basis-Statistiken")
aufgabe1.show()
```

</details>

---

## Aufgabe 2: Monatliche Analyse (25 Minuten)

### Deine Aufgabe

Dein Chef fragt: **"Wie verteilen sich die Fahrten ueber die Monate? In welchem Monat hatten wir die meisten Fahrten?"**

Schreibe eine SQL Query die PRO MONAT zeigt:
- Der Monat (1-12)
- Anzahl der Fahrten
- Gesamte Fahrtdauer in Stunden
- Durchschnittliche Fahrtdauer in Minuten

Sortiere nach Monat aufsteigend.

### Hinweis

- Nutze `MONTH(pickup_datetime)` um den Monat zu extrahieren
- `GROUP BY` nach dem Monat
- `ORDER BY` fuer die Sortierung

### Erwartetes Ergebnis

12 Zeilen (eine pro Monat), sortiert von Januar bis Dezember.

<details>
<summary>Loesung anzeigen</summary>

```python
aufgabe2 = spark.sql("""
    SELECT
        MONTH(pickup_datetime) AS monat,
        COUNT(*) AS anzahl_fahrten,
        ROUND(SUM(trip_duration) / 3600, 0) AS gesamt_stunden,
        ROUND(AVG(trip_duration) / 60, 1) AS avg_minuten
    FROM taxi
    GROUP BY MONTH(pickup_datetime)
    ORDER BY monat
""")

print("AUFGABE 2: Monatliche Analyse")
aufgabe2.show(12)
```

</details>

---

## Aufgabe 3: Rush Hour Analyse (20 Minuten)

### Deine Aufgabe

Dein Chef fragt: **"Wann ist Rush Hour? Zu welchen Uhrzeiten fahren die meisten Taxis?"**

Schreibe eine SQL Query die PRO STUNDE zeigt:
- Die Stunde (0-23)
- Anzahl der Fahrten
- Durchschnittliche Fahrtdauer in Minuten

Zeige die **Top 10 Stunden** mit den meisten Fahrten.

### Hinweis

- Nutze `HOUR(pickup_datetime)` um die Stunde zu extrahieren
- `ORDER BY anzahl DESC` fuer die meisten zuerst
- `LIMIT 10` fuer Top 10

### Erwartetes Ergebnis

10 Zeilen mit den verkehrsreichsten Stunden, beginnend mit der Stunde mit den meisten Fahrten.

<details>
<summary>Loesung anzeigen</summary>

```python
aufgabe3 = spark.sql("""
    SELECT
        HOUR(pickup_datetime) AS stunde,
        COUNT(*) AS anzahl_fahrten,
        ROUND(AVG(trip_duration) / 60, 1) AS avg_minuten
    FROM taxi
    GROUP BY HOUR(pickup_datetime)
    ORDER BY anzahl_fahrten DESC
    LIMIT 10
""")

print("AUFGABE 3: Rush Hour - Top 10 Stunden")
aufgabe3.show()
```

</details>

### Bonus-Frage

Schau dir das Ergebnis an: Welche Stunden sind die Rush Hour? Macht das Sinn fuer New York City?

---

## Aufgabe 4: Wachstumsanalyse mit LAG (30 Minuten)

### Deine Aufgabe

Dein Chef fragt: **"Wie entwickelt sich unser Geschaeft von Monat zu Monat? Waechst es oder schrumpft es?"**

Schreibe eine SQL Query die zeigt:
- Monat
- Anzahl Fahrten diesen Monat
- Anzahl Fahrten VORMONAT (mit LAG)
- Differenz zum Vormonat
- Wachstum in Prozent

### Hinweis

- Nutze eine CTE (`WITH monatlich AS (...)`)
- `LAG(spalte) OVER (ORDER BY monat)` holt den Vormonatswert
- Wachstum in Prozent: `(aktuell - vormonat) * 100.0 / vormonat`
- Der erste Monat hat NULL beim Vormonat - das ist korrekt!

### Erwartetes Ergebnis

6 Zeilen (oder wie viele Monate in den Daten sind), mit Wachstumsrate.

<details>
<summary>Loesung anzeigen</summary>

```python
aufgabe4 = spark.sql("""
    WITH monatlich AS (
        SELECT
            MONTH(pickup_datetime) AS monat,
            COUNT(*) AS fahrten
        FROM taxi
        GROUP BY MONTH(pickup_datetime)
    )
    SELECT
        monat,
        fahrten,
        LAG(fahrten) OVER (ORDER BY monat) AS vormonat,
        fahrten - LAG(fahrten) OVER (ORDER BY monat) AS differenz,
        ROUND(
            (fahrten - LAG(fahrten) OVER (ORDER BY monat)) * 100.0
            / LAG(fahrten) OVER (ORDER BY monat), 1
        ) AS wachstum_prozent
    FROM monatlich
    ORDER BY monat
""")

print("AUFGABE 4: Wachstumsanalyse mit LAG")
aufgabe4.show()
```

</details>

### Bonus-Frage

In welchem Monat gab es das staerkste Wachstum? In welchem den staerksten Rueckgang?

---

## Aufgabe 5: Ranking der Tage (25 Minuten)

### Deine Aufgabe

Dein Chef fragt: **"Welche Tage waren unsere besten Tage? Ich will ein Ranking!"**

Schreibe eine SQL Query die zeigt:
- Das vollstaendige Datum (Jahr, Monat, Tag)
- Anzahl der Fahrten an diesem Tag
- Drei Rankings: ROW_NUMBER, RANK, DENSE_RANK (alle nach Fahrten absteigend)

Zeige die **Top 15 Tage**.

### Hinweis

- Nutze `YEAR()`, `MONTH()`, `DAYOFMONTH()` oder kombiniert
- Gruppiere nach dem vollstaendigen Datum
- Nutze `ROW_NUMBER() OVER (ORDER BY ... DESC)`
- Nutze `RANK() OVER (ORDER BY ... DESC)`
- Nutze `DENSE_RANK() OVER (ORDER BY ... DESC)`

### Erwartetes Ergebnis

15 Zeilen mit den Tagen mit den meisten Fahrten und ihren Rankings.

<details>
<summary>Loesung anzeigen</summary>

```python
aufgabe5 = spark.sql("""
    WITH taeglich AS (
        SELECT
            YEAR(pickup_datetime) AS jahr,
            MONTH(pickup_datetime) AS monat,
            DAYOFMONTH(pickup_datetime) AS tag,
            COUNT(*) AS fahrten
        FROM taxi
        GROUP BY YEAR(pickup_datetime), MONTH(pickup_datetime), DAYOFMONTH(pickup_datetime)
    )
    SELECT
        jahr, monat, tag,
        fahrten,
        ROW_NUMBER() OVER (ORDER BY fahrten DESC) AS row_num,
        RANK() OVER (ORDER BY fahrten DESC) AS rang,
        DENSE_RANK() OVER (ORDER BY fahrten DESC) AS dense_rang
    FROM taeglich
    ORDER BY fahrten DESC
    LIMIT 15
""")

print("AUFGABE 5: Ranking der Top 15 Tage")
aufgabe5.show()
```

</details>

### Bonus-Frage

Schau dir die Top-Tage an. Gibt es ein Muster? Sind es bestimmte Wochentage? Events?

---

## Aufgabe 6: Top Stunden PRO Tag - PARTITION BY (30 Minuten)

### Deine Aufgabe

Dein Chef fragt: **"Ich will fuer jeden Tag wissen, welche 3 Stunden die meisten Fahrten hatten."**

Das ist eine komplexe Aufgabe! Du brauchst:
1. Erst: Fahrten pro Tag UND Stunde zaehlen
2. Dann: RANK mit PARTITION BY Tag
3. Dann: Filtern auf Rang <= 3

Zeige nur die ersten 7 Tage im Datensatz (um die Ausgabe uebersichtlich zu halten).

### Hinweis

- `RANK() OVER (PARTITION BY tag ORDER BY fahrten DESC)`
- PARTITION BY bedeutet: Der Rang startet fuer JEDEN Tag neu bei 1
- Du brauchst zwei CTEs oder eine Subquery

### Erwartetes Ergebnis

21 Zeilen (7 Tage x 3 Top-Stunden pro Tag).

<details>
<summary>Loesung anzeigen</summary>

```python
aufgabe6 = spark.sql("""
    WITH stuendlich AS (
        SELECT
            DAYOFMONTH(pickup_datetime) AS tag,
            HOUR(pickup_datetime) AS stunde,
            COUNT(*) AS fahrten
        FROM taxi
        WHERE MONTH(pickup_datetime) = 1
        GROUP BY DAYOFMONTH(pickup_datetime), HOUR(pickup_datetime)
    ),
    mit_rang AS (
        SELECT
            tag,
            stunde,
            fahrten,
            RANK() OVER (PARTITION BY tag ORDER BY fahrten DESC) AS rang
        FROM stuendlich
    )
    SELECT * FROM mit_rang
    WHERE rang <= 3 AND tag <= 7
    ORDER BY tag, rang
""")

print("AUFGABE 6: Top 3 Stunden PRO TAG")
aufgabe6.show(25)
```

</details>

### Bonus-Frage

Gibt es eine Stunde die JEDEN Tag in den Top 3 ist? Was sagt dir das ueber NYC?

---

## Aufgabe 7: Eigene Analyse (30 Minuten)

### Deine Aufgabe

Jetzt bist DU der Analyst. Finde ein **interessantes Insight** in den Daten.

Hier sind einige Ideen:
- Gibt es einen Zusammenhang zwischen Passagieranzahl und Fahrtdauer?
- Welche Kombination aus Tag + Stunde hat die laengsten Fahrten?
- Wie unterscheiden sich Wochenend-Fahrten von Werktags-Fahrten?
- Gibt es "tote Zeiten" mit sehr wenigen Fahrten?

### Anforderungen

- Mindestens eine Aggregation (SUM, AVG, COUNT)
- Mindestens eine Datums-Funktion (YEAR, MONTH, HOUR, etc.)
- Eine sinnvolle Sortierung oder Filter
- BONUS: Nutze eine Window Function

### Dein Code

```python
# AUFGABE 7: Deine eigene Analyse
# Beschreibe hier was du herausfinden willst:
#
# Meine Frage: ________________________________
#
# Mein Code:

eigene_analyse = spark.sql("""
    -- Dein SQL hier
    SELECT
        ???
    FROM taxi
    ???
""")

print("AUFGABE 7: Meine eigene Analyse")
eigene_analyse.show()

# Mein Ergebnis/Insight:
# ________________________________
```

---

## Aufgabe 8: Report erstellen (15 Minuten)

### Deine Aufgabe

Fasse deine Ergebnisse zusammen. Schreibe einen kurzen Report fuer deinen Chef.

Erstelle einen Print-Output der die wichtigsten Erkenntnisse zeigt:

```python
print("""
============================================================
   TAXI PERFORMANCE REPORT
   Analyst: [DEIN NAME]
   Datum: 05.12.2025
============================================================

1. GESAMTSTATISTIK
   - Anzahl Fahrten: ???
   - Durchschnittliche Fahrtdauer: ??? Minuten
   - Befoerderte Passagiere: ???

2. BESTE MONATE
   - Bester Monat: ??? mit ??? Fahrten
   - Schlechtester Monat: ??? mit ??? Fahrten

3. RUSH HOUR
   - Hauptverkehrszeit: ??? Uhr bis ??? Uhr
   - Ruhigste Zeit: ??? Uhr

4. WACHSTUM
   - Staerkstes Wachstum: Monat ??? mit ???%
   - Staerkster Rueckgang: Monat ??? mit ???%

5. MEIN INSIGHT
   [Beschreibe was du in Aufgabe 7 herausgefunden hast]

============================================================
""")
```

---

## Abgabe

Speichere deine `main.py` mit allen Loesungen.

Am Montag besprechen wir einige der interessantesten Analysen!

---

## Cheat Sheet

```sql
-- Aggregationen
COUNT(*), SUM(spalte), AVG(spalte), ROUND(wert, nachkommastellen)

-- Datum
YEAR(datum), MONTH(datum), DAYOFMONTH(datum), HOUR(datum)

-- Window Functions
LAG(spalte) OVER (ORDER BY x)
RANK() OVER (ORDER BY x DESC)
DENSE_RANK() OVER (ORDER BY x DESC)
ROW_NUMBER() OVER (ORDER BY x DESC)

-- Partition By
RANK() OVER (PARTITION BY gruppe ORDER BY wert DESC)

-- CTE
WITH name AS (
    SELECT ...
)
SELECT ... FROM name
```

---

## Ressourcen

- [Spark SQL Functions](https://spark.apache.org/docs/latest/sql-ref-functions.html)
- [Window Functions Guide](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)

---

**Viel Erfolg! Du schaffst das!**

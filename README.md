# **ETL proces datasetu IMDb**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **IMDb** datasetu. Projekt sa zameriava na preskúmanie filmov, režisérov a ich hodnotení na základe rôznych metrík. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrík.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, režisérov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v kinematografii, najpopulárnejšie filmy a charakteristiky úspešných režisérov.

Dataset obsahuje šesť hlavných tabuliek:
- `movies`
- `genres`
- `names`
- `ratings`
- `director_mapping`
- `role_mapping`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/user-attachments/assets/34ed1f47-7dc3-4da3-b5f8-739eff6f2488" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma IMDb</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_movies`**: Obsahuje podrobné informácie o filmoch (názov, rok vydania, krajina, tržby, jazyky).
- **`dim_directors`**: Obsahuje informácie o režiséroch (meno, dátum narodenia, výška).
- **`dim_genres`**: Obsahuje žánre filmov.
- **`dim_roles`**: Obsahuje informácie o hercoch a ich úlohách.
- **`dim_date`**: Zahrňuje informácie o dátumoch vydania filmov (deň, mesiac, rok).

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/user-attachments/assets/5ea69e3b-6b1a-4d86-a4c9-0bed44565d89" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre IMDb</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**

Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `dragon_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu na vytvorenie stage:
```sql
CREATE OR REPLACE STAGE dragon_stage
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', '')
        EMPTY_FIELD_AS_NULL = TRUE
);
```
Do stage boli následne nahraté súbory obsahujúce údaje o filmoch, režiséroch, hodnoteniach, rolách, menách a žanroch filmov. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO movies_staging
FROM @dragon_stage/movie.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```


---
### **3.2 Transform (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

`Dim_movies` obsahuje všetky informácie o filmoch. Táto dimenzia je typu SCD 0, pretože základné údaje o filme ako názov, rok vydania či krajina pôvodu sa nemenia. Obsahuje názov filmu, rok vydania, dátum uvedenia, dĺžku trvania, krajinu pôvodu, celosvetové tržby, jazyky a produkčnú spoločnosť.
```sql
CREATE TABLE dim_movies AS
SELECT DISTINCT
    m.id AS movie_id,
    m.title,
    m.year,
    m.date_published,
    m.duration,
    m.country,
    m.worldwide_gross_income,
    m.languages,
    m.production_company
FROM movies_staging m;
```

`Dim_directors` obsahuje údaje o režiséroch. Táto dimenzia je typu SCD 1, keďže niektoré údaje (napr. výška) sa môžu časom meniť, ale nepotrebujeme uchovávať históriu zmien. Obsahuje meno, dátum narodenia a výšku.
```sql
CREATE TABLE dim_directors AS
SELECT DISTINCT
    n.id AS director_id,
    n.name,
    n.date_of_birth,
    n.height
FROM names_staging n
JOIN director_mapping_staging dm ON n.id = dm.name_id;
```

`Dim_genres` je lookup tabuľka obsahujúca jedinečné žánre filmov. Je to typu SCD 0, pretože žánre sú statické a nemenia sa. Obsahuje názov žánru.
```sql
CREATE TABLE dim_genres AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY g.genre) AS genre_id,
    g.genre
FROM genres_staging g;
```

`Dim_roles` obsahuje informácie o hercoch a ich úlohách. Podobne ako dimenzia režisérov je typu SCD 1. Obsahuje názov role, meno herca, dátum narodenia herca a výšku herca.
```sql
CREATE TABLE dim_roles AS
SELECT DISTINCT
    n.id AS role_id,
    rm.category AS role_name,
    n.name AS actor_name,
    n.date_of_birth,
    n.height
FROM names_staging n
JOIN role_mapping_staging rm ON n.id = rm.name_id;
```

`Dim_date` je kalendárna dimenzia obsahujúca časové údaje o vydaní filmov. Je typu SCD 0, pretože časové údaje sú nemenné. Obsahuje ID dátumu (samotný dátum), deň, mesiac a rok.
```sql
CREATE TABLE dim_date AS
SELECT DISTINCT
    CAST(date_published AS DATE) AS date_id,
    DAY(date_published) AS day,
    MONTH(date_published) AS month,
    YEAR(date_published) AS year
FROM movies_staging
WHERE date_published IS NOT NULL;
```

Faktová tabuľka `fact_ratings` spája všetky dimenzie a obsahuje hodnotenia filmov:
```sql
CREATE TABLE fact_ratings AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY r.movie_id) AS rating_id,
    r.avg_rating AS rating,
    dg.genre_id AS dim_genres_genre_id,
    m.id AS dim_movies_movie_id,
    d.date_id AS dim_date_date_id,
    dr.director_id AS dim_directors_director_id,
    rl.role_id AS dim_roles_role_id
FROM ratings_staging r
LEFT JOIN movies_staging m ON r.movie_id = m.id
LEFT JOIN genres_staging g ON r.movie_id = g.movie_id
LEFT JOIN dim_genres dg ON g.genre = dg.genre
LEFT JOIN dim_date d ON m.date_published = d.date_id
LEFT JOIN director_mapping_staging dm ON r.movie_id = dm.movie_id
LEFT JOIN dim_directors dr ON dm.name_id = dr.director_id
LEFT JOIN role_mapping_staging rm ON r.movie_id = rm.movie_id
LEFT JOIN dim_roles rl ON rm.name_id = rl.role_id;
```

---
### **3.3 Load (Načítanie dát)**

Po vytvorení dimenzií a faktovej tabuľky boli staging tabuľky odstránené na optimalizáciu úložiska:
```sql
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS role_mapping_staging;
DROP TABLE IF EXISTS ratings_staging;
```

---
## **4 Vizualizácia dát**

Dashboard obsahuje `5 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov a režisérov.

<p align="center">
  <img src="https://github.com/user-attachments/assets/f8b43f37-7370-432d-b65f-3bac2c5a8380" alt="Dashboard">
  <br>
  <em>Obrázok 3 Dashboard IMDb datasetu</em>
</p>


---
### **Graf 1: Krajiny s najväčším počtom filmov (Top 5)**
Táto vizualizácia zobrazuje 5 krajín s najväčším počtom produkovaných filmov. Umožňuje identifikovať hlavné centrá filmovej produkcie. Zistíme napríklad, že `USA` má výrazne viac vyprodukovaných filmov ako ktorá koľvek iná krajina.

```sql
SELECT 
    m.country, 
    COUNT(*) AS movie_count
FROM dim_movies m
GROUP BY m.country
ORDER BY movie_count DESC
LIMIT 5;
```

---
### **Graf 2: Najdlhšie filmy (Top 5)**
Graf zobrazuje 5 filmov s najdlhšou stopážou. Táto metrika pomáha pochopiť trendy v dĺžke filmov a identifikovať výnimočne dlhé produkcie. Zatiaľ čo priemerná dĺžka filmu je do 200 minút, zistíme že najdlhší film je `La flor` so stopážou až 808 minút.

```sql
SELECT title, duration 
FROM dim_movies 
WHERE duration IS NOT NULL 
ORDER BY duration DESC 
LIMIT 5;
```

---
### **Graf 3: Filmy s najvyššími tržbami v USD (Top 10)**
Tento graf ukazuje 10 filmov s najvyššími celosvetovými tržbami v USD. Poskytuje pohľad na komerčne najúspešnejšie filmy.

```sql
SELECT title, 
       CAST(REPLACE(REPLACE(worldwide_gross_income, '$ ', ''), ' ', '') AS INTEGER) as revenue
FROM dim_movies 
WHERE worldwide_gross_income IS NOT NULL 
AND worldwide_gross_income NOT LIKE '%NULL%'
AND worldwide_gross_income LIKE '$%'
ORDER BY CAST(REPLACE(REPLACE(worldwide_gross_income, '$ ', ''), ' ', '') AS INTEGER) DESC 
LIMIT 10;
```

---
### **Graf 4: Najvyšší režiséri (Top 10)**
Graf zobrazuje 10 najvyšších režisérov v databáze. Ide o zaujímavú štatistiku, ktorá prezentuje fyzické charakteristiky režisérov.

```sql
SELECT DISTINCT name, height 
FROM dim_directors 
WHERE height IS NOT NULL 
ORDER BY height DESC 
LIMIT 10;
```

---
### **Graf 5: Najproduktívnejší režiséri (Top 5)**
Tento graf ukazuje 5 režisérov s najväčším počtom režírovaných filmov v databáze. Pomáha identifikovať najaktívnejších tvorcov v priemysle.

```sql
SELECT d.name, COUNT(DISTINCT fr.dim_movies_movie_id) as movie_count
FROM dim_directors d
JOIN fact_ratings fr ON d.director_id = fr.dim_directors_director_id
GROUP BY d.name
ORDER BY movie_count DESC
LIMIT 5;
```

Dashboard poskytuje komplexný pohľad na filmové dáta, pričom zodpovedá dôležité otázky týkajúce sa filmovej produkcie, úspešnosti filmov a charakteristík režisérov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na analýzu trendov v filmovom priemysle.



**Autor:** Simon Kováčik

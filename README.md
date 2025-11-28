# Data Engineering Mini Project

> **Un pipeline ETL completo y producción-ready para datos de criptomonedas usando las herramientas más demandadas del Modern Data Stack**

Este proyecto demuestra un flujo completo de ingeniería de datos desde la extracción de datos de una API pública, pasando por transformaciones SQL modulares, hasta la generación de tablas analíticas listas para visualización o reportes.

## Tabla de Contenidos

- [¿Qué hace este proyecto?](#qué-hace-este-proyecto)
- [Conceptos Demostrados](#conceptos-demostrados)
- [Tech Stack](#tech-stack)
- [Arquitectura](#arquitectura)
- [Instalación y Uso](#instalación-y-uso)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Pipeline Detallado](#pipeline-detallado)
- [Modelos de Datos](#modelos-de-datos)
- [Monitoreo y Troubleshooting](#monitoreo-y-troubleshooting)
- [Recursos Adicionales](#recursos-adicionales)

---

## ¿Qué hace este proyecto?

Este proyecto implementa un **pipeline ETL automatizado** que:

1. **Extrae** datos del top 50 de criptomonedas desde la API de CoinGecko
2. **Carga** los datos raw en una base de datos PostgreSQL
3. **Transforma** los datos usando dbt en capas (staging → marts)
4. **Valida** la calidad de los datos con tests automáticos
5. **Documenta** automáticamente el linaje y esquema de datos
6. **Orquesta** todo el proceso con Apache Airflow cada 6 horas

**Resultado**: Tablas analíticas listas para dashboards o reportes con métricas como volatilidad, categorías de market cap, performance, etc.

---

## Conceptos Demostrados

Este proyecto es ideal para portafolio porque demuestra conocimiento en:

### Ingeniería de Datos
- ✅ **ETL Pipeline Design**: Separación clara de Extract, Load, Transform
- ✅ **Data Modeling**: Medallion Architecture (Raw → Staging → Marts)
- ✅ **Data Quality**: Tests automáticos con dbt
- ✅ **Orchestration**: Workflows complejos con Airflow DAGs
- ✅ **SQL Analytics**: Transformaciones SQL con CTEs, CASE statements, agregaciones

### DevOps & Infraestructura
- ✅ **Infrastructure as Code**: Docker Compose para todo el stack
- ✅ **Containerization**: Multi-container orchestration
- ✅ **Environment Management**: Variables de entorno y configuración
- ✅ **Reproducibility**: Un solo comando para levantar todo el sistema

### Best Practices
- ✅ **Version Control**: Estructura Git-friendly
- ✅ **Documentation**: README completo + dbt docs automáticos
- ✅ **Modularity**: Código organizado y reutilizable
- ✅ **Scalability**: Fácil de extender con nuevas fuentes de datos
- ✅ **Monitoring**: Logs estructurados y healthchecks

---

## Tech Stack

| Tecnología | Propósito | ¿Por qué esta herramienta? |
|------------|-----------|----------------------------|
| **Apache Airflow** | Orquestación de workflows | El estándar de la industria para scheduling y monitoreo de pipelines de datos. Usado por Airbnb, Twitter, Reddit |
| **dbt (data build tool)** | Transformaciones SQL | La herramienta #1 para Analytics Engineers. Permite tests, documentación y versionado de transformaciones SQL |
| **PostgreSQL** | Data Warehouse | Base de datos relacional robusta y open-source. Fácil de escalar a Redshift, BigQuery o Snowflake |
| **Docker Compose** | Orquestación de contenedores | Permite replicar todo el ambiente de desarrollo/producción en cualquier máquina con un solo comando |
| **Python 3.11** | Scripting y procesamiento | Lenguaje estándar en Data Engineering. Usado para extracción, procesamiento y orquestación |
| **CoinGecko API** | Fuente de datos | API pública gratuita con datos reales de mercado. Fácil de reemplazar con otras APIs |

## Arquitectura

### Diagrama de Arquitectura

```
                    ┌─────────────────────────────────────────┐
                    │         DOCKER COMPOSE                  │
                    │                                         │
┌─────────────┐     │  ┌──────────────────────────────────┐  │
│             │     │  │      APACHE AIRFLOW              │  │
│  CoinGecko  │────────▶│                                  │  │
│     API     │ HTTP  │  │  ┌─────────┐   ┌────────────┐ │  │
│             │     │  │  │ Webserver│   │  Scheduler │ │  │
└─────────────┘     │  │  └─────────┘   └────────────┘ │  │
                    │  │                                  │  │
   (Datos reales    │  │  DAG: crypto_etl_pipeline        │  │
    de mercado)     │  └────────────┬─────────────────────┘  │
                    │               │                        │
                    │               ▼                        │
                    │  ┌──────────────────────────────────┐  │
                    │  │       POSTGRESQL                 │  │
                    │  │                                  │  │
                    │  │  Table: raw_crypto_data          │  │
                    │  │  (Datos sin procesar)            │  │
                    │  └────────────┬─────────────────────┘  │
                    │               │                        │
                    │               ▼                        │
                    │  ┌──────────────────────────────────┐  │
                    │  │         DBT CORE                 │  │
                    │  │                                  │  │
                    │  │  Staging Layer:                  │  │
                    │  │  └─ stg_crypto_data (VIEW)       │  │
                    │  │                                  │  │
                    │  │  Marts Layer:                    │  │
                    │  │  └─ crypto_market_summary (TABLE)│  │
                    │  │                                  │  │
                    │  │  + Tests + Documentation         │  │
                    │  └──────────────────────────────────┘  │
                    │                                         │
                    └─────────────────────────────────────────┘

           Todo corre en tu máquina local con un solo comando
```

### Flujo de Datos Detallado

#### 1️⃣ **EXTRACT** (Python Script)
- **Archivo**: `scripts/extract_data.py`
- **Acción**: Hace request GET a la API de CoinGecko
- **Datos obtenidos**: Top 50 cryptos con precio, market cap, volumen, cambios 24h, etc.
- **Formato**: JSON almacenado en `data/raw/crypto_data_YYYYMMDD_HHMMSS.json`
- **Frecuencia**: Cada 6 horas (configurable)

#### 2️⃣ **LOAD** (Python + SQLAlchemy)
- **Archivo**: `scripts/extract_data.py` (función `load_to_postgres`)
- **Acción**: Lee el JSON y lo carga a PostgreSQL
- **Tabla destino**: `raw_crypto_data` (schema público)
- **Estrategia**: Append (se van acumulando registros históricos)

#### 3️⃣ **TRANSFORM** (dbt)
- **Staging**: `stg_crypto_data` (VIEW)
  - Limpieza de datos (nulls → 0, uppercase symbols)
  - Renombrado de columnas para claridad
  - Parsing de timestamps
  - **Materialización**: VIEW (no consume espacio, siempre actualizado)

- **Marts**: `crypto_market_summary` (TABLE)
  - Cálculos de métricas analíticas:
    - Categoría de volatilidad (High/Medium/Low)
    - Categoría de market cap (Large/Mid/Small)
    - Indicador de performance (Strong Gain/Gain/Loss/Strong Loss)
    - Ratio volumen/market cap (indicador de liquidez)
  - **Materialización**: TABLE (optimizado para queries analíticas)

#### 4️⃣ **TEST** (dbt tests)
- Tests de unicidad (crypto_id debe ser único)
- Tests de NOT NULL (campos críticos no pueden ser nulos)
- Tests personalizados definidos en `schema.yml`
- **Resultado**: Pipeline falla si los datos no pasan los tests

#### 5️⃣ **DOCUMENT** (dbt docs)
- Genera documentación HTML interactiva
- Muestra lineage (qué tabla depende de qué)
- Descripción de cada columna
- Código SQL de cada modelo

#### 6️⃣ **ORCHESTRATE** (Airflow DAG)
- **Archivo**: `airflow/dags/crypto_etl_pipeline.py`
- **Dependencias**: `extract → load → dbt_run → dbt_test → dbt_docs`
- **Retry logic**: 2 reintentos con 5 minutos de espera
- **Alertas**: Configurable por email (actualmente desactivado)

---

## Project Structure

```
data-engineering-mini-project/
├── README.md
├── docker-compose.yml          # Docker services configuration
├── .env.example                # Environment variables template
├── .gitignore
│
├── airflow/
│   ├── dags/
│   │   └── crypto_etl_pipeline.py   # Main ETL DAG
│   ├── plugins/
│   └── requirements.txt        # Python dependencies
│
├── dbt/
│   ├── dbt_project.yml         # dbt configuration
│   ├── profiles.yml            # Database connection
│   └── models/
│       ├── staging/
│       │   ├── stg_crypto_data.sql
│       │   └── schema.yml
│       └── marts/
│           ├── crypto_market_summary.sql
│           └── schema.yml
│
├── scripts/
│   └── extract_data.py         # Data extraction script
│
└── data/
    └── raw/                    # Raw data storage
```

## Instalación y Uso

### Prerrequisitos

Antes de comenzar, asegúrate de tener instalado:

| Software | Versión mínima | Link de descarga |
|----------|----------------|------------------|
| Docker Desktop | 20.10+ | [docker.com](https://www.docker.com/products/docker-desktop) |
| Docker Compose | 2.0+ | Incluido en Docker Desktop |
| Git | 2.0+ | [git-scm.com](https://git-scm.com/) |

**Recursos de sistema recomendados**:
- 💾 RAM: Mínimo 4GB disponible para Docker
- 💿 Disco: ~5GB de espacio libre
- 🖥️ CPU: 2 cores mínimo

---

### 🚀 Instalación Paso a Paso

#### **Paso 1: Clonar el repositorio**

```bash
git clone https://github.com/tu-usuario/data-engineering-mini-project.git
cd data-engineering-mini-project
```

#### **Paso 2: Configurar variables de entorno**

```bash
# El archivo .env ya está incluido con valores por defecto
# Si quieres personalizarlo:
nano .env  # o usa tu editor favorito

# Contenido del .env:
# POSTGRES_USER=airflow
# POSTGRES_PASSWORD=airflow
# POSTGRES_DB=data_warehouse
```

> 💡 **Nota**: Para producción, cambia las credenciales por defecto

#### **Paso 3: Levantar los servicios**

```bash
# Iniciar todos los contenedores en background
docker-compose up -d

# Ver el progreso de inicio
docker-compose logs -f

# Espera ~2 minutos hasta ver: "Airflow webserver is ready"
```

**Verificar que todo está corriendo**:
```bash
docker-compose ps

# Deberías ver 4 contenedores corriendo:
# - de_postgres          (PostgreSQL database)
# - de_airflow_webserver (Airflow UI)
# - de_airflow_scheduler (Airflow worker)
# - de_airflow_init      (Completed - inicialización)
```

#### **Paso 4: Acceder a Airflow UI**

1. Abre tu navegador en: **http://localhost:8080**
2. Ingresa las credenciales:
   ```
   Usuario: admin
   Password: admin
   ```
3. Verás el dashboard de Airflow con el DAG `crypto_etl_pipeline`

![Airflow Dashboard](https://via.placeholder.com/800x400.png?text=Airflow+UI+Dashboard)

#### **Paso 5: Ejecutar el pipeline**

**Opción A: Ejecución manual (recomendado para primera vez)**
1. En Airflow UI, localiza el DAG `crypto_etl_pipeline`
2. Actívalo usando el toggle (OFF → ON)
3. Click en el botón "▶️ Trigger DAG" (arriba a la derecha)
4. Ve a "Graph" view para monitorear el progreso en tiempo real

**Opción B: Dejar que se ejecute automáticamente**
- El DAG está configurado para correr **cada 6 horas**
- Primera ejecución: Cuando lo actives
- Siguientes: 00:00, 06:00, 12:00, 18:00 UTC

#### **Paso 6: Verificar los resultados**

**Conectarse a la base de datos**:
```bash
# Opción 1: Desde la terminal
docker exec -it de_postgres psql -U airflow -d data_warehouse

# Opción 2: Usar un cliente SQL (DBeaver, pgAdmin, etc.)
# Host: localhost
# Port: 5432
# Database: data_warehouse
# User: airflow
# Password: airflow
```

**Queries de ejemplo**:
```sql
-- Ver datos raw
SELECT id, symbol, name, current_price, market_cap_rank
FROM raw_crypto_data
LIMIT 10;

-- Ver datos transformados (staging)
SELECT crypto_id, symbol, crypto_name, current_price
FROM stg_crypto_data
LIMIT 10;

-- Ver tabla analítica final (marts)
SELECT
    crypto_name,
    symbol,
    current_price,
    market_cap_category,
    volatility_24h,
    performance_24h
FROM marts.crypto_market_summary
ORDER BY market_cap_rank
LIMIT 10;

-- Top 5 criptos más volátiles
SELECT crypto_name, volatility_24h, price_change_24h_pct
FROM marts.crypto_market_summary
WHERE volatility_24h = 'High'
ORDER BY ABS(price_change_24h_pct) DESC
LIMIT 5;
```

---

### 🔄 Comandos Útiles

```bash
# Ver logs en tiempo real
docker-compose logs -f airflow-scheduler

# Reiniciar servicios
docker-compose restart

# Detener todo
docker-compose down

# Detener y borrar TODOS los datos (⚠️ cuidado!)
docker-compose down -v

# Ver uso de recursos
docker stats

# Acceder a un contenedor
docker exec -it de_airflow_scheduler bash
```

### ⏰ Programación del Pipeline

**Configuración actual**: Cron expression `0 */6 * * *`
- Ejecuta cada 6 horas: 00:00, 06:00, 12:00, 18:00 UTC
- Configurable en `airflow/dags/crypto_etl_pipeline.py`

**Otras opciones de schedule**:
```python
schedule_interval='@hourly'       # Cada hora
schedule_interval='@daily'        # Diario a medianoche
schedule_interval='0 9 * * *'     # Diario a las 9 AM
schedule_interval='*/30 * * * *'  # Cada 30 minutos
schedule_interval=None            # Solo manual
```

---

## Modelos de Datos

Este proyecto sigue la **Medallion Architecture** (Bronze → Silver → Gold), también conocida como arquitectura de capas de datos.

### 🥉 Capa RAW (Bronze Layer)

**Tabla**: `raw_crypto_data`
- **Tipo**: Table (PostgreSQL)
- **Schema**: `public`
- **Propósito**: Almacenar datos crudos tal como vienen de la API
- **Estrategia**: Append-only (histórico completo)

**Columnas principales**:
```sql
id                     VARCHAR    -- Identificador único (ej: 'bitcoin')
symbol                 VARCHAR    -- Símbolo (ej: 'btc')
name                   VARCHAR    -- Nombre completo
current_price          FLOAT      -- Precio actual en USD
market_cap             BIGINT     -- Capitalización de mercado
market_cap_rank        INTEGER    -- Ranking por market cap
total_volume           BIGINT     -- Volumen de trading 24h
high_24h               FLOAT      -- Precio máximo 24h
low_24h                FLOAT      -- Precio mínimo 24h
price_change_24h       FLOAT      -- Cambio de precio 24h (USD)
price_change_percentage_24h FLOAT -- Cambio de precio 24h (%)
circulating_supply     BIGINT     -- Supply en circulación
total_supply           BIGINT     -- Supply total
max_supply             BIGINT     -- Supply máximo
ath                    FLOAT      -- All-time high price
atl                    FLOAT      -- All-time low price
last_updated           TIMESTAMP  -- Última actualización de la API
extracted_at           TIMESTAMP  -- Timestamp de extracción
```

---

### 🥈 Capa STAGING (Silver Layer)

**Modelo**: `stg_crypto_data`
- **Tipo**: VIEW (se recalcula en cada query)
- **Schema**: `public`
- **Propósito**: Limpiar y estandarizar datos raw
- **Archivo**: `dbt/models/staging/stg_crypto_data.sql`

**Transformaciones aplicadas**:
- ✅ Renombrado de columnas para mayor claridad (`id` → `crypto_id`)
- ✅ Conversión de símbolos a UPPERCASE (`btc` → `BTC`)
- ✅ Manejo de nulls (`COALESCE(fully_diluted_valuation, 0)`)
- ✅ Casting de timestamps para consistencia
- ✅ Eliminación de columnas innecesarias

**Tests aplicados**:
- `unique`: `crypto_id` debe ser único
- `not_null`: `crypto_id` y `current_price` no pueden ser null

**SQL de ejemplo**:
```sql
SELECT
    crypto_id,
    symbol,
    crypto_name,
    current_price,
    market_cap,
    all_time_high,
    all_time_low
FROM stg_crypto_data
WHERE market_cap_rank <= 10;
```

---

### 🥇 Capa MARTS (Gold Layer)

**Modelo**: `crypto_market_summary`
- **Tipo**: TABLE (materializada, optimizada para analytics)
- **Schema**: `marts`
- **Propósito**: Datos listos para análisis y visualización
- **Archivo**: `dbt/models/marts/crypto_market_summary.sql`

**Métricas calculadas**:

1. **Volatility Indicator** (`volatility_24h`)
   - **High**: Cambio de precio > 10%
   - **Medium**: Cambio de precio entre 5-10%
   - **Low**: Cambio de precio < 5%

2. **Market Cap Category** (`market_cap_category`)
   - **Large Cap**: Market cap > $10B
   - **Mid Cap**: Market cap $1B - $10B
   - **Small Cap**: Market cap < $1B

3. **Performance Indicator** (`performance_24h`)
   - **Strong Gain**: +5% o más
   - **Gain**: 0% a +5%
   - **Loss**: 0% a -5%
   - **Strong Loss**: -5% o menos

4. **Liquidity Ratio** (`volume_to_mcap_ratio`)
   - Ratio de volumen diario vs market cap
   - Indica qué tan líquido es el activo
   - Fórmula: `(total_volume / market_cap) * 100`

**Columnas finales**:
```sql
crypto_id                    VARCHAR
symbol                       VARCHAR
crypto_name                  VARCHAR
current_price                FLOAT
market_cap                   BIGINT
market_cap_rank              INTEGER
total_volume                 BIGINT
price_change_24h_pct         NUMERIC(10,2)
market_cap_change_24h_pct    NUMERIC(10,2)
volatility_24h               VARCHAR    -- High/Medium/Low
market_cap_category          VARCHAR    -- Large/Mid/Small Cap
performance_24h              VARCHAR    -- Strong Gain/Gain/Loss/Strong Loss
volume_to_mcap_ratio         NUMERIC(10,2)
last_updated                 TIMESTAMP
extracted_at                 TIMESTAMP
```

**Queries analíticas de ejemplo**:

```sql
-- Distribución de cryptos por categoría de market cap
SELECT
    market_cap_category,
    COUNT(*) as count,
    ROUND(AVG(current_price)::NUMERIC, 2) as avg_price
FROM marts.crypto_market_summary
GROUP BY market_cap_category;

-- Top 10 cryptos con mejor performance 24h
SELECT
    crypto_name,
    symbol,
    price_change_24h_pct,
    performance_24h
FROM marts.crypto_market_summary
WHERE performance_24h IN ('Strong Gain', 'Gain')
ORDER BY price_change_24h_pct DESC
LIMIT 10;

-- Cryptos más líquidas (alto volumen relativo)
SELECT
    crypto_name,
    symbol,
    volume_to_mcap_ratio,
    total_volume
FROM marts.crypto_market_summary
WHERE volume_to_mcap_ratio > 10
ORDER BY volume_to_mcap_ratio DESC;

-- Análisis de volatilidad por categoría de market cap
SELECT
    market_cap_category,
    volatility_24h,
    COUNT(*) as count
FROM marts.crypto_market_summary
GROUP BY market_cap_category, volatility_24h
ORDER BY market_cap_category, volatility_24h;
```

---

### 📊 Lineage de Datos

```
CoinGecko API
     ↓
raw_crypto_data (Bronze)
     ↓
stg_crypto_data (Silver) ← Tests de calidad
     ↓
crypto_market_summary (Gold) ← Métricas analíticas
     ↓
Dashboard / BI Tool / Análisis
```

---

## dbt Commands

Run dbt commands inside the Airflow scheduler container:

```bash
# Access the container
docker exec -it de_airflow_scheduler bash

# Navigate to dbt directory
cd /opt/airflow/dbt

# Run all models
dbt run --profiles-dir .

# Run specific model
dbt run --select stg_crypto_data --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Generate documentation
dbt docs generate --profiles-dir .

# View documentation (requires additional setup)
dbt docs serve --profiles-dir .
```

## Database Access

Connect to PostgreSQL directly:

```bash
# Using Docker
docker exec -it de_postgres psql -U airflow -d data_warehouse

# Using external client
Host: localhost
Port: 5432
Database: data_warehouse
User: airflow
Password: airflow
```

### Query Examples

```sql
-- View raw data
SELECT * FROM raw_crypto_data LIMIT 10;

-- View staged data
SELECT * FROM stg_crypto_data LIMIT 10;

-- View analytics summary
SELECT * FROM marts.crypto_market_summary
ORDER BY market_cap_rank
LIMIT 10;

-- Top gainers in 24h
SELECT crypto_name, symbol, price_change_24h_pct
FROM marts.crypto_market_summary
WHERE performance_24h = 'Strong Gain'
ORDER BY price_change_24h_pct DESC;
```

## Monitoring & Troubleshooting

### View Logs

```bash
# Airflow webserver logs
docker logs de_airflow_webserver

# Airflow scheduler logs
docker logs de_airflow_scheduler

# PostgreSQL logs
docker logs de_postgres

# Follow logs in real-time
docker logs -f de_airflow_scheduler
```

### Common Issues

**Issue**: Airflow UI not loading
```bash
# Restart services
docker-compose restart
```

**Issue**: DAG not appearing
```bash
# Check scheduler logs
docker logs de_airflow_scheduler

# Verify DAG syntax
docker exec -it de_airflow_scheduler airflow dags list
```

**Issue**: dbt connection error
```bash
# Verify PostgreSQL is running
docker-compose ps postgres

# Test connection
docker exec -it de_airflow_scheduler psql -h postgres -U airflow -d data_warehouse
```

## Stopping the Project

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (deletes all data)
docker-compose down -v
```

## Extending the Project

### Add More Data Sources
1. Create new extraction script in `scripts/`
2. Add task to DAG in `airflow/dags/`
3. Create corresponding dbt models

### Add More Transformations
1. Create new SQL models in `dbt/models/`
2. Add tests in `schema.yml`
3. Run `dbt run` to materialize

### Schedule Changes
Edit `schedule_interval` in `crypto_etl_pipeline.py`:
```python
schedule_interval='0 */6 * * *',  # Every 6 hours
# Other examples:
# '@daily'           - Once per day
# '@hourly'          - Once per hour
# '0 9 * * *'        - Daily at 9 AM
```

## 🚀 Extensiones y Mejoras Posibles

Este proyecto es un template perfecto para extender. Aquí algunas ideas:

### 1. **Agregar más fuentes de datos**
```python
# Ejemplo: Agregar datos de noticias de cryptos
def extract_news_data():
    url = "https://newsapi.org/v2/everything?q=cryptocurrency"
    # ... similar a extract_crypto_data
```

### 2. **Implementar alertas**
```python
# En el DAG de Airflow
from airflow.operators.email import EmailOperator

email_alert = EmailOperator(
    task_id='send_alert',
    to='tu-email@example.com',
    subject='ETL Pipeline Completado',
    html_content='Pipeline ejecutado exitosamente'
)
```

### 3. **Agregar incremental loading en dbt**
```sql
-- En dbt, usar incremental materializations
{{ config(materialized='incremental', unique_key='crypto_id') }}
```

### 4. **Conectar con un dashboard**
- **Metabase**: Open-source, fácil de usar
- **Apache Superset**: Potente y escalable
- **Grafana**: Para métricas en tiempo real
- **Power BI / Tableau**: Enterprise solutions

### 5. **Agregar más tests de dbt**
```yaml
# En schema.yml
- name: current_price
  tests:
    - not_null
    - dbt_utils.expression_is_true:
        expression: "> 0"
```

### 6. **Implementar CI/CD**
```yaml
# .github/workflows/ci.yml
name: CI
on: [push]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run dbt tests
        run: dbt test
```

### 7. **Migrar a la nube**
- **AWS**: Airflow → MWAA, PostgreSQL → RDS/Redshift
- **GCP**: Airflow → Composer, PostgreSQL → BigQuery
- **Azure**: Airflow → Data Factory, PostgreSQL → Synapse

---

## 📚 Recursos de Aprendizaje

### Documentación Oficial
- [Apache Airflow Docs](https://airflow.apache.org/docs/) - Orquestación de workflows
- [dbt Documentation](https://docs.getdbt.com/) - Transformaciones SQL
- [CoinGecko API](https://www.coingecko.com/en/api/documentation) - Fuente de datos
- [PostgreSQL Docs](https://www.postgresql.org/docs/) - Base de datos

### Tutoriales Recomendados
- [Airflow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)
- [dbt Learn](https://courses.getdbt.com/collections) - Curso gratuito oficial
- [Modern Data Stack](https://www.moderndatastack.xyz/) - Herramientas del ecosistema

### Comunidades
- [dbt Slack Community](https://www.getdbt.com/community/) - 50K+ data practitioners
- [r/dataengineering](https://reddit.com/r/dataengineering) - Reddit community
- [Data Engineering Weekly](https://www.dataengineeringweekly.com/) - Newsletter

---

## 🎯 Casos de Uso

Este proyecto puede adaptarse para múltiples industrias:

1. **Finanzas**: Stock prices, forex rates, financial indicators
2. **E-commerce**: Product prices, inventory levels, sales metrics
3. **Marketing**: Campaign metrics, social media stats, web analytics
4. **IoT**: Sensor data, device telemetry, time series data
5. **Healthcare**: Patient metrics, hospital occupancy, medical device data

---

## 🤝 Contribuciones

Este es un proyecto de portfolio, pero las mejoras son bienvenidas:

1. Fork el repositorio
2. Crea una branch (`git checkout -b feature/mejora`)
3. Commit tus cambios (`git commit -m 'Agregar nueva feature'`)
4. Push a la branch (`git push origin feature/mejora`)
5. Abre un Pull Request

---

## 📝 Notas Técnicas

### Por qué estas decisiones de arquitectura

**¿Por qué Airflow y no solo cron jobs?**
- UI para monitoreo visual
- Manejo de dependencias entre tareas
- Retry logic automático
- Logging centralizado
- Escalable a múltiples workers

**¿Por qué dbt y no solo SQL scripts?**
- Versionado de transformaciones
- Tests automáticos de calidad de datos
- Documentación autogenerada
- Lineage tracking
- Macros y reutilización de código

**¿Por qué PostgreSQL y no un data warehouse cloud?**
- Gratis y open-source
- Fácil de correr localmente
- Fácil de migrar a Redshift, BigQuery, Snowflake
- Bueno para aprender fundamentos

**¿Por qué Docker Compose y no Kubernetes?**
- Más simple para desarrollo local
- No requiere cluster
- Suficiente para proyectos pequeños/medianos
- Fácil de entender para principiantes

---

## 📄 Licencia

Este proyecto está bajo la licencia MIT. Eres libre de usar, modificar y distribuir este código.

```
MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
```

---

## ✨ Autor

**Proyecto de Portfolio - Data Engineering**

Creado como demostración de habilidades en:
- 📊 Data Engineering
- 🔄 ETL Pipeline Design
- 🐳 DevOps & Containerization
- 📈 Analytics Engineering
- 🛠️ Modern Data Stack

**Contacto**:
- GitHub: [Tu perfil]
- LinkedIn: [Tu perfil]
- Email: [Tu email]

---

## 🙏 Agradecimientos

- CoinGecko por proporcionar una API gratuita y confiable
- Comunidad de Apache Airflow
- Comunidad de dbt
- Todos los que contribuyen al Modern Data Stack open-source

---

## ⭐ Star History

Si este proyecto te fue útil, considera darle una estrella en GitHub!

---

**¿Preguntas o problemas?** Abre un issue en GitHub o contacta directamente.

**Última actualización**: Enero 2025

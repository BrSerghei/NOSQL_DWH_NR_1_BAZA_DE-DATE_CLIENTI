# Retail Data Platform & Analytics Architecture (DWH & MongoDB & PySpark)

> **Author:** Serghei Brodovoi (Group SD-251)  
> **Project Type:** Synthesis Annual Project / Course Work  
> **Domain:** Retail Analytics, Data Warehousing, NoSQL Document Databases, Big Data Processing & Machine Learning  

---

## 📌 Project Overview

This comprehensive repository implements an end-to-end Data Engineering & Analytics ecosystem for a supermarket network retail domain ("Nr1"). The system covers relational Data Warehousing (MS SQL Server DWH), NoSQL Document-based storage (MongoDB Normalized & Denormalized), PySpark Distributed Big Data processing, Machine Learning & Linear Regression models, CRUD latency benchmarkings, and an interactive **Streamlit Dashboard**.

The project addresses the key challenges of retail data modeling, comparing **Relational Data Warehouses vs. NoSQL Document Stores**, analyzing B2C retail transactions alongside B2B wholesale contracts, evaluating query latencies, and delivering predictive analytics for business KPIs.

---

## 🏗 System Architecture & Workflow

```
                  +-----------------------------------+
                  |   MS SQL Server (DWH_NR1 Backup)  |
                  +-----------------+-----------------+
                                    |
              +---------------------+---------------------+
              |                                           |
              v                                           v
    +-------------------+                       +-------------------+
    | PySpark Processing|                       |  Python Connectors|
    | (PySpark & ML)    |                       | (SSMS <-> MongoDB)|
    +---------+---------+                       +---------+---------+
              |                                           |
              +---------------------+---------------------+
                                    |
                                    v
                  +-----------------------------------+
                  |         MongoDB Storage           |
                  |  (Normalized & Denormalized)      |
                  +-----------------+-----------------+
                                    |
                                    v
                  +-----------------------------------+
                  |   Interactive Streamlit Dashboard |
                  |   (KPIs, Regression, Latencies)   |
                  +-----------------------------------+
```

---

## 📂 Repository Structure

Below is the directory breakdown of the project components:

```
SD_251_BRODOVOI_SERGHEI_LUCRARE_DE_AN_SINTETIZAT/
├── INAINTE_DE_BIG_DATA_SI_SPARK/
│   ├── 1. COLECTII_MONGODB/           # Exported MongoDB collections (Normalized & Denormalized)
│   ├── 2. JSON_IMPORT/                # JSON import files for MongoDB initialization
│   └── 3. DWH_NR1_BAK/                # MS SQL Server database backup (DWH_NR1_Comun.bak)
│
├── INTEROGARI_SSMS/                   # T-SQL scripts executed in SSMS
│   ├── COMPARATIE_INTRE_MODELE.sql    # Comparative queries between schemas
│   ├── TOP_CLIENTI_B2C.sql             # Top B2C retail customers query
│   ├── TOP_COMPANII_B2B.sql            # Top B2B wholesale companies query
│   └── VERIFICAREA_KPI-ULUI_INTEGRAT.sql # Integrated KPI verification query
│
├── PYTHON_SCRIPTURI_PROIECTE/         # Modular Python scripts
│   ├── 1. PYTHON_CONVERT_ÎN_DENORMALIZAT/  # Script to convert normalized JSONs to denormalized document structure
│   ├── 2. PYTHON_SSMS_CONECTOR_MONGODB/   # Connector transferring DWH data directly to MongoDB
│   ├── 3. PYTHON_SPARK_BIG_DATA/          # PySpark script for distributed ETL and RFM segmentation
│   ├── 4. PYTHON_CSV_EXPORT/              # Automated CSV exporter from MongoDB / DWH
│   ├── 5. PYTHON_ML + GRAFICE/            # B2C & B2B KPI analytics and ML visualization scripts
│   ├── 6. PYTHON_REGRESIE_LINIARA.../     # Linear Regression pipeline across CSV, DWH, and MongoDB
│   ├── 7. PYTHON_CRUD_LATENTA.../         # Benchmark script measuring CRUD latency (Normalized vs Denormalized)
│   └── 8. PYTHON_CRUD/                    # Interactive Python CRUD CLI module for MongoDB
│
├── REGRESIE_LINIARA/                  # Linear Regression analytics artifacts
│   ├── EXCEL_REGRESIE_LINIARA/        # Excel modeling workbooks
│   ├── FISIERE_CSV_PENTRU_REGRESIE... # CSV datasets prepared for model fitting
│   ├── POWERBI_REGRESIE_LINIARA/      # Power BI dashboards and reports
│   └── VISUAL_STUDIO_2019.../         # SSAS / SSIS Data Mining project solution (Nr1_DM)
│
└── STREAMLIT/                         # Streamlit Interactive Web Application
    ├── app.py                         # Main application interface script
    ├── assets/                        # UI images and styling assets
    └── data/                          # Normalized and Denormalized cached JSON data
```

---

## ⚡ Core Features & Modules

### 1. Data Warehousing & NoSQL Modeling
* **MS SQL Server DWH (`DWH_NR1_Comun.bak`):** Relational schema storing customers, products, categories, brands, B2C orders, B2B wholesale contracts, store locations, and RFM segments.
* **MongoDB Normalized Collections:** Relational-style document collections (`clienti`, `comenzi`, `produse`, `companii`, `contracte_angro`, etc.).
* **MongoDB Denormalized Collections:** Embedded document structure optimized for single-query retrieval (`clienti_denormalizat`, `companii_denormalizat`, `produse_denormalizat`, `nr1_denormalizat_full`).

### 2. Big Data & PySpark ETL
* Distributed data processing with **Apache Spark (PySpark)** (`nr1_bigdata_spark.py`).
* Real-time aggregation of retail transactions, calculating integrated B2C customer lifetime values and B2B contract revenues.
* Automatic RFM (Recency, Frequency, Monetary) classification.

### 3. Machine Learning & Predictive Analytics
* **Linear Regression Pipelines:** Fits predictive models for sales forecasting, B2C spend prediction, and B2B wholesale order volumes.
* **Multi-source ML Execution:** Evaluates performance on raw CSV exports, direct SQL DWH queries, and NoSQL MongoDB pipelines.
* **Data Mining & BI Integration:** Complemented by Visual Studio Data Mining solutions (`Nr1_DM`) and Power BI visual dashboards.

### 4. Performance & Latency Benchmarking
* Automated CRUD benchmark module (`PYTHON_CRUD_LATENTA...`) comparing query response times between Normalized and Denormalized MongoDB schemas across Create, Read, Update, and Delete operations.

### 5. Interactive Web Dashboard (Streamlit)
* Streamlit web interface (`STREAMLIT/app.py`) presenting:
  * **Top B2C Retail Customers & Top B2B Companies**
  * **Integrated KPI Metrics**
  * **CRUD Operations & Schema Latency Visualizations**
  * **Interactive Linear Regression Visualizations & Predictions**

---

## 🚀 Installation & Setup

### Prerequisites
* **Python 3.10+**
* **MongoDB Community Server** (running on `localhost:27017` by default)
* **MS SQL Server 2019+** / SSMS (for restoring `.bak` file and running T-SQL queries)
* **Java JDK 8 or 11** (required for PySpark)

### Environment Setup

1. **Clone the Repository / Unpack Project:**
   ```bash
   git clone <repository-url>
   cd SD_251_BRODOVOI_SERGHEI_LUCRARE_DE_AN_SINTETIZAT
   ```

2. **Create and Activate Virtual Environment:**
   ```bash
   python -m venv .venv
   # On Windows:
   .venv\Scripts\activate
   # On macOS/Linux:
   source .venv/bin/activate
   ```

3. **Install Dependencies:**
   ```bash
   pip install pymongo pandas numpy matplotlib seaborn scikit-learn pyspark streamlit openpyxl pyodbc
   ```

---

## 💻 Usage Guide

### 1. Database Restoration & Data Ingestion
* **MS SQL Server:** Restore `INAINTE_DE_BIG_DATA_SI_SPARK/3. DWH_NR1_BAK/DWH_NR1_Comun.bak` in SQL Server Management Studio (SSMS).
* **Convert Normalized JSON to Denormalized MongoDB Documents:**
  ```bash
  python "PYTHON_SCRIPTURI_PROIECTE/1. PYTHON_CONVERT_ÎN_DENORMALIZAT/converter.py"
  ```
* **Load SQL DWH to MongoDB:**
  ```bash
  python "PYTHON_SCRIPTURI_PROIECTE/2. PYTHON_SSMS_CONECTOR_MONGODB/main.py"
  ```

### 2. PySpark Big Data Aggregation
Execute the PySpark ETL script for big data transformations:
```bash
python "PYTHON_SCRIPTURI_PROIECTE/3. PYTHON_SPARK_BIG_DATA/nr1_bigdata_spark.py"
```

### 3. Run Benchmark Latency Tests
Run the CRUD performance evaluation script:
```bash
python "PYTHON_SCRIPTURI_PROIECTE/7. PYTHON_CRUD_LATENTA_NORMALIZAT+DENORMALIZAT/main.py"
```

### 4. Execute Machine Learning & Linear Regression
Run the ML models for KPI analysis and regression plotting:
```bash
python "PYTHON_SCRIPTURI_PROIECTE/5. PYTHON_ML + GRAFICE/Y1_KPI_CLIENTI_B2C.py"
python "PYTHON_SCRIPTURI_PROIECTE/6. PYTHON_REGRESIE_LINIARA_CSV_DWH_MONGODB_GRAFICE/main.py"
```

### 5. Launch Streamlit Web Dashboard
Launch the interactive Streamlit application:
```bash
cd STREAMLIT
streamlit run app.py
```

---

## 📊 Summary of Key Queries & KPI Verification

| SSMS Script Name | Purpose / Focus Area |
| :--- | :--- |
| `TOP_CLIENTI_B2C.sql` | Ranks individual retail customers based on total spent, visit frequency, and discount card tier. |
| `TOP_COMPANII_B2B.sql` | Ranks B2B corporate partners based on total contract value and volume of wholesale orders. |
| `VERIFICAREA_KPI-ULUI_INTEGRAT.sql` | Computes cross-channel integrated KPIs (Retail B2C + Wholesale B2B revenue consolidation). |
| `COMPARATIE_INTRE_MODELE.sql` | Analyzes schema joins and performance execution plans between relational and NoSQL structures. |

---

## 🛠 Tech Stack

* **Database Systems:** MS SQL Server (DWH), MongoDB (NoSQL Document Database)
* **Big Data Framework:** Apache Spark (PySpark)
* **Programming & Libraries:** Python 3, PyMongo, PyODBC, Pandas, NumPy
* **Machine Learning & Analytics:** Scikit-Learn, Matplotlib, Seaborn
* **Business Intelligence & Reporting:** Power BI, Visual Studio SSAS/SSIS (`Nr1_DM`), Excel
* **Frontend Dashboard:** Streamlit

---

## 👤 Author Information

* **Developer:** Serghei Brodovoi
* **Group:** SD-251
* **Academic Context:** Course Work / Annual Synthesis Project (Lucrare de An Sintetizată)

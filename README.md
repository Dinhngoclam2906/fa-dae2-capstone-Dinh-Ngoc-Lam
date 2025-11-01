# Daily News Summarization Project 

## Overview
An end-to-end AI-powered data analytics system that processes real-time and historical news data from The Guardian to summarize and provide insights into news trends and sentiment. This enables users to receive updates from current and historical news trends and query insights using a natural language AI interface.

## Project Structure
### **1. Problem & Scope**:
The rapid pace of news publication requires real-time ingestion and analysis to stay relevant, while historical data provides context for long-term trends. Constraints include API rate limits, data quality issues, and ensuring AI responses are grounded in accurate data.

### **2. Personas/Stakeholders and Primary Use Cases:**  
- **Journalists:** Query news trends and sentiment for reporting.  
- **Analysts:** Analyze historical news patterns for research.  
- **General Users:** Ask natural language questions about news topics.  
- **Use Cases:** Real-time news monitoring, historical trend analysis, sentiment analysis via AI chatbot.

### **3. In/Out of Scope:**  
- **In Scope**: Real-time news ingestion, batch processing of historical news, AI-driven insights, data quality validation.  
- **Out of Scope**: Real-time social media integration, multi-language support, advanced ML model training.

### Data Sources
**1. Real-time Source:**  
- API Name: The Guardian API  
- Endpoint: https://content.guardianapis.com/search  
- Format: JSON  
- Volume: 500 latest articles
- Update Frequency: Hourly updates with new articles  
- Description: Provides live news articles with metadata (title, publication date, section, etc.).

**2. Batch Source:**
- Dataset Name: Hugging Face - "TheGuardian-Articles" (Stefan171)
- Format: Parquet shards (downloaded via huggingface_hub, preprocessed to CSV)  
- Volume: 500 articles (filtered to full data quality) 
- Update Cadence: Static dataset  
- Description: Historical news articles scraped from The Guardian (2010-2024) with fields like URL, category, publication date, title, contents, author, and data quality ('Full' or 'Partial').

### Architecture Overview:
**High-level Diagram:** Data flows from The Guardian API (real-time) and Hugging Face Dataset (batch) into PostgreSQL for staging (real-time only), then to Snowflake for warehousing. dbt transforms data into analytics-ready models, Kafka handles streaming, Airflow orchestrates pipelines, and a LangGraph-based AI chatbot with RAG queries the warehouse.

**Data Flow:**
- **Real-time:** The Guardian API → Python ingestion script → PostgreSQL (staging) → Snowflake (RAW schema).  
- **Batch:** Hugging Face Dataset → Python preprocessing script (BatchDataCollector: download, filter 'Full' quality, transform to schema-aligned CSV) → Python ingestion script → Snowflake (RAW schema).
- **Transformation:** dbt models (stg_*, dim_*, fct_*) in Snowflake ANALYTICS schema.  
- **Streaming:** Kafka processes real-time article updates.  

**Technology Choices:**
- **Python:** Flexible for data ingestion and processing (requests, polars).  
- **PostgreSQL:** Local staging for rapid development and testing.  
- **Snowflake:** Scalable cloud warehouse for analytics.  
- **dbt:** Industry-standard for data transformations.  
- **Kafka:** Robust for real-time streaming.  
- **Airflow:** Reliable pipeline orchestration.  
- **LangGraph/PineCone:** Enables conversational AI with grounded responses.  
- **GitHub Actions:** Simplifies CI/CD for deployment.  

**ERD:**
```dbml
Table raw_data {
  crawl_timestamp timestamp [pk]
  article_id string [pk, unique]
  web_publication_date timestamp
  web_title string
  body_text text
  web_url string
  section_name string
  loaded_at timestamp
  Note: 'Raw Guardian articles from Parquet ingestion'
}

Table stg_sf__guardian {
  crawl_timestamp timestamp [pk]
  article_id string [pk]
  web_publication_date timestamp
  web_title string
  body_text text
  web_url string
  section_name string
  loaded_at timestamp
  has_valid_title boolean
  has_valid_url boolean
  has_valid_content boolean
  has_valid_section boolean
  processed_at timestamp
  Note: 'Staging: Cleaned raw with quality flags'
}

Table dim_date {
  date_id date [pk]
  year integer
  month integer
  day integer
  day_of_week integer
  quarter string
  fiscal_quarter string
  is_weekend boolean
  is_holiday boolean
  holiday_name string
  dbt_updated_at timestamp
  Note: 'Date dimension with UK holidays (Guardian-focused)'
}

Table dim_sections {
  section_key string [pk, unique]
  section_name string
  section_group string
  dbt_updated_at timestamp
  Note: 'Section dimension with bucketing macro'
}

Table dim_articles {
  article_id string [pk]  // Composite PK with valid_from
  web_title string
  web_url string
  section_key string
  valid_from timestamp [pk]  // Composite PK with article_id
  valid_to timestamp
  is_current boolean
  version_surrogate_key string
  content_change_hash string
  change_type string
  crawl_timestamp timestamp
  dbt_updated_at timestamp
  Note: 'SCD Type 2: Versions on title/URL/section/body changes (PK: [article_id, valid_from])'
}

Table fct_articles {
  article_event_hk string [pk, unique]
  article_id string
  crawl_timestamp timestamp
  web_publication_date timestamp
  web_title string
  body_text text
  date_id date
  section_key string
  is_current boolean
  body_length_chars integer
  valid_content integer
  title_to_body_ratio float
  web_url string
  loaded_at timestamp
  dbt_updated_at timestamp
  Note: 'Fact: Article events with metrics (granular per crawl)'
}

// Relationships (star schema)
Ref: stg_sf__guardian.article_id > dim_articles.article_id  // Staging feeds SCD dim
Ref: stg_sf__guardian.section_name > dim_sections.section_key  // Normalized sections
Ref: fct_articles.date_id > dim_date.date_id  // Fact to date dim
Ref: fct_articles.section_key > dim_sections.section_key  // Fact to section dim
Ref: fct_articles.article_id > dim_articles.article_id  // Fact to article dim (current only)
Ref: dim_articles.section_key > dim_sections.section_key  // Article dim to section
Ref: "dim_date"."date_id" < "dim_date"."day"
```

**Star Schema:**
<image-card alt="Logo" src="Untitled.png" ></image-card>

**Project Structure:**
```
capstone/
├── .github/workflows
|   ├── pr_ci.yml
├── capstone_dbt_project/
|   ├── .dbt
|   ├── .venv
|   ├── dbt_packages
|   ├── logs
|   ├── macros
|   ├── models
|   ├── seeds
|   ├── snapshot
|   ├── target
|   ├── tests
|   ├── .gitignore/
|   ├── README.md
|   ├── expressions.txt
|   ├── pyproject.toml/
|   ├── .sqlfluff
|   ├── .user.yml
|   ├── .dbt_project.yml
|   ├── package-lock.yml
|   ├── packages.yml
|   ├── profiles.yml
├── scripts/  
|   ├── data_collection                               # Data collection scripts
|      ├── real_time_data_collector.py                # Guardian API ingestion
|      ├── batch_data_collector.py                    # HuggingFace historical news data ingestion
|   ├── ingestion
|      ├── load_parquet_to_postgre_then_snowflake.py  # Load local CSV to PostgreSQL
|      ├── load_parquet_to_snowflake.py               # Load local CSV to Snowflake
|   ├── sql
|      ├── init.sql
├── .env.example 
├── .gitattributes/                    
├── .gitignore/                    
├── .python-version/                          
├── docker-compose.yml/                      
├── main.py/                        
├── pyproject.toml/ 
├── uv.lock/                       
└── README.md                     # Documentation
```

**Setup Instructions:**

**1. Clone Repository:**
``` 
git clone https://github.com/Dinhngoclam2906/fa-dae2-capstone-Dinh-Ngoc-Lam/
```

**2. Install Dependencies:**  
```
uv sync
```

**3. Start Docker Services:**
```  
docker-compose -f docker/compose.yml up -d
```

**4. Run Pipelines:**

Real-time: 
```python scripts/data_collection/real_time_data_collector.py && python scripts/ingestion/load_parquet_to_postgres_then_snowflake.py```

Batch: 
```python scripts/data_collection/batch_data_collector && python scripts/ingestion/load_parquet_to_snowflake.py```

**5. Verify Data:** 
Check PostgreSQL and Snowflake for loaded data (500+ rows for batch, continuous updates for real-time).
# Daily News Summarization Project 

## Overview
An end-to-end AI-powered data analytics system that processes real-time and historical news data from The Guardian to summarize and provide insights into news trends and sentiment. This enables users to receive updates from current and historical news trends and query insights using a natural language AI interface.

**Success Metrics (Quantitative):**  
* Real-time pipeline processes 500+ articles daily with <5% error rate.  
* Batch pipeline processes 87,000+ historical articles with >95% data completeness.
* AI chatbot answers 90% of queries accurately with response time <3 seconds.  
* Data warehouse supports queries with <1 second latency for 95% of requests.

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
- Data Format: JSON  
- Update Frequency: Hourly updates with new articles  
- Description: Provides live news articles with metadata (title, publication date, section, etc.).

**2. Batch Source:**
- Dataset Name: Hugging Face - "TheGuardian-Articles" (Stefan171)
- Format: Parquet shards (downloaded via huggingface_hub, preprocessed to CSV)SV  
- Volume: ~87,000 articles (filtered to full data quality) 
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

**Project Structure:**
```
capstone/
├── scripts/  
|   ├──data_collection                            # Data collection scripts
|      ├── real_time_data_collector.py            # Guardian API ingestion
|      ├── batch_data_collector.py                # HuggingFace historical news data ingestion
|   ├──ingestion
|      ├── load_csv_to_postgre_then_snowflake.py  # Load local CSV to PostgreSQL
|      ├── load_csv_to_snowflake.py               # Load local CSV to Snowflake
|      ├── snowflake_objects_verification.py
|   ├──sql
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
```python scripts/data_collection/real_time_data_collector.py && python scripts/ingestion/load_csv_to_postgres.py && python scripts/ingestion/load_postgre_to_snowflake.py```

Batch: 
```python scripts/data_collection/batch_data_collector && python scripts/ingestion/load_csv_to_snowflake.py```

**5. Verify Data:** 
Check PostgreSQL and Snowflake for loaded data (500+ rows for batch, continuous updates for real-time).
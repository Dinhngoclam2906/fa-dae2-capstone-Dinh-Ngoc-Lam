# News Analytics System Project 

## Overview
An end-to-end AI-powered data analytics system that processes real-time and historical news data from The Guardian to provide insights into news trends and sentiment.
Business/Value Objective: Enable users to analyze current and historical news trends, identify sentiment patterns, and query insights using a natural language AI interface
Success Metrics (Quantitative):  
* Real-time pipeline processes 500+ articles daily with <5% error rate.  
* Batch pipeline processes 10,000+ historical articles with >95% data completeness.  
* AI chatbot answers 90% of queries accurately with response time <3 seconds.  
* Data warehouse supports queries with <1 second latency for 95% of requests.

## Project Structure
Problem & Scope
Problem Statement and Constraints:The rapid pace of news publication requires real-time ingestion and analysis to stay relevant, while historical data provides context for long-term trends. Constraints include API rate limits, data quality issues, and ensuring AI responses are grounded in accurate data.
Personas/Stakeholders and Primary Use Cases:  

Journalists: Query news trends and sentiment for reporting.  
Analysts: Analyze historical news patterns for research.  
General Users: Ask natural language questions about news topics.  
Use Cases: Real-time news monitoring, historical trend analysis, sentiment analysis via AI chatbot.

In/Out of Scope:  

In Scope: Real-time news ingestion, batch processing of historical news, AI-driven insights, data quality validation.  
Out of Scope: Real-time social media integration, multi-language support, advanced ML model training.

Data Sources
Real-time Source:  

API Name: The Guardian API  
Endpoint: https://content.guardianapis.com/search  
Data Format: JSON  
Update Frequency: Hourly updates with new articles  
Description: Provides live news articles with metadata (title, publication date, section, etc.).

Batch Source:  

Dataset Name: Kaggle - "News Articles of The Guardian (11/2015 - 11/2023)"  
Format: CSV  
Volume: ~10,000+ articles  
Update Cadence: Static dataset  
Description: Historical news articles with fields like title, body, publication date, and category.

Why 2 Different Sources:Real-time data from The Guardian API enables monitoring of current news trends, while the Kaggle dataset provides a rich historical context for analyzing long-term patterns and sentiment.
Architecture Overview
High-level Diagram:[Insert link to diagram or describe]: Data flows from The Guardian API (real-time) and Kaggle CSV (batch) into PostgreSQL for staging, then to Snowflake for warehousing. dbt transforms data into analytics-ready models, Kafka handles streaming, Airflow orchestrates pipelines, and a LangGraph-based AI chatbot with RAG queries the warehouse.
Data Flow:  

Real-time: The Guardian API → Python ingestion script → PostgreSQL (staging) → Snowflake (RAW schema).  
Batch: Kaggle CSV → Python ingestion script → Snowflake (RAW schema).  
Transformation: dbt models (stg_*, dim_*, fct_*) in Snowflake ANALYTICS schema.  
Streaming: Kafka processes real-time article updates.  
AI: LangGraph agent with PineCone RAG queries Snowflake for insights.

Technology Choices:  

Python: Flexible for data ingestion and processing (requests, polars).  
PostgreSQL: Local staging for rapid development and testing.  
Snowflake: Scalable cloud warehouse for analytics.  
dbt: Industry-standard for data transformations.  
Kafka: Robust for real-time streaming.  
Airflow: Reliable pipeline orchestration.  
LangGraph/PineCone: Enables conversational AI with grounded responses.  
GitHub Actions: Simplifies CI/CD for deployment.  
Justification: These tools align with modern data engineering practices, are widely adopted, and meet project scalability and flexibility needs.

Project Structure
capstone/
├── .env                          # Environment variables
├── docker/                       # Docker configs (PostgreSQL, Kafka, Airflow)
├── ingestion/                    # Data collection scripts
│   ├── collect_realtime.py       # Guardian API ingestion
│   ├── collect_batch.py          # Kaggle CSV ingestion
│   ├── load_to_postgres.py       # Load to PostgreSQL
│   ├── load_to_snowflake.py      # Load to Snowflake
├── postgres/                     # PostgreSQL schema definitions
├── snowflake/                    # Snowflake utilities and scripts
├── dbt/                          # dbt project for transformations
├── airflow/                      # Airflow DAGs and configs
├── kafka/                        # Kafka producer/consumer scripts
├── agent/                        # LangGraph AI chatbot with RAG
└── docs/                         # Documentation
    ├── DATASOURCE.md             # Data source details
    ├── execution_plan.md         # Implementation plan

Setup Instructions

Clone Repository:  
git clone https://github.com/[your-username]/capstone.git
cd capstone


Install Dependencies:  
uv sync


Set Up Environment:Copy .env.example to .env and fill in credentials (Guardian API key, Snowflake credentials, etc.).

Start Docker Services:  
docker-compose -f docker/compose.yml up -d


Run Pipelines:  

Real-time: python ingestion/collect_realtime.py && python ingestion/load_to_postgres.py  
Batch: python ingestion/collect_batch.py && python ingestion/load_to_snowflake.py


Verify Data:Check PostgreSQL and Snowflake for loaded data (500+ rows for batch, continuous updates for real-time).


Development Status

M01 W04 (Current):  

Repository setup with GitHub Actions for CI/CD.  
Real-time pipeline: Guardian API → PostgreSQL (row-by-row).  
Batch pipeline: Kaggle CSV → Snowflake (500+ rows).  
Documentation: README.md, execution_plan.md, DATASOURCE.md.  
Two branches created, one merged PR, 3+ commits.


Future Milestones:  

M02: dbt setup, dimensional modeling, data quality tests.  
M03: Kafka streaming, Airflow orchestration.  
M04: LangGraph AI agent with RAG.  
M05: Final integration, testing, and demo prep.



Notes

Data Quality: Ensuring >95% completeness, validated relationships between real-time and batch data.  
Testing: Using pytest for ingestion scripts, great-expectations for data validation.  
Monitoring: Prometheus/Grafana planned for M05 observability.  
Flexibility: Technologies (e.g., Kafka, Snowflake) can be swapped per Technology Alternatives guide.  
Contact: Reach out to trainers (il-dat, il-minh, nhatil, thu-IL) for consultation.

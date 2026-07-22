# Food Delivery Data Engineering for AI Systems — Capstone

**Students:** Noura, Moudi Alhomoud, Shahad  
**Program:** Modern Data Engineering for AI Systems — SDAIA Academy  
**Trainer:** Mohammed Albeladi  
**Session dates:** 19 July 2026 - 23 July 2026

---

## Project Overview

This project implements an end-to-end modern data engineering pipeline for a food delivery analytics platform.

The platform processes food delivery events, validates incoming records, stores trusted data using a Delta Lakehouse architecture, supports policy question answering through an advanced Retrieval-Augmented Generation pipeline, orchestrates all stages using Apache Airflow, and applies automated quality gates and lineage tracking.

The project integrates all five capstone deliverables into one working pipeline.

---

## Business Problem

Food delivery platforms receive data from restaurants, drivers, customers, orders, and delivery systems.

Malformed records, invalid values, missing fields, and inconsistent schemas can produce unreliable analytics and incorrect AI-generated answers.

This project addresses those problems through:

- schema validation at ingestion;
- quarantine routing for invalid records;
- Bronze, Silver, and Gold Delta Lake layers;
- real Delta MERGE operations;
- hybrid RAG retrieval;
- automated orchestration;
- data quality gates;
- end-to-end lineage events.

---

## Pipeline Architecture

```text
Kafka Producer
      │
      ▼
Kafka Consumer + Pydantic Validation
      │
      ├── Invalid records → Quarantine / Dead-letter path
      │
      ▼
Bronze Delta Layer
      │
      ▼
Silver Delta Layer + MERGE
      │
      ▼
Great Expectations Quality Gate
      │
      ├── Failure → Pipeline stops
      │
      ├── Success → Gold Analytics
      │
      └── Success → RAG Pipeline

Every pipeline stage emits OpenLineage START, COMPLETE, or FAIL events.

---

# Deliverable 1 — Kafka Ingestion

The ingestion layer uses Kafka to publish and consume food delivery records.

#Main features:

Kafka producer and consumer
Pydantic schema validation
Accepted and rejected record separation
Quarantine routing
Rejection reason recording
Demonstrated malformed-record failure path

Notebook:

notebooks/Deliverable1_Kafka_Ingestion.ipynb

#Deliverable 2 — Delta Lakehouse

The lakehouse uses Delta Lake with three layers:

#Bronze

Stores raw accepted records in append-only form.

#Silver

Cleans and standardizes Bronze records.

A real Delta MERGE performs updates and inserts using a business key.

#Gold

Produces genuine analytical aggregates rather than copying Silver data.

The notebook also demonstrates schema enforcement by intentionally attempting an invalid write.

Notebook:

notebooks/Deliverable2_Delta_Lakehouse.ipynb

#Deliverable 3 — Advanced RAG Pipeline

The RAG pipeline answers questions about food delivery policies.

It includes:

document chunking;
Sentence Transformer embeddings;
ChromaDB vector storage;
dense semantic retrieval;
BM25 keyword retrieval;
Reciprocal Rank Fusion;
Cross-Encoder reranking;
grounded answer generation;
source citations.

Notebook:

notebooks/Deliverable3_RAG_Pipeline.ipynb

#Deliverable 4 — Airflow Orchestration

Apache Airflow connects all pipeline stages with explicit task dependencies.

The quality gate is placed before downstream publishing tasks. When the gate fails, Gold and RAG tasks do not run.

DAG:

dags/food_delivery_pipeline_dag.py

Notebook:

notebooks/Deliverable4_Airflow_Orchestration.ipynb

#Deliverable 5 — Quality Gate and Lineage

Great Expectations validates the trusted Silver data.

Example checks include:

required columns are non-null;
business keys are unique;
quantities and monetary values are valid;
categorical values follow expected rules.

The pipeline emits OpenLineage events for every stage:

START
COMPLETE
FAIL

Notebook:

notebooks/Deliverable5_Quality_Lineage.ipynb

---

#Technologies Used :
Python
Apache Kafka
Pydantic
PySpark
Delta Lake
ChromaDB
Sentence Transformers
BM25
Cross-Encoder reranking
Hugging Face Transformers
Apache Airflow
Great Expectations
OpenLineage
Google Colab

---

Repository Structure
├── notebooks/
│   ├── Deliverable1_Kafka_Ingestion.ipynb
│   ├── Deliverable2_Delta_Lakehouse.ipynb
│   ├── Deliverable3_RAG_Pipeline.ipynb
│   ├── Deliverable4_Airflow_Orchestration.ipynb
│   └── Deliverable5_Quality_Lineage.ipynb
├── dags/
│   └── food_delivery_pipeline_dag.py
├── src/
├── docs/
│   └── ARCHITECTURE.md
├── README.md
├── requirements.txt
└── .gitignore


#Installation

Clone the repository:

git clone https://github.com/YOUR-USERNAME/food-delivery-data-engineering-capstone.git
cd food-delivery-data-engineering-capstone

Install dependencies:

pip install -r requirements.txt

The notebooks may also be opened directly in Google Colab.


#How to Run

Run the deliverables in this order:

- Kafka ingestion
- Delta Lakehouse
- RAG pipeline
- Airflow orchestration
- Quality gate and lineage

Each notebook contains setup cells and captured execution output.

#Expected Results

A successful project run demonstrates:

valid records accepted by Kafka;
invalid records quarantined with reasons;
Bronze data written to Delta;
Silver data updated using a real MERGE;
invalid schema write rejected;
Gold analytical aggregate created;
RAG answers generated from retrieved context with citations;
Airflow task dependencies displayed;
Great Expectations quality checks executed;
OpenLineage events emitted;
pipeline stopped when the quality gate fails.


#Documentation

Detailed architecture documentation is available in:

docs/ARCHITECTURE.md


#Training Attribution

This project was completed as part of the Modern Data Engineering for AI Systems training program delivered by SDAIA Academy.

SDAIA Academy GitHub:

https://github.com/SDAIAAcademy

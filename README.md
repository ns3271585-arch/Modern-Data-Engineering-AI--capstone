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



https://github.com/SDAIAAcademy

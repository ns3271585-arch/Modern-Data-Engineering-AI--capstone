# Food Delivery Data Engineering for AI Systems — Capstone

**Students:** Noura Almuqbil, Moudi Alhomoud, Shahad Alotaish

**Program:** Modern Data Engineering for AI Systems — SDAIA Academy  
**Trainer:** Mohammed Albeladi  
**Session dates:** 19 July 2026 – 23 July 2026  

---

## Project Overview

This capstone implements an end-to-end modern data engineering workflow for a simulated food delivery analytics platform.

The project begins with real-time delivery-event ingestion through Apache Kafka. Incoming messages are validated against a Pydantic data contract before they are accepted. Records that violate the contract are rejected and routed to a quarantine path with the reason for rejection preserved.

Accepted events are processed through a Delta Lakehouse using Bronze, Silver, and Gold layers. Bronze preserves accepted raw records, Silver cleans and standardizes the data, and a real Delta `MERGE` performs inserts and updates using `delivery_id` as the business key. Gold produces analytical aggregates by city zone.

The project also includes an advanced Retrieval-Augmented Generation pipeline for answering questions from a food-delivery policy knowledge base. It combines dense vector search, BM25 keyword retrieval, Reciprocal Rank Fusion, Cross-Encoder reranking, grounded generation, and source citations.

Apache Airflow orchestrates the quality-controlled analytics workflow. Great Expectations validates Silver data before Gold is built, while OpenLineage emits execution events for successful and failed stages.

---

## Business Problem

Food delivery platforms receive operational data from restaurants, customers, drivers, and order-management systems. This data may contain malformed messages, missing identifiers, invalid monetary values, inconsistent capitalization, duplicated business keys, or unsupported categorical values.

Without validation and governance, these issues can:

- corrupt operational reports and revenue calculations;
- allow invalid records to enter trusted analytical tables;
- produce unreliable AI-generated answers;
- make pipeline failures difficult to trace;
- prevent teams from understanding where data originated or why it was rejected.

This project addresses those risks by enforcing validation at ingestion, maintaining trusted Delta layers, placing a quality gate before downstream analytics, grounding AI answers in retrieved evidence, and recording lineage events.

---

## Project Scope

### Included

- Kafka producer and consumer
- Pydantic validation at the ingestion boundary
- Accepted-record and quarantine paths
- Bronze, Silver, and Gold Delta Lake layers
- Delta schema enforcement
- Delta `MERGE` upsert using a business key
- Gold analytical aggregation
- PDF-based policy knowledge base
- Sentence-level overlapping chunking
- Sentence Transformer embeddings
- ChromaDB vector storage
- Dense retrieval and BM25 keyword retrieval
- Reciprocal Rank Fusion
- Cross-Encoder reranking
- FLAN-T5 grounded answer generation
- Source citations and retrieval evaluation
- Apache Airflow orchestration
- Great Expectations quality gate
- Happy-path and failure-path orchestration tests
- OpenLineage `START`, `COMPLETE`, and `FAIL` events

### Outside the Current Scope

- Production cloud deployment
- Managed Kafka or distributed Spark clusters
- A permanent hosted vector database
- A production Marquez/OpenLineage server
- A public API or web user interface
- Continuous integration and container deployment

The capstone focuses on demonstrating the required data-engineering concepts using executable Google Colab notebooks and real open-source libraries.

---

## System Architecture

```text
Food Delivery Events
        │
        ▼
Apache Kafka Producer
        │
        ▼
Apache Kafka Consumer
        │
        ▼
Pydantic Data Contract
   ┌────┴───────────────┐
   │                    │
Valid Records      Invalid Records
   │                + rejection reason
   │                    │
   │                    ▼
   │               Quarantine Path
   ▼
Bronze Delta Layer
        │
        ▼
Silver Delta Layer
Cleaning + Standardization
        │
        ▼
Delta MERGE on delivery_id
        │
        ▼
Great Expectations Quality Gate
   ┌────┴─────────────────┐
   │                      │
 PASS                    FAIL
   │                      │
   ▼                      ▼
Gold Aggregate       Pipeline stops
        │
        └──────────────► Analytics output

Food Delivery Policy PDF
        │
        ▼
Overlapping Document Chunks
        │
        ├──► Dense Embeddings ──► ChromaDB
        │
        └──► BM25 Keyword Index
                       │
                       ▼
            Reciprocal Rank Fusion
                       │
                       ▼
             Cross-Encoder Reranking
                       │
                       ▼
          Grounded FLAN-T5 Answer
                 + Source Citations

Apache Airflow coordinates:
load_silver → quality_gate → build_gold → emit_lineage

OpenLineage records stage execution states.
```

---

## Deliverables

### Deliverable 1 — Kafka Ingestion

The ingestion notebook demonstrates a real Kafka producer and consumer using `kafka-python`.

Key features:

- real Kafka broker interaction;
- JSON event production;
- Pydantic validation;
- accepted and rejected record separation;
- quarantine routing;
- rejection reasons;
- deliberately malformed records to prove the failure path.

**Notebook:** `notebooks/Deliverable1_Kafka_Ingestion.ipynb`

### Deliverable 2 — Delta Lakehouse

The lakehouse notebook implements:

- Bronze raw Delta storage;
- Silver cleaning and standardization;
- a real Delta `MERGE` keyed by `delivery_id`;
- matched-row updates and new-row inserts;
- demonstrated schema enforcement;
- Gold aggregation by city zone.

**Notebook:** `notebooks/Deliverable2_Delta_Lakehouse.ipynb`

### Deliverable 3 — Advanced RAG Pipeline

The RAG notebook reads a food-delivery policy PDF and implements:

- overlapping sentence chunking;
- `all-MiniLM-L6-v2` embeddings;
- ChromaDB vector storage;
- dense semantic search;
- BM25 retrieval;
- Reciprocal Rank Fusion;
- `cross-encoder/ms-marco-MiniLM-L-6-v2` reranking;
- FLAN-T5 answer generation;
- grounded prompts and source labels;
- multiple evaluation questions and retrieval metrics.

**Notebook:** `notebooks/Deliverable3_RAG_Pipeline.ipynb`

### Deliverable 4 — Airflow Orchestration

The Airflow DAG contains four real tasks:

```text
load_silver → quality_gate → build_gold → emit_lineage
```

The quality gate raises an exception when invalid Silver data is detected. Airflow's default `all_success` trigger rule prevents `build_gold` and `emit_lineage` from running after a failed gate.

The notebook demonstrates:

- a successful clean-data run;
- an intentional bad-data run;
- downstream tasks becoming `upstream_failed`;
- lineage-event verification.

**Notebook:** `notebooks/Deliverable4_Airflow_Orchestration.ipynb`  
**DAG:** `dags/restaurant_delivery_pipeline.py`

### Deliverable 5 — Quality Gate and Lineage

The quality and lineage notebook:

- loads the persisted Silver Delta table;
- applies Great Expectations checks for completeness, uniqueness, numerical validity, and categorical validity;
- raises `QualityGateFailed` when validation fails;
- demonstrates both passing and failing batches;
- emits real OpenLineage events using the file transport;
- verifies the presence of `START`, `COMPLETE`, and `FAIL`.

**Notebook:** `notebooks/Deliverable5_Quality_Gate_Lineage.ipynb`

---

## Technologies Used

### Data Engineering

- Python
- Apache Kafka
- PySpark
- Delta Lake
- Apache Airflow

### Validation and Governance

- Pydantic
- Great Expectations
- OpenLineage

### Retrieval and AI

- ChromaDB
- Sentence Transformers
- BM25
- Reciprocal Rank Fusion
- Cross-Encoder reranking
- Hugging Face Transformers
- FLAN-T5

### Development Environment

- Google Colab
- GitHub

---

## Repository Structure

```text
food-delivery-data-engineering-capstone/
├── notebooks/
│   ├── Deliverable1_Kafka_Ingestion.ipynb
│   ├── Deliverable2_Delta_Lakehouse.ipynb
│   ├── Deliverable3_RAG_Pipeline.ipynb
│   ├── Deliverable4_Airflow_Orchestration.ipynb
│   └── Deliverable5_Quality_Gate_Lineage.ipynb
├── dags/
│   └── restaurant_delivery_pipeline.py
├── docs/
│   ├── ARCHITECTURE.md
│   └── RUBRIC_CHECKLIST.md
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Prerequisites

- Python 3.10 or later
- Java runtime compatible with PySpark
- Google Colab or a local Jupyter environment
- Internet access for downloading packages and machine-learning models
- The food-delivery knowledge-base PDF for Deliverable 3

Airflow installation in Colab uses version-specific constraints inside the notebook. For the most reliable demonstration, run each notebook using its own setup cells.

---

## Installation

Clone the repository:

```bash
git clone https://github.com/YOUR-USERNAME/Modern-Data-Engineering-AI--capstone.git
cd Modern-Data-Engineering-AI--capstone
```

Install the common dependencies:

```bash
pip install -r requirements.txt
```

The notebooks also include their own installation cells because Kafka, Delta Lake, Airflow, and transformer models have environment-specific setup requirements.

---

## How to Run

Run the notebooks in numerical order:

1. `Deliverable1_Kafka_Ingestion.ipynb`
2. `Deliverable2_Delta_Lakehouse.ipynb`
3. `Deliverable3_RAG_Pipeline.ipynb`
4. `Deliverable4_Airflow_Orchestration.ipynb`
5. `Deliverable5_Quality_Gate_Lineage.ipynb`

### Deliverable 3 Input

Upload the food-delivery policy PDF to Colab using the filename expected by the notebook:

```text
/content/food delivery knowledge base.pdf
```

### Deliverable 5 Dependency

Deliverable 5 reads:

```text
./delivery_data/silver
```

The Silver Delta table must exist in the same runtime or be copied to the expected path before running the quality checks. Deliverable 4 independently builds its own Bronze, Silver, and Gold data for the orchestration demonstration.

---

## Expected Results

A complete execution demonstrates:

- valid Kafka messages accepted;
- malformed Kafka messages quarantined with rejection reasons;
- Bronze, Silver, and Gold Delta tables created;
- a Delta `MERGE` updating and inserting records;
- an invalid schema write being rejected;
- genuine Gold business aggregates;
- RAG retrieval using dense and keyword search;
- RRF fusion and Cross-Encoder reranking;
- grounded answers accompanied by sources;
- a successful Airflow run;
- a failed quality gate blocking downstream tasks;
- Great Expectations validation results;
- OpenLineage `START`, `COMPLETE`, and `FAIL` events.

---

## Evidence

The notebooks retain captured outputs from real runs wherever available. These outputs demonstrate successful stages and intentional failure paths rather than presenting code alone.

Before final submission, run every notebook from top to bottom and save the resulting outputs.

---

## Configuration and Generated Files

Generated runtime data is intentionally excluded through `.gitignore`, including:

- Kafka and ZooKeeper runtime files;
- Spark warehouses and local Delta tables;
- Airflow databases and logs;
- OpenLineage JSON logs;
- Great Expectations temporary files;
- vector-store caches;
- quarantine and output data;
- secrets and environment files.

---

## Limitations

- Colab runtimes are temporary and separate notebooks do not automatically share `/content`.
- The Delta tables are stored locally during notebook demonstrations.
- The RAG vector store is recreated in memory after a runtime restart.
- FLAN-T5 is a lightweight local model and may not always follow citation instructions perfectly.
- OpenLineage events are written to files instead of a hosted lineage server.
- Airflow is demonstrated locally rather than through a long-running scheduler and web server.

---

## Future Improvements

- Persist Delta tables in cloud object storage
- Deploy Kafka and Spark in distributed environments
- Use a persistent ChromaDB service
- Add a Marquez server for lineage visualization
- Package stages as reusable Python modules
- Add Docker and Docker Compose
- Add automated tests and GitHub Actions
- Deploy the RAG pipeline behind an API
- Add operational dashboards and alerting

---

## Training Attribution

This project was completed as part of the **Modern Data Engineering for AI Systems** program delivered by **SDAIA Academy** from **19 July 2026 to 23 July 2026**, under trainer **Mohammed Albeladi**.

SDAIA Academy GitHub:  
https://github.com/SDAIAAcademy

---


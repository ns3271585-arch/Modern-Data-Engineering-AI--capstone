# Architecture Documentation

## 1. Purpose

This document describes the architecture of the Food Delivery Data Engineering for AI Systems capstone. The project demonstrates how ingestion, lakehouse processing, retrieval-augmented generation, orchestration, quality control, and lineage can be combined using real open-source libraries.

## 2. Architectural Principles

The implementation follows five principles:

1. **Reject invalid records early.** Pydantic validation is applied before records enter trusted storage.
2. **Separate raw, trusted, and analytical data.** Delta Lake Bronze, Silver, and Gold layers have distinct responsibilities.
3. **Use complementary retrieval methods.** Dense semantic retrieval and BM25 keyword matching are fused before reranking.
4. **Make quality gates load-bearing.** A failed Great Expectations checkpoint raises an exception and blocks downstream work.
5. **Record execution state.** OpenLineage events capture stage start, completion, and failure.

## 3. Ingestion Architecture

### Producer

The Kafka producer publishes food-delivery events as JSON messages. Test data includes valid and intentionally malformed records.

### Consumer

The consumer reads messages and validates each one using a Pydantic model.

### Accepted Path

Contract-valid events are retained for downstream processing.

### Quarantine Path

Invalid events are written separately with their rejection reason. This makes failures traceable and supports later correction or replay.

## 4. Delta Lakehouse Architecture

### Bronze Layer

Bronze stores accepted delivery events with minimal transformation.

### Silver Layer

Silver:

- trims text fields;
- standardizes city-zone capitalization;
- standardizes delivery-status values;
- supports reliable downstream validation.

### MERGE

A Delta `MERGE` uses `delivery_id` as the business key.

- Existing matching records are updated.
- New keys are inserted.
- The transaction is atomic.

### Schema Enforcement

The notebook intentionally attempts an invalid write containing an undeclared column. Delta rejects the incompatible schema.

### Gold Layer

Gold is a real aggregate grouped by city zone. It includes delivery counts, total order values, and average delivery times.

## 5. RAG Architecture

### Document Loading

A food-delivery policy PDF is extracted with PyPDF.

### Chunking

Text is split into overlapping sentence windows to preserve context across boundaries.

### Dense Index

`all-MiniLM-L6-v2` produces semantic embeddings stored in ChromaDB.

### Sparse Index

BM25 provides lexical matching for exact policy wording, values, and terminology.

### Fusion

Reciprocal Rank Fusion combines the dense and sparse ranked lists without adding incompatible raw scores.

### Reranking

A Cross-Encoder jointly scores each query–chunk pair and returns the strongest final evidence.

### Grounded Generation

FLAN-T5 receives only the selected context. Evidence blocks are numbered so the generated answer and displayed source list are traceable.

## 6. Airflow Architecture

The DAG is:

```text
load_silver → quality_gate → build_gold → emit_lineage
```

### `load_silver`

Creates Bronze and Silver Delta data and can optionally inject one invalid record.

### `quality_gate`

Loads Silver and runs a Great Expectations checkpoint.

### `build_gold`

Runs only when the quality gate succeeds.

### `emit_lineage`

Emits final lineage information after the analytical path succeeds.

With Airflow's default `all_success` trigger rule, a failed quality gate marks downstream tasks as `upstream_failed`.

## 7. Quality Gate

The quality suite validates:

- non-null delivery identifiers;
- unique delivery identifiers;
- non-null customer identifiers;
- positive order amounts;
- positive delivery durations;
- approved city zones;
- approved delivery statuses.

The gate raises `QualityGateFailed` if the checkpoint does not succeed.

## 8. OpenLineage

The project uses `openlineage-python` and file transport.

Each demonstrated stage emits:

- `START`
- `COMPLETE`
- `FAIL`

The event contains a job name, run ID, event timestamp, state, and dataset references where applicable.

## 9. Failure Paths

### Ingestion Failure

Malformed Kafka records are quarantined and include rejection reasons.

### Schema Failure

Delta rejects the intentionally incompatible write.

### Quality Failure

Invalid Silver data causes Great Expectations failure and raises an exception.

### Orchestration Failure

A failed quality-gate task blocks Gold and lineage-summary tasks.

### Lineage Failure Evidence

A `FAIL` event is emitted before the original exception is propagated.

## 10. Data and Runtime Paths

Common local paths include:

```text
./delivery_data/bronze
./delivery_data/silver
./delivery_data/gold
/content/delivery_data/bronze
/content/delivery_data/silver
/content/delivery_data/gold
/content/openlineage_events.jsonl
```

The exact path depends on the notebook. Colab notebooks run in separate temporary runtimes, so local files are not automatically shared between different notebook tabs.

## 11. Security and Repository Hygiene

The repository excludes:

- credentials and `.env` files;
- Kafka and Airflow runtime data;
- Delta tables and Spark warehouses;
- generated lineage logs;
- model and vector-store caches;
- quarantine and intermediate outputs.

## 12. Production Evolution

A production implementation would replace local paths with cloud object storage, use managed Kafka, deploy Airflow as a persistent service, connect OpenLineage to Marquez, store vector embeddings persistently, and expose the RAG workflow through a secure API.

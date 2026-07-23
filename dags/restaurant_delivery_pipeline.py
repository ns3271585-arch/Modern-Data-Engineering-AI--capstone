"""
Restaurant Delivery Lakehouse — orchestration DAG.

Tasks: load_silver -> quality_gate -> build_gold -> emit_lineage

quality_gate raises on failure, so Airflow's default trigger_rule ("all_success") makes
build_gold and emit_lineage upstream_failed automatically whenever the gate fails.
"""
import json
import os
import shutil
import uuid
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

BRONZE_PATH = "/content/delivery_data/bronze"
SILVER_PATH = "/content/delivery_data/silver"
GOLD_PATH = "/content/delivery_data/gold"
LINEAGE_LOG_PATH = "/content/openlineage_events.jsonl"


# ---------------------------------------------------------------------------
# OpenLineage helper — same OpenLineageClient / FileTransport / RunEvent calls
# verified working in Day 4 Lab's emit_real_openlineage_events(), reused here
# per-stage instead of once per pipeline run.
# ---------------------------------------------------------------------------
def _emit(run_id: str, job_name: str, state: str, detail: str = None):
    from openlineage.client import OpenLineageClient
    from openlineage.client.transport.file import FileConfig, FileTransport
    from openlineage.client.event_v2 import RunEvent, RunState, Run, Job

    os.makedirs(os.path.dirname(LINEAGE_LOG_PATH), exist_ok=True)
    transport = FileTransport(FileConfig(log_file_path=LINEAGE_LOG_PATH))
    client = OpenLineageClient(transport=transport)

    job = Job(namespace="restaurant_delivery_pipeline", name=job_name)
    # OpenLineage requires runId to be a real UUID. Airflow's own run_id
    # ("manual__2026-01-01T00:00:00+00:00") is not one, so derive a stable
    # UUID5 from it -- deterministic per DAG run, and still groupable in
    # the verification step below.
    lineage_run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, run_id))
    run = Run(runId=lineage_run_id)
    now = datetime.now(timezone.utc).isoformat()

    client.emit(RunEvent(
        eventType=getattr(RunState, state),
        eventTime=now,
        run=run,
        job=job,
        producer="https://github.com/sdaia/modern-data-engineering-lab",
    ))
    suffix = f" -> {detail}" if detail else ""
    print(f"[OpenLineage] {state:8s} {job_name}{suffix}")


# ---------------------------------------------------------------------------
# Task 1 — load_silver: build Bronze, clean into Silver
# ---------------------------------------------------------------------------
def load_silver(**context):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType, IntegerType, StructField, StructType, StringType,
    )

    run_id = context["run_id"]
    inject_bad_data = (context["dag_run"].conf or {}).get("inject_bad_data", False)
    _emit(run_id, "load_silver", "START")

    if os.path.exists("/content/delivery_data"):
        shutil.rmtree("/content/delivery_data")

    builder = (
        SparkSession.builder
        .appName("RestaurantDeliveryLakehouse")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("delivery_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("restaurant_name", StringType(), nullable=True),
        StructField("order_amount", DoubleType(), nullable=True),
        StructField("delivery_minutes", IntegerType(), nullable=True),
        StructField("city_zone", StringType(), nullable=True),
        StructField("delivery_status", StringType(), nullable=True),
    ])

    batch = [
        ("DEL101", "CUS101", "Najd Kitchen", 86.50, 28, " North ", "COMPLETED"),
        ("DEL102", "CUS102", "Burger House", 42.00, 18, "East", "Completed"),
        ("DEL103", "CUS103", "Sushi Corner", 110.25, 35, "West", "PREPARING"),
        ("DEL104", "CUS104", "Pasta Villa", 63.75, 25, "South", "Completed"),
    ]
    bronze_df = spark.createDataFrame(batch, schema)
    bronze_df.write.format("delta").mode("overwrite").save(BRONZE_PATH)

    silver_df = (
        spark.read.format("delta").load(BRONZE_PATH)
        .withColumn("restaurant_name", F.trim(F.col("restaurant_name")))
        .withColumn("city_zone", F.upper(F.trim(F.col("city_zone"))))
        .withColumn("delivery_status", F.lower(F.trim(F.col("delivery_status"))))
    )

    if inject_bad_data:
        # Deliberately invalid row: negative order_amount, blank city_zone.
        bad_row = spark.createDataFrame(
            [("DEL999", "CUS999", "Broken Kitchen", -40.0, 20, "", "completed")],
            schema,
        ).withColumn("restaurant_name", F.trim(F.col("restaurant_name")))
        silver_df = silver_df.unionByName(bad_row)

    silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)

    print("Silver table loaded:")
    spark.read.format("delta").load(SILVER_PATH).show(truncate=False)
    spark.stop()

    _emit(run_id, "load_silver", "COMPLETE")


# ---------------------------------------------------------------------------
# Task 2 — quality_gate: real Great Expectations checkpoint
#
# Same Checkpoint / ValidationDefinition / checkpoint.run() calls verified
# working in Day 4 Lab's run_great_expectations_checkpoint(), pointed at the
# Silver Delta table's columns instead of the retail CSV's columns.
# ---------------------------------------------------------------------------
def quality_gate(**context):
    import glob
    import pandas as pd
    import great_expectations as gx
    import great_expectations.expectations as gxe

    run_id = context["run_id"]
    _emit(run_id, "quality_gate", "START")

    parquet_files = glob.glob(f"{SILVER_PATH}/*.parquet")
    silver_pdf = pd.concat([pd.read_parquet(p) for p in parquet_files], ignore_index=True)

    gx_context = gx.get_context(mode="ephemeral")
    data_source = gx_context.data_sources.add_pandas("silver_layer")
    data_asset = data_source.add_dataframe_asset(name="restaurant_delivery_silver")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("silver_batch")

    suite = gx_context.suites.add(gx.ExpectationSuite(name="silver_layer_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="delivery_id"))
    suite.add_expectation(gxe.ExpectColumnValueLengthsToBeBetween(column="city_zone", min_value=1))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="order_amount", min_value=0.0001)
    )

    validation_definition = gx_context.validation_definitions.add(
        gx.ValidationDefinition(
            name="silver_layer_validation",
            data=batch_definition,
            suite=suite,
        )
    )
    checkpoint = gx_context.checkpoints.add(
        gx.Checkpoint(
            name="silver_layer_checkpoint",
            validation_definitions=[validation_definition],
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": silver_pdf})

    print(f"[GX] Real Great Expectations checkpoint success={result.success}")
    failed_expectations = []
    for run_result in result.run_results.values():
        for r in run_result["results"]:
            status = "PASSED" if r["success"] else "FAILED"
            print(f"  [GX] {status} {r['expectation_config']['type']}")
            if not r["success"]:
                failed_expectations.append(r["expectation_config"]["type"])

    if not result.success:
        detail = f"{len(failed_expectations)} expectation(s) failed: {failed_expectations}"
        _emit(run_id, "quality_gate", "FAIL", detail=detail)
        raise ValueError(f"Quality Gate FAILED: {detail}")

    print("Quality Gate PASSED.")
    _emit(run_id, "quality_gate", "COMPLETE")


# ---------------------------------------------------------------------------
# Task 3 — build_gold: real aggregate (only runs if quality_gate passed)
# ---------------------------------------------------------------------------
def build_gold(**context):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    run_id = context["run_id"]
    _emit(run_id, "build_gold", "START")

    builder = (
        SparkSession.builder
        .appName("RestaurantDeliveryLakehouse")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    gold_df = (
        spark.read.format("delta").load(SILVER_PATH)
        .groupBy("city_zone")
        .agg(
            F.count("delivery_id").alias("total_deliveries"),
            F.round(F.sum("order_amount"), 2).alias("total_order_value"),
            F.round(F.avg("delivery_minutes"), 2).alias("average_delivery_minutes"),
        )
        .orderBy("city_zone")
    )
    gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

    print("Gold layer created:")
    spark.read.format("delta").load(GOLD_PATH).show(truncate=False)
    spark.stop()

    _emit(run_id, "build_gold", "COMPLETE")


# ---------------------------------------------------------------------------
# Task 4 — emit_lineage: pipeline-level summary event (only if build_gold ran)
# ---------------------------------------------------------------------------
def emit_lineage(**context):
    run_id = context["run_id"]
    _emit(run_id, "emit_lineage", "START")
    print("Emitting OpenLineage COMPLETE event for the full pipeline run...")
    _emit(run_id, "restaurant_delivery_pipeline", "COMPLETE")
    _emit(run_id, "emit_lineage", "COMPLETE")


with DAG(
    dag_id="restaurant_delivery_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    description="Restaurant Delivery Lakehouse Pipeline",
) as dag:

    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver,
    )

    quality_gate_task = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
    )

    build_gold_task = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold,
    )

    lineage_task = PythonOperator(
        task_id="emit_lineage",
        python_callable=emit_lineage,
    )

    load_silver_task >> quality_gate_task >> build_gold_task >> lineage_task

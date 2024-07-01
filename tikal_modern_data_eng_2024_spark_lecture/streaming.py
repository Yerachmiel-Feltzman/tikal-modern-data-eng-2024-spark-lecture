import logging
from multiprocessing import Array
from pathlib import Path
from typing import List

import pyspark
import pyspark.sql.functions as F
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, IntegerType, \
    BooleanType

from tikal_modern_data_eng_2024_spark_lecture import batch

logger = logging.getLogger(__name__)


def read_from_socket(spark: SparkSession) -> DataFrame:
    # nc -lk 9999
    source_df = (
        spark
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
    )

    return source_df


def write_to_console(result_df) -> StreamingQuery:
    query = (
        result_df
        .writeStream
        .format("console")
        .start()
    )

    return query


def stream_from_socket_to_console(spark: SparkSession) -> StreamingQuery:
    source = read_from_socket(spark)
    return write_to_console(source)


def parse_transactions_cdc(cdc: DataFrame) -> DataFrame:
    # {"customer":"David","date":"2020-10-10","price":200}
    # op: "u|i|d
    # fullDoc
    # pk

    # TODO: make test `test_parse_transactions_cdc` pass

    dedup_prep = cdc.withColumn(
        "rank",
        F.rank().over(Window.partitionBy("pk").orderBy(F.desc("offset")))
    )

    result = (
        dedup_prep
        .withColumn("is_deleted", cdc.op == "d")
        .select(
            "fullDoc.customer",
            "fullDoc.date",
            "fullDoc.price",
            "is_deleted",
            "pk"
        )
    )

    return result


def write_incremental_state(spark: SparkSession, incremental_state: DataFrame, path_to_table: Path):
    # TODO: make test `test_write_cdc` pass

    path_to_table.mkdir(exist_ok=True)

    if DeltaTable.isDeltaTable(spark, path_to_table.__str__()):
        (
            DeltaTable
            .forPath(spark, path_to_table.__str__())
            .alias("old")
            .merge(
                incremental_state.alias("new"),
                "new.pk = old.pk"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )




def stream_transactions_cdc(spark: SparkSession) -> StreamingQuery:
    cdc = read_transactions_cdc_from_socket(spark)

    def batch_processing(df, epoch_id):
        parsed_cdc = parse_transactions_cdc(df)

        # logic replicated from batch:
        enriched = batch.enrich_transactions(parsed_cdc)
        batch.collect_monitoring_metrics(enriched)
        ####

        write_incremental_state(spark, enriched, path_to_table=Path("../data/output/transactions_cdc"))

    query = (
        cdc
        .writeStream
        .foreachBatch(batch_processing)
        .start()
    )

    return query


def read_transactions_cdc_from_socket(spark):
    socket = read_from_socket(spark)
    schema = StructType([
        StructField("pk", IntegerType()),
        StructField("op", StringType()),
        StructField("offset", IntegerType()),
        StructField("fullDoc", StructType([
            StructField("customer", StringType()),
            StructField("price", IntegerType()),
            StructField("date", DateType()),
            StructField("is_deleted", BooleanType()),
            StructField("pk", IntegerType()),

        ])),
    ])
    cdc = socket.withColumn("value", F.from_json(F.cast("str", "value"), schema)).select("value.*")
    return cdc


def main():
    builder = (
        SparkSession
        .builder
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # streaming_query = stream_from_socket_to_console(spark)
    streaming_query = stream_transactions_cdc(spark)

    spark.streams.awaitAnyTermination()

    # DeltaTable.forPath(spark, "../data/output/transactions_cdc").toDF().show()


if __name__ == '__main__':
    main()

import uuid

from chispa import assert_df_equality

from tests.models import *
from tikal_modern_data_eng_2024_spark_lecture.batch import *
from tikal_modern_data_eng_2024_spark_lecture.streaming import *


def test_parse_transactions_cdc(spark: SparkSession):
    cdc = [
        CDCTransaction(op="i", pk=1, offset=1, fullDoc=Transaction(pk=1, customer="David", price=150)),
        CDCTransaction(op="i", pk=2, offset=2, fullDoc=Transaction(pk=2, customer="Moses", price=1000)),
        CDCTransaction(op="i", pk=3, offset=3, fullDoc=Transaction(pk=3, customer="Rivka", price=2000)),
        CDCTransaction(op="u", pk=3, offset=4, fullDoc=Transaction(pk=3, customer="Rivka", price=3000)),
        CDCTransaction(op="d", pk=1, offset=5)
    ]

    expected_state = [
        Transaction(pk=1, date=None, is_deleted=True),
        Transaction(pk=2, customer="Moses", price=1000),
        Transaction(pk=3, customer="Rivka", price=3000),
    ]

    cdc = spark.createDataFrame(cdc)
    expected_state = spark.createDataFrame(expected_state)

    result = parse_transactions_cdc(cdc)

    assert_df_equality(result, expected_state, ignore_column_order=True, ignore_row_order=True)


def test_write_cdc(spark: SparkSession, data_output_folder: Path):
    prev_state = [
        Transaction(pk=1, customer="David", price=150),
        Transaction(pk=2, customer="Moses", price=1000),
        Transaction(pk=3, customer="Rivka", price=2000),
    ]

    incremental_state = [
        Transaction(pk=1, is_deleted=True),
        Transaction(pk=3, customer="Rivka", price=3000),
    ]

    expected_final_state = [
        Transaction(pk=1, is_deleted=True),
        Transaction(pk=2, customer="Moses", price=1000),
        Transaction(pk=3, customer="Rivka", price=3000),
    ]

    prev_state = spark.createDataFrame(prev_state)
    incremental_state = spark.createDataFrame(incremental_state)
    expected_final_state = spark.createDataFrame(expected_final_state)

    path_to_table = (data_output_folder / uuid.uuid4().__str__())

    write_incremental_state(spark, prev_state, path_to_table)
    write_incremental_state(spark, incremental_state, path_to_table)

    final_state = DeltaTable.forPath(spark, path_to_table.absolute().__str__()).toDF()

    assert_df_equality(expected_final_state, final_state, ignore_column_order=True, ignore_row_order=True)

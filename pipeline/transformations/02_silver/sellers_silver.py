from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_or_fail("rule1",'seller_id is not null')
@dp.table(name="silver.sellers_silver")
def sellers_silver():
    return (
        spark.readStream.table('olist_medallion.bronze.sellers_bronze')
        .dropDuplicates(['seller_id'])
        .withColumn("ingest_ts", F.current_timestamp())
    )
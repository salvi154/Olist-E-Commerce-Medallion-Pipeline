from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_or_drop("rule1",'review_id is not null and order_id IS NOT NULL')
@dp.expect_or_drop("rule2",'review_score >= 1 and review_score <= 5')
@dp.table(name="silver.reviews_silver")
def reviews_silver():
    return (
        spark.readStream.table('olist_medallion.bronze.reviews_bronze')
        .dropDuplicates(['review_id','order_id'])
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("review_score",F.col("review_score").cast("double"))
        .withColumn("review_creation_date",F.col("review_creation_date").cast("timestamp"))
        .withColumn("review_answer_timestamp",F.col("review_answer_timestamp").cast("timestamp"))
    )
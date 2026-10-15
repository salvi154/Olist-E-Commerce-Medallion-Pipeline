from pyspark.sql.functions import sum, count, row_number
from pyspark import pipelines as dp
from pyspark.sql.window import Window

@dp.materialized_view(name='gold.dim_payment_type')
def dim_payment_type():
  df = spark.read.table('olist_medallion.silver.payments_silver')
  return (
    df.select('payment_type')
    .distinct()
    .withColumn('payment_type_id',row_number().over(Window.orderBy('payment_type')))
  )
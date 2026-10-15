from pyspark import pipelines as dp 
from pyspark.sql.functions import count, sum, col, date_format



@dp.materialized_view(name='gold.fact_order')
def fact_order():
  order = spark.table('olist_medallion.silver.orders_silver')
  payment = (
    spark.table('olist_medallion.gold.fact_payment')
    .groupBy('order_id')
    .agg(
      sum('payment_value').alias('total_payment'),
      count('payment_value').alias('payment_count')
          )
    )
  customer = spark.table('olist_medallion.gold.dim_customer')
  dates = spark.table('olist_medallion.gold.dim_date')
  order = (order.withColumn('order_purchase_timestamp_date_key',
          date_format(col('order_purchase_timestamp'),'yyyyMMdd').cast('int')))
  joined_df = order.join(payment, on="order_id",how='left').join(customer,on='customer_id',how='left').join(dates,order.order_purchase_timestamp_date_key==dates.date_id,'left')
  return joined_df

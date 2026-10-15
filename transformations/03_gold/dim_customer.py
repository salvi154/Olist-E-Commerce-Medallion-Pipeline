from pyspark import pipelines as dp 


@dp.materialized_view(name='gold.dim_customer')
def dim_customer():
  return spark.table('olist_medallion.silver.customers_silver').select('customer_id','customer_unique_id','customer_zip_code_prefix','customer_city','customer_state')
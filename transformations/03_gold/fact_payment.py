from pyspark import pipelines as dp 
from pyspark.sql.functions import *



@dp.materialized_view(name='gold.fact_payment')
def fact_payment():
 payments = spark.read.table('olist_medallion.silver.payments_silver')
 payment_type = spark.read.table('olist_medallion.gold.dim_payment_type')
 return (
        payments.join(payment_type, on='payment_type',how='left')
        .select("order_id",
            "payment_sequential",
            "payment_type_id",
            "payment_installments",
            "payment_value",)
 )
  

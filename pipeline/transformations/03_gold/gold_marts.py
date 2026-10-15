from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name='gold.mart_daily_revenue')
def mart_daily_revenue():
    df = spark.read.table('olist_medallion.gold.fact_order')
    df = df.groupBy('date').agg(
        F.sum('total_payment').alias('daily_revenue'),
        F.count('order_id').alias('order_count'),
        F.round(F.sum('total_payment') / F.count('order_id'), 2).alias('avg_order_value'),
        )
    return df

@dp.materialized_view(name='gold.mart_monthly_revenue')
def mart_monthly_revenue():
    df = spark.read.table('olist_medallion.gold.fact_order')
    df = df.groupBy('year_month').agg(
        F.sum('total_payment').alias('monthly_revenue'),
        F.count('order_id').alias('order_count'),
        F.round(F.sum('total_payment') / F.count('order_id'), 2).alias('avg_order_value'),
        )
    return df

@dp.materialized_view(name='gold.mart_seller_performance')
def mart_seller_performance():
    df = spark.read.table('olist_medallion.gold.fact_order_item')
    df = df.groupBy('seller_id').agg(
        F.sum('price').alias('total_product_revenue'),
        F.sum('freight_value').alias('total_freight_revenue'),
        F.sum(F.col('price') + F.col('freight_value')).alias('total_revenue'),
        F.count('order_id').alias('items_sold'),
        F.countDistinct('product_id').alias('distinct_products_sold'),
        F.countDistinct('order_id').alias('distinct_orders'),
        ).orderBy(F.desc('total_revenue'))
    return df
    
@dp.materialized_view(name='gold.mart_delivery_performance')
def mart_delivery_performance():
    df = spark.read.table('olist_medallion.gold.fact_order')
    df = (df.filter(F.col('order_status') == 'delivered')
        .withColumn('deliver_flag',
        F.when(F.col('order_estimated_delivery_date') == F.col('order_delivered_customer_date'),'on_time')
        .when(F.col('order_estimated_delivery_date') < F.col('order_delivered_customer_date'),'late')
        .otherwise('early')
        ))
    df = (
        df.groupBy('customer_state').agg(
        F.count('order_id').alias('total_delivered_orders'),
        F.sum(F.when(F.col('deliver_flag') == 'on_time', 1).otherwise(0)).alias('on_time_orders'),
        F.sum(F.when(F.col('deliver_flag') == 'late', 1).otherwise(0)).alias('late_orders'),
        F.sum(F.when(F.col('deliver_flag') == 'early', 1).otherwise(0)).alias('early_orders'),
        F.round(
            F.avg(F.datediff(F.col('order_delivered_customer_date'), F.col('order_estimated_delivery_date'))), 2
            ).alias('avg_delivery_delay_days'))
        .withColumn('late_percentage', F.round(F.col('late_orders') / F.col('total_delivered_orders') *100 , 2))
        .orderBy(F.desc('late_percentage'))
        )
    return df


@dp.materialized_view(name='gold.mart_customer_rfm')
def mart_customer_rfm():
    df = spark.read.table('olist_medallion.gold.fact_order')
    max_date = df.select(F.max('order_purchase_timestamp')).first()[0]

    rfm = (
        df.groupBy('customer_unique_id')
        .agg(
            F.datediff(F.lit(max_date), F.max('order_purchase_timestamp')).alias('recency_days'),
            F.count('order_id').alias('frequency'),
            F.sum('total_payment').alias('monetary'),
        )
    )
    rfm = rfm.withColumn('monetary',F.coalesce(F.col('monetary'),F.lit(0.0)))
    r_window = Window.orderBy(F.desc('recency_days'))
    f_window = Window.orderBy('frequency')
    m_window = Window.orderBy('monetary')

    scored = (
        rfm
        .withColumn('r_score',F.ntile(5).over(r_window))
        .withColumn('f_score',F.ntile(5).over(f_window))
        .withColumn('m_score',F.ntile(5).over(m_window))
    )

    scored = scored.withColumn('rfm_score',F.concat(F.col('r_score'),F.col('f_score'),F.col('m_score')))

    scored = scored.withColumn('fm_avg', (F.col('f_score') + F.col('m_score')) / 2)

    scored = (
        scored.withColumn(
            'customer_segment',
            F.when((F.col('r_score') >= 4) & (F.col('fm_avg') >= 4),'Champions')
            .when((F.col('r_score') >= 3) & (F.col('fm_avg') >= 4),'Loyal Customers')
            .when((F.col('r_score') >= 4) & (F.col('fm_avg') < 4) & (F.col('fm_avg') >= 2), 'Potential Loyalists')
            .when((F.col('r_score') >= 4) & (F.col('fm_avg') < 2), 'New Customers')
            .when((F.col('r_score') == 3) & (F.col('fm_avg') < 4) & (F.col('fm_avg') >= 2) , 'Need Attention')
            .when((F.col('r_score') <= 2) & (F.col('fm_avg') >= 4), 'At Risk')
            .when((F.col('r_score') <= 2) & (F.col('fm_avg') >= 2) & (F.col('fm_avg') < 4), 'Cannot Lose Them')
            .when((F.col('r_score') <= 2) & (F.col('fm_avg') < 2), 'Lost')
            .otherwise('Others')
        )
    )
    return scored


        
    
    
    
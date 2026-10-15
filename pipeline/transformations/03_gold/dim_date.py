from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.dim_date",
    comment="Calendar dimension table covering 2016 to 2020"
)
def dim_date():
    return (
        spark.range(0, 1827)  # 2016-01-01 through 2020-12-31 (1827 days)
        .select(F.expr("date_add(date '2016-01-01', cast(id as int))").alias("date"))
        .select(
            F.date_format(F.col("date"), "yyyyMMdd").cast("int").alias("date_id"),
            F.col("date"),
            F.date_format(F.col("date"), "EEEE").alias("day_name"),
            F.date_format(F.col("date"), "EEE").alias("day_name_short"),
            F.dayofweek(F.col("date")).alias("day_of_week"),
            F.dayofmonth(F.col("date")).alias("day_of_month"),
            F.dayofyear(F.col("date")).alias("day_of_year"),
            F.weekofyear(F.col("date")).alias("week_of_year"),
            F.date_format(F.col("date"), "MMMM").alias("month_name"),
            F.date_format(F.col("date"), "MMM").alias("month_name_short"),
            F.month(F.col("date")).alias("month"),
            F.quarter(F.col("date")).alias("quarter"),
            F.year(F.col("date")).alias("year"),
            F.date_format(F.col("date"), "yyyy-MM").alias("year_month"),
            F.dayofweek(F.col("date")).isin(1,7).alias("is_weekend")
        )
    )

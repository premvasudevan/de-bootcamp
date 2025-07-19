from pyspark.sql import SparkSession

query = """
with cte as (select *, row_number() over (partition by actor, year, filmid order by year) as rn
from actor_films)
select actor, year, film, votes, rating, filmid from cte where rn = 1
order by actor, year
"""

def do_dedupe_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actor_films")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("dedupe_job") \
        .getOrCreate()
    output_df = do_dedupe_transformation(spark, spark.table("actor_films"))
    output_df.write.mode("overwrite").insertInto("actor_films_deduped")
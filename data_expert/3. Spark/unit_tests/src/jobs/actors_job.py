from pyspark.sql import SparkSession

query = """
select actor, year,
		array_agg( map(
          'film', film,
          'votes', cast(votes as string),
          'rating', cast(rating as string),
          'filmid', cast(filmid as string))) as films,
		case when avg(cast(rating as float)) > 8 then 'star'
		when avg(cast(rating as float)) > 7 and avg(cast(rating as float)) <= 8 then 'good'
		when avg(cast(rating as float)) > 6 and avg(cast(rating as float)) <= 7 then 'average'
		when avg(cast(rating as float)) <= 6 then 'bad'
	end as quality_class
from actor_films 
group by actor, year
order by actor, year
"""

def do_actor_films_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actor_films")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors") \
        .getOrCreate()
    output_df = do_actor_films_transformation(spark, spark.table("actor_films"))
    output_df.write.mode("overwrite").insertInto("actors_scd")
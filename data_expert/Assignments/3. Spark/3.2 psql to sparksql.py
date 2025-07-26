from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("spark_sql").getOrCreate()

actor_films_df = spark.read.format("csv")\
                        .option("header","true")\
                        .option("inferschema","true")\
                        .load("file:/home/iceberg/data/actor_films.csv")

actor_films_df.createOrReplaceTempView("actor_films")

# actor dataset grouped for year 1980 and customized quality_class
spark.sql("""
select actor, year,
		array_agg( map(
          'film', film,
          'votes', cast(votes as string),
          'rating', cast(rating as string),
          'filmid', cast(filmid as string))) as films,
		case when avg(rating) > 8 then 'star'
		when avg(rating) > 7 and avg(rating) <= 8 then 'good'
		when avg(rating) > 6 and avg(rating) <= 7 then 'average'
		when avg(rating) <= 6 then 'bad'
	end as quality_class
from actor_films 
group by actor, year
""").show(5, truncate=False)

# deduped data

spark.sql("""
with cte as (select *, row_number() over (partition by actorid, year, filmid order by year) as rn
from actor_films)
select actorid, year, film, votes, rating, filmid from cte where rn = 1
""").show(10)

spark.stop()
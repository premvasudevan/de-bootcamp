from chispa.dataframe_comparer import *

from ..jobs.actors_job import do_actor_films_transformation
from collections import namedtuple

ActorFilmInput = namedtuple("ActorFilmInput", "actor year film votes rating filmid")
ActorFilmOutput = namedtuple("ActorFilmOutput", "actor year films quality_class")

def test_actor_films_transformation(spark):
    input_data = [
        ActorFilmInput("Amit", 1980, "Film A", 100, 8.5, 1),
        ActorFilmInput("Amit", 1980, "Film B", 150, 7.5, 2),
        ActorFilmInput("John", 1980, "Film C", 200, 6.5, 3),
        ActorFilmInput("Smith", 1980, "Film D", 50, 5.5, 4)
    ]

    input_df = spark.createDataFrame(input_data)

    actual_df = do_actor_films_transformation(spark, input_df)

    expected_data = [
        ActorFilmOutput("Amit", 1980,
                        [{"film": "Film A", "votes": "100", "rating": "8.5", "filmid": "1"},
                         {"film": "Film B", "votes": "150", "rating": "7.5", "filmid": "2"}],
                        'good'),
        ActorFilmOutput("John", 1980,
                        [{"film": "Film C", "votes": "200", "rating": "6.5", "filmid": "3"}],
                        'average'),
        ActorFilmOutput("Smith", 1980,
                        [{"film": "Film D", "votes": "50", "rating": "5.5", "filmid": "4"}],
                        'bad')
    ]

    expected_df = spark.createDataFrame(expected_data)
    
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
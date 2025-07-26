from chispa.dataframe_comparer import *

from ..jobs.dedupe_job import do_dedupe_transformation
from collections import namedtuple

ActorFilmInput = namedtuple("ActorFilmInput", "actor year film votes rating filmid")
ActorFilmOutput = namedtuple("ActorFilmOutput", "actor year film votes rating filmid")

def test_dedupe_transformation(spark):
    input_data = [
        ActorFilmInput("Amit", 1980, "Film A", 100, 8.5, 1),
        ActorFilmInput("Amit", 1980, "Film B", 150, 7.5, 2),
        ActorFilmInput("John", 1980, "Film C", 200, 6.5, 3),
        ActorFilmInput("Smith", 1980, "Film D", 50, 5.5, 4),
        ActorFilmInput("Amit", 1980, "Film A", 100, 8.5, 1) # Duplicate entry
    ]

    input_df = spark.createDataFrame(input_data)

    actual_df = do_dedupe_transformation(spark, input_df)
    expected_data = [
        ActorFilmOutput("Amit", 1980, "Film A", 100, 8.5, 1),
        ActorFilmOutput("Amit", 1980, "Film B", 150, 7.5, 2),
        ActorFilmOutput("John", 1980, "Film C", 200, 6.5, 3),
        ActorFilmOutput("Smith", 1980, "Film D", 50, 5.5, 4)
    ]

    expected_df = spark.createDataFrame(expected_data)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

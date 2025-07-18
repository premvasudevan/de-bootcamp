from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("spark_hands_on").getOrCreate()

match_details = spark.read.format("csv")\
                        .option("header","true")\
                        .option("inferschema","true")\
                        .load("file:/home/iceberg/data/match_details.csv")

matches = spark.read.format("csv")\
                .option("header", "true")\
                .option("inferschema", "true")\
                .load("file:/home/iceberg/data/matches.csv")

medals_matches_players = spark.read.format("csv")\
                .option("header", "true")\
                .option("inferschema", "true")\
                .load("file:/home/iceberg/data/medals_matches_players.csv")

medals = spark.read.format("csv")\
                .option("header", "true")\
                .option("inferschema", "true")\
                .load("file:/home/iceberg/data/medals.csv")

maps = spark.read.format("csv")\
                .option("header", "true")\
                .option("inferschema", "true")\
                .load("file:/home/iceberg/data/maps.csv")

# Set the Spark configuration to disable broadcast joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

f.broadcast(medals)
f.broadcast(maps)

spark.sql("""
DROP TABLE IF EXISTS bootcamp.match_details_bucketed
""")

match_details_bucketed_DDL = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
        match_id string,
        player_gamertag string,
        previous_spartan_rank integer,
        spartan_rank integer,
        previous_total_xp integer,
        total_xp integer,
        previous_csr_tier integer,
        previous_csr_designation integer,
        previous_csr integer,
        previous_csr_percent_to_next_tier integer,
        previous_csr_rank integer,
        current_csr_tier integer,
        current_csr_designation integer,
        current_csr integer,
        current_csr_percent_to_next_tier integer,
        current_csr_rank integer,
        player_rank_on_team integer,
        player_finished boolean,
        player_average_life string,
        player_total_kills integer,
        player_total_headshots integer,
        player_total_weapon_damage double,
        player_total_shots_landed integer,
        player_total_melee_kills integer,
        player_total_melee_damage double,
        player_total_assassinations integer,
        player_total_ground_pound_kills integer,
        player_total_shoulder_bash_kills integer,
        player_total_grenade_damage double,
        player_total_power_weapon_damage double,
        player_total_power_weapon_grabs integer,
        player_total_deaths integer,
        player_total_assists integer,
        player_total_grenade_kills integer,
        did_win integer,
        team_id integer
)
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""

spark.sql(match_details_bucketed_DDL)

match_details.write.mode("append")\
                    .bucketBy(16, "match_id")\
                    .saveAsTable("bootcamp.match_details_bucketed")

spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")

matches_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
        match_id string ,
        mapid string ,
        is_team_game boolean ,
        playlist_id string ,
        game_variant_id string ,
        is_match_over boolean ,
        completion_date timestamp ,
        match_duration string ,
        game_mode string ,
        map_variant_id string
)
USING iceberg
PARTITIONED BY(bucket(16, match_id));
"""

spark.sql(matches_bucketed_ddl)

matches.write.mode("append")\
                .bucketBy(16, "match_id")\
                .saveAsTable("bootcamp.matches_bucketed")

spark.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed""")

medal_matches_players_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
        match_id string ,
        player_gamertag string ,
        medal_id long ,
        count integer 
)
USING iceberg
PARTITIONED BY(bucket(16, match_id));

"""

spark.sql(medal_matches_players_bucketed_ddl)

medals_matches_players.write.mode("append")\
                        .bucketBy(16, "match_id")\
                        .saveAsTable("bootcamp.medal_matches_players_bucketed")


# Read the bucketed tables
bucketed_md = spark.table("bootcamp.match_details_bucketed")
bucketed_m = spark.table("bootcamp.matches_bucketed")
bucketed_mmp = spark.table("bootcamp.medal_matches_players_bucketed")


joined_df = bucketed_m.join(bucketed_md, "match_id")\
                        .join(bucketed_mmp, "match_id")

agg_df = joined_df\
            .groupby("match_id","mapid","playlist_id","match_details_bucketed.player_gamertag","medal_id")\
            .agg(f.sum("player_total_kills").alias("total_kills"), f.sum("count").alias("total_medals"))\
            .select("match_id","mapid","playlist_id","match_details_bucketed.player_gamertag","medal_id", "total_kills", "total_medals")
            

sorted_mapid = agg_df.repartition(10).sortWithinPartitions("mapid")
sorted_playlist_id = agg_df.repartition(10).sortWithinPartitions("playlist_id")
sorted_sorted_map_playlist_id = agg_df.repartition(10).sortWithinPartitions("mapid","playlist_id")

sorted_mapid.write.mode("overwrite").saveAsTable("bootcamp.sorted_mapid")
sorted_playlist_id.write.mode("overwrite").saveAsTable("bootcamp.sorted_playlist_id")
sorted_sorted_map_playlist_id.write.mode("overwrite").saveAsTable("bootcamp.sorted_sorted_map_playlist_id")

spark.sql("""
select sum(file_size_in_bytes), 'sorted_mapid' as tablename from bootcamp.sorted_mapid.files
union all
select sum(file_size_in_bytes), 'sorted_playlist_id' as tablename from bootcamp.sorted_playlist_id.files
union all
select sum(file_size_in_bytes), 'sorted_sorted_map_playlist_id' as tablename from bootcamp.sorted_sorted_map_playlist_id.files
""").show()

# stop the spark session
spark.stop()
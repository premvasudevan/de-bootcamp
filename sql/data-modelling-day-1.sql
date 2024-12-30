-- select * from player_seasons

-- create type season_stats as (
-- season integer,
-- gp integer,
-- pts real,
-- reb real,
-- ast real
-- )

-- create type scoring_class as enum ('star', 'good', 'average', 'bad')

-- drop table players
-- create table players (
-- player_name text,
-- height text,
-- college text,
-- country text,
-- draft_year text,
-- draft_round text,
-- draft_number text,
-- season_stats season_stats[],
-- current_season integer,
-- scoring_class scoring_class,
-- year_since_last_played integer,
-- primary key(player_name, current_season)
-- )
-- select min(season) from player_seasons
DO $$
BEGIN 
	FOR COUNTER IN 1996..2022 LOOP
		INSERT INTO PLAYERS
		WITH
			YESTERDAY AS (
				SELECT
					*
				FROM
					PLAYERS
				WHERE
					CURRENT_SEASON = COUNTER -1
			),
			TODAY AS (
				SELECT
					*
				FROM
					PLAYER_SEASONS
				WHERE
					SEASON = COUNTER
			)
		SELECT
			COALESCE(T.PLAYER_NAME, Y.PLAYER_NAME) AS PLAYER_NAME,
			COALESCE(T.HEIGHT, Y.HEIGHT) AS HEIGHT,
			COALESCE(T.COLLEGE, Y.COLLEGE) AS COLLEGE,
			COALESCE(T.COUNTRY, Y.COUNTRY) AS COUNTRY,
			COALESCE(T.DRAFT_YEAR, Y.DRAFT_YEAR) AS DRAFT_YEAR,
			COALESCE(T.DRAFT_ROUND, Y.DRAFT_ROUND) AS DRAFT_ROUND,
			COALESCE(T.DRAFT_NUMBER, Y.DRAFT_NUMBER) AS DRAFT_NUMBER,
			CASE
				WHEN Y.SEASON_STATS IS NULL THEN ARRAY[
					ROW (T.SEASON, T.GP, T.PTS, T.REB, T.AST)::SEASON_STATS
				]
				WHEN T.SEASON IS NOT NULL THEN Y.SEASON_STATS || ARRAY[
					ROW (T.SEASON, T.GP, T.PTS, T.REB, T.AST)::SEASON_STATS
				]
				ELSE Y.SEASON_STATS
			END AS SEASON_STATS,
			COALESCE(T.SEASON, Y.CURRENT_SEASON + 1) AS CURRENT_SEASON,
			CASE
				WHEN T.SEASON IS NOT NULL THEN CASE
					WHEN T.PTS > 20 THEN 'star'
					WHEN T.PTS > 15 THEN 'good'
					WHEN T.PTS > 10 THEN 'average'
					ELSE 'bad'
				END::SCORING_CLASS
				ELSE Y.SCORING_CLASS
			END AS SCORING_CLASS,
			CASE
				WHEN T.SEASON IS NOT NULL THEN 0
				ELSE Y.YEAR_SINCE_LAST_PLAYED + 1
			END AS YEAR_SINCE_LAST_PLAYED
		FROM
			YESTERDAY Y FULL OUTER JOIN TODAY T ON Y.PLAYER_NAME = T.PLAYER_NAME;
	END LOOP;
END; $$

-- select player_name, (unnest(season_stats)::season_stats).* from players where current_season = 2001
-- select max(season) from player_seasons

-- DO $$
-- BEGIN
-- 	FOR cnt IN 1..5 LOOP
-- 		RAISE NOTICE 'cnt: %', cnt;
-- 	END LOOP;
-- END; $$

-- SELECT * FROM PLAYERS WHERE CURRENT_SEASON = 2022

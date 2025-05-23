-- create table players_scd (
-- player_name text,
-- scoring_class scoring_class,
-- is_active BOOLEAN,
-- start_season integer,
-- end_season integer,
-- current_season integer,
-- primary key(player_name, start_season)
-- )
INSERT INTO
	PLAYERS_SCD
WITH
	PREVIOUS AS (
		SELECT
			PLAYER_NAME,
			SCORING_CLASS,
			IS_ACTIVE,
			LAG(SCORING_CLASS, 1) OVER (
				PARTITION BY
					PLAYER_NAME
				ORDER BY
					CURRENT_SEASON
			) AS PREV_SCORING_CLASS,
			LAG(IS_ACTIVE, 1) OVER (
				PARTITION BY
					PLAYER_NAME
				ORDER BY
					CURRENT_SEASON
			) AS PREV_IS_ACTIVE,
			CURRENT_SEASON
		FROM
			PLAYERS
		WHERE
			CURRENT_SEASON <= 2021
	),
	WITH_INDICATORS AS (
		SELECT
			*,
			CASE
				WHEN SCORING_CLASS <> PREV_SCORING_CLASS THEN 1
				WHEN IS_ACTIVE <> PREV_IS_ACTIVE THEN 1
				ELSE 0
			END AS CHANGE_INDICATOR
		FROM
			PREVIOUS
	),
	WITH_STREAKS AS (
		SELECT
			*,
			SUM(CHANGE_INDICATOR) OVER (
				PARTITION BY
					PLAYER_NAME
				ORDER BY
					CURRENT_SEASON
			) AS STREAK_IDENTIFIER
		FROM
			WITH_INDICATORS
	)
SELECT
	PLAYER_NAME,
	SCORING_CLASS,
	IS_ACTIVE,
	MIN(CURRENT_SEASON) AS START_SEASON,
	MAX(CURRENT_SEASON) AS END_SEASON,
	2021 AS CURRENT_SEASON
FROM
	WITH_STREAKS
GROUP BY
	PLAYER_NAME,
	SCORING_CLASS,
	IS_ACTIVE,
	STREAK_IDENTIFIER
ORDER BY
	PLAYER_NAME,
	START_SEASON

-- scd type 2 with incremental approach
-- create type scd_type as (
-- scoring_class scoring_class,
-- is_active boolean,
-- start_season integer,
-- end_season integer
-- )
WITH
	LAST_SEASON AS (
		SELECT
			*
		FROM
			PLAYERS_SCD
		WHERE
			CURRENT_SEASON = 2021
			AND END_SEASON = 2021
	),
	HISTORICAL_SCD AS (
		SELECT
			PLAYER_NAME,
			SCORING_CLASS,
			IS_ACTIVE,
			START_SEASON,
			END_SEASON
		FROM
			PLAYERS_SCD
		WHERE
			CURRENT_SEASON = 2021
			AND END_SEASON < 2021
	),
	THIS_SEASON AS (
		SELECT
			*
		FROM
			PLAYERS
		WHERE
			CURRENT_SEASON = 2022
	),
	UNCHANGED_RECORDS AS (
		SELECT
			TS.PLAYER_NAME,
			TS.SCORING_CLASS,
			TS.IS_ACTIVE,
			LS.START_SEASON,
			TS.CURRENT_SEASON AS END_SEASON
		FROM
			THIS_SEASON TS
			JOIN LAST_SEASON LS ON TS.PLAYER_NAME = LS.PLAYER_NAME
		WHERE
			TS.SCORING_CLASS = LS.SCORING_CLASS
			AND TS.IS_ACTIVE = LS.IS_ACTIVE
	),
	CHANGED_RECORDS AS (
		SELECT
			TS.PLAYER_NAME,
			(
				UNNEST(
					ARRAY[
						ROW (
							LS.SCORING_CLASS,
							LS.IS_ACTIVE,
							LS.START_SEASON,
							LS.END_SEASON
						)::SCD_TYPE,
						ROW (
							TS.SCORING_CLASS,
							TS.IS_ACTIVE,
							TS.CURRENT_SEASON,
							TS.CURRENT_SEASON
						)::SCD_TYPE
					]
				)::SCD_TYPE
			).*
		FROM
			THIS_SEASON TS
			LEFT JOIN LAST_SEASON LS ON TS.PLAYER_NAME = LS.PLAYER_NAME
		WHERE
			(
				TS.SCORING_CLASS <> LS.SCORING_CLASS
				OR TS.IS_ACTIVE <> LS.IS_ACTIVE
			)
	),
	NEW_RECORDS AS (
		SELECT
			TS.PLAYER_NAME,
			TS.SCORING_CLASS,
			TS.IS_ACTIVE,
			TS.CURRENT_SEASON AS START_SEASON,
			TS.CURRENT_SEASON AS END_SEASON
		FROM
			THIS_SEASON TS
			LEFT JOIN LAST_SEASON LS ON TS.PLAYER_NAME = LS.PLAYER_NAME
		WHERE
			LS.PLAYER_NAME IS NULL
	)
SELECT
	*
FROM
	HISTORICAL_SCD
UNION ALL
SELECT
	*
FROM
	UNCHANGED_RECORDS
UNION ALL
SELECT
	*
FROM
	CHANGED_RECORDS
UNION ALL
SELECT
	*
FROM
	NEW_RECORDS
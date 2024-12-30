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
insert into players
with yesterday as (
select * from players where current_season = 2000
),
today as (
select * from player_seasons where season = 2001
)
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats IS null then array[row(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
	when t.season is not null then y.season_stats || array[row(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
	else y.season_stats end as season_stats,
	coalesce(t.season, y.current_season + 1) as current_season,
	case when t.season is not null then
		case when t.pts > 20 then 'star'
		when t.pts > 15 then 'good'
		when t.pts > 10 then 'average'
		else 'bad' END::scoring_class
	else y.scoring_class end as scoring_class,
	case when t.season is not null then 0
	else y.year_since_last_played + 1
	end as year_since_last_played
	
from yesterday y full outer join today t on y.player_name = t.player_name


-- select player_name, (unnest(season_stats)::season_stats).* from players where current_season = 2001
select max(season) from player_seasons

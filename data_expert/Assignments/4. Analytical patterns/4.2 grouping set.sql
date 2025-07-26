--A query that uses GROUPING SETS to do efficient aggregations of game_details data

--drop table games_agg
--create agg table
create table games_agg (
	player_name text,
	team_name text,
	season	text,
	total_games_played int,
	total_points int,
	total_wins int,
	primary key (player_name, team_name, season)
)

-- load agg data to table
--insert into games_agg
with base as (
select gd.player_name, gd.team_abbreviation , g.season,
coalesce(gd.pts,0) as pts,
case
	when (case when g.home_team_wins = 1 then home_team_id end) = gd.team_id then 'won'
	when (case when g.home_team_wins = 0 then visitor_team_id end) = gd.team_id then 'won'
	else 'lost'
	end as result
from game_details gd join
games g on gd.game_id = g.game_id
),
agg as (
select 
coalesce(base.player_name , '(overall)') as  player_name,
coalesce(base.team_abbreviation , '(overall)') as  team_abbreviation,
coalesce(cast(base.season as text) , '(overall)') as  season,
count(1) as num_of_games,
sum(pts) as total_points,
sum(case when result = 'won' then 1 else 0 end) as result
from base
group by grouping sets (
	(base.season,base.team_abbreviation, base.player_name ),
	(base.team_abbreviation)
))
select *
from agg
order by season, team_abbreviation 


--who scored the most points playing for one team?
select team_name, player_name, sum(total_points) score_per_team
from games_agg
where player_name != '(overall)'
group by team_name, player_name
order by 3 desc
limit 1

--who scored the most points in one season?
select season, player_name, sum(total_points) as score_per_season
from games_agg
where player_name != '(overall)'
group by season, player_name
order by 3 desc
limit 1

--which team has won the most games?
select team_name, sum(total_wins) as max_wins
from games_agg
where season != '(overall)'
and player_name != '(overall)'
and team_name != '(overall)'
group by team_name
order by 2 desc
limit 1
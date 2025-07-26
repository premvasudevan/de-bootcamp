
--What is the most games a team has won in a 90 game stretch?
with base as (
select g.game_date_est , g.game_id , team_id, gd.team_abbreviation , g.season,
case
	when (case when g.home_team_wins = 1 then home_team_id end) = gd.team_id then 'won'
	when (case when g.home_team_wins = 0 then visitor_team_id end) = gd.team_id then 'won'
	else 'lost'
	end as result
from game_details gd join
games g on gd.game_id = g.game_id
),
deduped as (
select *, row_number() over (partition by game_date_est,game_id, team_id ) as rn
from base),
match_90 as (select game_date_est,game_id, team_id, team_abbreviation ,season ,result,
sum(case when result = 'won' then 1 else 0 end) over (partition by team_id order by game_date_est rows between 89 preceding and current row) as win_90_matches
from deduped
where rn = 1)
select team_abbreviation , max(win_90_matches ) as most_wins_in_90_matches
from match_90 
group by team_abbreviation 
order by 2 desc
limit 1



--How many games in a row did LeBron James score over 10 points a game?
with lebron_data as (select player_name, game_id , coalesce(pts ,0) as points, 
case when pts > 10 then 1 else 0 end as gt_10_pts,
row_number() over (partition by player_name , game_id ) as rn
from game_details gd 
where player_name = 'LeBron James'
),
deduped as (
	select player_name, game_id, points,gt_10_pts
	from lebron_data 
	where rn = 1),
result_dataset as (select *,
  ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY game_id) -
    ROW_NUMBER() OVER (PARTITION BY player_name, gt_10_pts ORDER BY game_id) AS rank_group
from deduped)
select count(1) as max_streak_gt_10_points
from result_dataset
group by rank_group
order by 1 desc 
limit 1

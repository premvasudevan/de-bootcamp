
-- create players table to determine state of a player
create table players (
	player_name		text,
	points 			float,
	last_active_season int,
	current_season	int,
	primary key(player_name, current_season)
)

--incremental query to populate players table
@set ds = 2010
insert into players 
with previous_season as (
	select *
	from players p 
	where current_season = $ds - 1
),
current_season as (
	select player_name, pts , season
	from player_seasons
	where season = $ds
)
select coalesce(cs.player_name, ps.player_name) as player_name,
coalesce(cs.pts, ps.points) as points,
coalesce(cs.season, ps.last_active_season) as last_active_season,
coalesce(cs.season, ps.current_season + 1) as current_season
from previous_season ps full outer join current_season cs
on ps.player_name = cs.player_name


--query to determine players state as per requirement
with base as (
	select player_name,current_season,
	case 
		when lag(current_season) over(partition by player_name order by current_season rows between unbounded preceding and current row) is null 
	then 'New'
		when lag(last_active_season) over (partition by player_name order by current_season) + 1 != current_season 
	and last_active_season = current_season
	then 'Returned from Retirement'
	when last_active_season = current_season
	and lag(current_season) over(partition by player_name order by current_season rows between unbounded preceding and current row) is not null 
	then 'Continued Playing'
	when last_active_season + 1 = current_season then 'Retired'
	else 'Stayed Retired'
	end as state
	from players
)
select *
from base

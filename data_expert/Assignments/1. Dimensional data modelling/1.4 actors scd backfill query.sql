--select min(year), max(year) from actors; --1970, 1980

--insert into actors_history_scd
with cte1 as (
select actor,
year,
"quality_class",
lag("quality_class" ) over(partition by actor order by year) as previous_qc,
is_active,
lag("is_active" ) over(partition by actor order by year) as previous_active_flag
from actors),
change_indicator as (
	select
	actor, 
	year,
	quality_class,
	previous_qc,
	is_active,
	previous_active_flag,
	case when is_active <> previous_active_flag or quality_class <> previous_qc then 1 else 0 end as change_ind
	from cte1 
),
with_num_changes as (
select
	*,
	sum(change_ind) over (partition by actor order by year) num_changes
	from change_indicator )
select actor, 
quality_class,
is_active,
min(year) as start_date,
max(year) as end_date,
1980 as year
from with_num_changes 
group by actor,num_changes,quality_class,is_active
order by actor,start_date
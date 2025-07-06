--select * from actors_history_scd

--select min(year), max(year) from actors; --1970, 1980

--create type scd_type as (
--	quality_class quality_class,
--	is_active bool,
--	start_date int,
--	end_date int)

--insert into actors_history_scd
with history_scd as (
select *
from actors_history_scd
where current_year = 1980 and end_date < 1980),
last_year as (
	select actor,
	quality_class,
	is_active,
	start_date,
	end_date
	from actors_history_scd
	where current_year = 1980
	and end_date = 1980
),
this_year as ( 
	select actor,
	"quality_class",
	is_active,
	year
	from actors 
	where year = 1981
),
unchanged_records as (
	select t.actor,
	t."quality_class",
	t.is_active,
	l.start_date,
	t."year" as end_date,
	t."year"
	from last_year l join this_year t
	on l.actor = t.actor
	where l."quality_class" = t."quality_class" and l.is_active = t.is_active
),
changed_records as (
	select t.actor,
	(unnest(array[
		row(
			l."quality_class",
			l.is_active,
			l.start_date,
			l.end_date
		)::scd_type,
		row(
			t."quality_class",
			t.is_active,
			t.year,
			t.year
		)::scd_type
	])::scd_type).*,
	t."year"
	from last_year l left join this_year t
	on l.actor = t.actor
	where l."quality_class" <> t."quality_class" or l.is_active <> t.is_active
),
new_records as ( 
		select t.actor,
			t."quality_class",
			t.is_active,
			t.year as start_date,
			t.year as end_date,
			t."year"
	from last_year l right join this_year t
	on l.actor = t.actor
	where l.actor is null
)
select * from history_scd
union all
select * from unchanged_records
union all
select * from changed_records
union all
select * from new_records


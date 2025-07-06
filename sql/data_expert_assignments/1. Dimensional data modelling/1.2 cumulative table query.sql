--select min(year), max(year) -- 1970, 2021
--from actor_films af 
insert into actors 
with last_year as (
select actor, year,
	array_agg(
		row(film, votes, rating, filmid)::films
	) as films,
		case when avg(rating) > 8 then 'star'
		when avg(rating) > 7 and avg(rating) <= 8 then 'good'
		when avg(rating) > 6 and avg(rating) <= 7 then 'average'
		when avg(rating) <= 6 then 'bad'
	end as quality_class
from actor_films 
where year = 1980
group by actor,"year"),
this_year as (
select actor, year,
	array_agg(
		row(film, votes, rating, filmid)::films
	) as films,
		case when avg(rating) > 8 then 'star'
		when avg(rating) > 7 and avg(rating) <= 8 then 'good'
		when avg(rating) > 6 and avg(rating) <= 7 then 'average'
		when avg(rating) <= 6 then 'bad'
	end as quality_class
from actor_films 
where year = 1981
group by actor,"year"),
final as (select 
coalesce(ty.actor, ly.actor) actor, 
ty.films, 
coalesce(ty.quality_class::quality_class, ly.quality_class::quality_class) as quality_class,
case when ty.actor is not null then true 
else false end as is_active,
coalesce(ty.year, ly.year + 1) as "year"
from last_year ly
full outer join this_year ty
on ly.actor = ty.actor)
--select * from this_year_final 
--order by actor,year
select * from final
order by actor;

--select actor,year from actors
--group by actor,year
--having cardinality(films)>1;
--
--select * from actors where actor = 'Patrick Wayne' order by year
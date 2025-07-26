--select * from actor_films af limit 10;

create type films as (
	film text,
	votes int,
	rating float,
	filmid text);
	
create type quality_class as enum ('star', 'good', 'average', 'bad');
	
--drop table actors;
create table actors (
	actor text,
	films films[],
	quality_class quality_class,
	is_active bool,
	year int,
	primary key(actor, year)
);

--select * from actors;
--drop table actors_history_scd
create table actors_history_scd (
	actor text,
	quality_class quality_class,
	is_active bool,
	start_date int,
	end_date int,
	current_year int,
	primary key(actor, start_date)
)

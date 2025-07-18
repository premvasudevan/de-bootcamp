
--drop table host_activity_reduced
create table host_activity_reduced (
	month_start date,
	host	text,
	hit_array int[],
	unique_visitors int[],
	primary key(month_start, host)
)
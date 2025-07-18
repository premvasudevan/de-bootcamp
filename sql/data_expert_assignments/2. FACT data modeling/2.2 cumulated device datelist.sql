


--drop table user_devices_cumulated
create table user_devices_cumulated ( 
	userid		numeric,
	browser_type		text,
	device_activity_datelist		date[],
	date date,
	primary key (userid, browser_type, date)
)
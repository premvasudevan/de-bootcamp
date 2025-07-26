with users as (
	select * from user_devices_cumulated udc 
	where date = '2023-01-31'
),
date_series as (
select cast(date as date) sdate from generate_series(date('2023-01-01'),date('2023-01-31'), interval '1 day') as date
),
datelist_int as (
select *,
case when 
	device_activity_datelist @> array[date_series.sdate] -- bool column to show date when user was active
	then
	power(2,32-( users.date-date_series.sdate))
	else 0
end as date_int
from users
cross join date_series)
select userid, browser_type, date,
cast(cast(sum(date_int) as bigint) as bit(32))
from datelist_int
group by userid,browser_type,date
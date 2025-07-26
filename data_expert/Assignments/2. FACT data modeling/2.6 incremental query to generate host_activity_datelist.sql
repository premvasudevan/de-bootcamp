@set ds = '2023-01-10'
insert into hosts_cumulated
with previous as (
	select *
	from hosts_cumulated
	where date = cast($ds as date) - interval '1 day'
),
current as (
	select host,
	cast(event_time as date) as date 
	from events
	where cast(event_time as date) = cast($ds as date)
	group by host, cast(event_time as date)
)
select coalesce(c.host, p.host) host,
case when p.host is null then array[c.date]
when c.host is null then p.host_activity_datelist
else array[c.date] || p.host_activity_datelist
end
 as host_datelist,
coalesce(c.date, p.date + interval '1 days') as date
from previous p 
full outer join current c
on p.host = c.host
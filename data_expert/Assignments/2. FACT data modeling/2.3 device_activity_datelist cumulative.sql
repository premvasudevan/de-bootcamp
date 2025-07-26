@set ds = '2023-01-31'
insert into user_devices_cumulated
with events_partitioned as (
	select url, referrer, host, user_id, device_id,cast(event_time as date),
	row_number() over(partition by user_id, device_id, cast(event_time as date)) as rn
	from events
),
events_deduped as (
	select url, referrer, user_id, device_id, host, event_time
	from events_partitioned
	where rn = 1),
devices_partitioned as (
	select *, row_number() over(partition by device_id) as rn
	from devices
),
devices_deduped as (
	select *
	from devices_partitioned
	where rn = 1),	
data as (
	select e.user_id, e.device_id, d.browser_type,cast(e.event_time as date) date,
	row_number() over(partition by e.user_id, d.browser_type, cast(e.event_time as date)) as rn
from events_deduped e join devices_deduped d
on e.device_id = d.device_id
where e.user_id is not null
),
data_deduped as (
	select user_id , device_id , browser_type , date 
	from data
	where rn = 1
),
previous as (
	select *
	from user_devices_cumulated
	where date =  cast($ds as date) - interval '1 days'
),
current as (
	select *
	from data_deduped
	where date = cast($ds as date)
),
--select user_id , "date" ,count(1)
--from data
--group by user_id , "date" 
--order by 3 desc
--select * from data_deduped 
--where user_id = 14411703903415400000 and date = '2023-01-08'
final_deduped as (select 
coalesce(c.user_id, p.userid) as userid,
coalesce(c.browser_type, p.browser_type ) as browser_type,
case when p.device_activity_datelist is null then array[c.date]
when c.date is null then p.device_activity_datelist
else array[c."date" ] || p.device_activity_datelist
end as device_activity_datelist,
coalesce(c."date", p.date + interval '1 days') as date,
row_number() over(partition by coalesce(c.user_id, p.userid), coalesce(c.browser_type, p.browser_type ), coalesce(c."date", p.date + interval '1 days')) as rn
from previous p
full outer join current c
on p.userid = c.user_id)
select  userid, browser_type,device_activity_datelist,date
from final_deduped
where rn = 1
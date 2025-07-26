@set ds = '2023-01-08'
insert into host_activity_reduced
with previous as (
	select * 
	from host_activity_reduced
	where month_start = cast($ds as date) - interval '1 days'
),
current as (
select host,
count(url) as site_hits,
count(distinct user_id) as unique_user_count,
cast(event_time as date) as date
from events
where user_id is not null
and cast(event_time as date) = cast($ds as date)
group by host,cast(event_time as date)
)
select 
coalesce(cast(date_trunc('month',c.date) as date), p.month_start) as date,
coalesce(c.host, p.host) as host,
case when p.host is not null then p.hit_array || array[coalesce(c.site_hits, 0)]
when p.host is null and h.hit_array is not null then h.hit_array ||  array[coalesce(c.site_hits, 0)]
when p.host is null then array_fill(0, array[cast(c.date - cast(date_trunc('month',c.date) as date) as int)]) || array[coalesce(c.site_hits, 0)]
end as hit_array,
case when p.host is not null then p.unique_visitors || array[coalesce(c.unique_user_count, 0)]
when p.host is null and h.unique_visitors is not null then h.unique_visitors || array[coalesce(c.unique_user_count, 0)]
when p.host is null then array_fill(0, array[cast(c.date - cast(date_trunc('month',c.date) as date) as int)]) || array[coalesce(c.unique_user_count, 0)]
end as unique_visitors
from previous p 
full outer join current c on p.host = c.host
full outer join host_activity_reduced h on c.host = h.host
on conflict (month_start, host)
do update set hit_array = excluded.hit_array, unique_visitors=excluded.unique_visitors
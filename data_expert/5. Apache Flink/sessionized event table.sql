--What is the average number of web events of a session from a user on Tech Creator?

select session_time, host, avg(event_count) as avg_events_per_session
from sessionized_events
where host like '%techcreator.io'
group by session_time, host



-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
-- comparing average number of events per session for given hosts
select session_time, host, avg(event_count) as avg_events_per_session
from sessionized_events
where host in ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
group by session_time, host

-- comparing result of given hosts across all sessions
select host, avg(event_count) as avg_events_per_session
from sessionized_events
where host in ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
group by host
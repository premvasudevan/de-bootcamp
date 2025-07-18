with partitioned_data as (select *, row_number() over (partition by game_id , team_id , player_id) as rn from game_details)
select * from partitioned_data where rn = 1


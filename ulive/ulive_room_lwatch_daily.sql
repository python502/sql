set hive.execution.engine=mr;
set mapreduce.reduce.shuffle.input.buffer.percent=0.6;

use ulive;
set pt=<?=date('Y-m-d', $worker_pt);?>;

create table if not exists ulive_room_lwatch_daily
(
    room_id               string
    ,room_owner_id        string
    ,owner_create_at      string
    ,owner_geo            string
    ,live_times           string
    ,viewer_id            string
    ,viewer_create_at     string
    ,viewer_geo           string
    ,watch_times          string

) partitioned by (pt string)
STORED AS ORC;


insert overwrite table ulive_room_lwatch_daily partition (pt='${hiveconf:pt}')

select x1.room_id
,room_owner_id
,owner_create_at
,owner_geo
,live_times
,viewer_id
,viewer_create_at
,viewer_geo
,if(x2.room_id is null, 0 ,watch_times) as watch_times
from
(
-- 直播用户数据
  select a.id as room_id,room_owner_id,live_times,create_at as owner_create_at,geo as owner_geo
  from
  (
    select id, room_owner_id, COALESCE(create_time-start_time, 0) as live_times
    from ulive_room_info_history
    where pt = '${hiveconf:pt}'
  ) a
  left JOIN
  (
    select id, create_at,geo from ulive_member_alive_detail where pt='${hiveconf:pt}'
  ) b
  on a.room_owner_id=b.id
)x1
left JOIN
(
-- 观看用户数据
  select room_id,member_id as viewer_id,watch_times,create_at as viewer_create_at,geo as viewer_geo
  from
  (
    select room_id, member_id, sum(if(watch_times is null, 0, watch_times)) as watch_times
    from
    (
      select room_id, member_id, COALESCE(exit_time-create_time, 0) as watch_times
      from ulive_room_member_info_history
      where pt = '${hiveconf:pt}'
    ) x
    group by room_id,member_id
  ) a
  left JOIN
  (
    select id, create_at,geo from ulive_member_alive_detail where pt='${hiveconf:pt}'
  ) b
  on a.member_id=b.id
)x2
on x1.room_id = x2.room_id
where room_owner_id<>viewer_id or room_owner_id is null or viewer_id is null
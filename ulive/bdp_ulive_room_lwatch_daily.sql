set hive.execution.engine=mr;
set mapreduce.reduce.shuffle.input.buffer.percent=0.6;
use ulive;

set pt=<?=date('Y-m-d', $worker_pt);?>;

select 'pt'                       as pt
     , 'room_id'                   as room_id
     , 'room_owner_id'             as room_owner_id
     , 'owner_create_at'           as owner_create_at
     , 'owner_geo'                 as owner_geo
     , 'live_times'                as live_times
     , 'viewer_id'                 as viewer_id
     , 'viewer_create_at'          as viewer_create_at
     , 'viewer_geo'                as viewer_geo
     , 'watch_times'               as watch_times
from avazu.t_header limit 1;

select concat('${hiveconf:pt}',' 00:00:00')
, room_id
, room_owner_id
, owner_create_at
, owner_geo
, live_times
, viewer_id
, viewer_create_at
, viewer_geo
, watch_times
FROM ulive_room_lwatch_daily
where pt='${hiveconf:pt}'
set hive.execution.engine=mr;
set mapreduce.reduce.shuffle.input.buffer.percent=0.6;
create temporary function fromUnixTimestamp as  'com.avazu.dc.hive.udf.UDFFromUnixTimestamp';

use ulive;

create table if not exists ulive_member_alive_detail
(
    id                   string,
    create_at            string,
    create_time          string,
    active_at            string,
    active_time          string,
    geo                  string,
    live_counts          string,
    live_times           string,
    watch_times          string,
    buy_amount           string,
    cost_amount          string
) partitioned by (pt string)
STORED AS ORC;

set pt=<?=date('Y-m-d', $worker_pt);?>;

insert overwrite table ulive_member_alive_detail partition (pt='${hiveconf:pt}')

SELECT x.member_id as id
,y.create_at as create_at
,y.create_time as create_time
,x.active_at as active_at
,x.active_time as active_time
,y.geo as geo
,x.live_counts as live_counts
,x.live_times as live_times
,x.watch_times as watch_times
,x.buy_amount as buy_amount
,x.cost_amount as cost_amount
FROM
(
  select if(a.member_id is not null, a.member_id, b.id) as member_id
  ,'${hiveconf:pt}' as active_at
  ,a.active_time as active_time
  ,if(b.id is not null, b.live_counts, 0) as live_counts
  ,if(b.id is not null, b.live_times, 0) as live_times
  ,if(b.id is not null, b.watch_times, 0) as watch_times
  ,if(b.id is not null, b.buy_amount, 0) as buy_amount
  ,if(b.id is not null, b.cost_amount, 0) as cost_amount
  from
  (
    select if(x.member_id is not null,x.member_id, y.id) as member_id
    ,if(x.member_id is not null,x.active_time, y.active_time) as active_time
    from
    (
        -- 用户日活表数据
      select member_id, fromUnixTimestamp(cast(max(create_time) as bigint), 'yyyy-MM-dd HH:mm:ss') as active_time from ulive_member_alive_record where pt = '${hiveconf:pt}' group by member_id
    ) as x
    FULL OUTER JOIN
    (
        -- 用户新增表数据
      select id, fromUnixTimestamp(cast(create_time as bigint), 'yyyy-MM-dd HH:mm:ss') as active_time from ulive_member where pt = '${hiveconf:pt}'
    ) as y
    on x.member_id = y.id
  ) a
  FULL OUTER JOIN
  (
    select id
    , sum(if(live_counts is null, 0 , live_counts)) as live_counts
    , sum(if(live_times is null, 0 , live_times)) as live_times
    , sum(if(watch_times is null, 0 , watch_times)) as watch_times
    , sum(if(buy_amount is null, 0 , buy_amount)) as buy_amount
    , sum(if(cost_amount is null, 0 , cost_amount)) as cost_amount
    from (
  -- 直播时长
      select room_owner_id as id
      , '1' as live_counts
      , live_times
      , '0' as watch_times
      , '0' as buy_amount
      , '0' as cost_amount
      from
      (
        select room_owner_id, 0 as live_times from ulive_room_info where pt = '${hiveconf:pt}'
        union ALL
        select room_owner_id, COALESCE(create_time-start_time, 0) as live_times from ulive_room_info_history where pt = '${hiveconf:pt}'
      )a

      union all
    -- 观看时长
      select member_id as id
      , '0' as live_counts
      , '0' as live_times
      , COALESCE(exit_time-create_time, 0) as watch_times
      , '0' as buy_amount
      , '0' as cost_amount
      from ulive_room_member_info_history
      where pt = '${hiveconf:pt}'

      union all
    -- 购买砖石
      select member_id as id
      , '0' as live_counts
      , '0' as live_times
      , '0' as watch_times
      , buy_amount
      , '0' as cost_amount
      from ulive_member_charge where pt = '${hiveconf:pt}' and pay_type in('1','2') and status='1'

      union all
    -- 消费砖石
      select give_member_id as id
      , '0' as live_counts
      , '0' as live_times
      , '0' as watch_times
      , '0' as buy_amount
      , cost_diamond_amount as cost_amount
      from ulive_gift_give_info where pt = '${hiveconf:pt}'
    )n
    group by id
  ) b
  on a.member_id = b.id
) x
JOIN
(
  -- 用户全表数据 得到create_at,geo
  select id, create_at, create_time, geo from ulive_member_all where pt = '${hiveconf:pt}'
)y
on x.member_id=y.id
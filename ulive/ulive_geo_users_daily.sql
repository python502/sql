set hive.execution.engine=mr;
set mapreduce.reduce.shuffle.input.buffer.percent=0.6;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

use ulive;
set pt=<?=date('Y-m-d', $worker_pt);?>;

create table if not exists ulive_geo_users_daily
(
    geo               string
    ,dnu               bigint
    ,dau               bigint
    ,dlu               bigint
    ,nlu               bigint
    ,live_times        bigint
    ,watch_times       bigint
    ,buy_amount        bigint
    ,cost_amount       bigint
    ,retains_1         bigint
    ,retains_2         bigint
    ,retains_3         bigint
    ,retains_7         bigint
    ,retains_14        bigint
    ,retains_21        bigint
    ,retains_30        bigint
) PARTITIONED BY(pt string, type string)
stored AS orc;


insert overwrite table ulive_geo_users_daily partition (pt, type)
select geo
    , sum(dnu) as dnu, sum(dau) as dau, sum(dlu) as dlu, sum(nlu) as nlu
    , sum(live_times) as live_times, sum(watch_times) as watch_times
    , sum(buy_amount) as buy_amount, sum(cost_amount) as cost_amount
    , sum(retain_1) as retain_1, sum(retain_2) as retain_2, sum(retain_3) as retain_3
    , sum(retain_7) as retain_7, sum(retain_14) as retain_14, sum(retain_21) as retain_21, sum(retain_30) as retain_30
    , pt, type

from
(
    select '${hiveconf:pt}' as pt
        , '0' as type,  geo as geo
        , sum(if(pt= create_at, 1, 0))   as dnu
        , sum(if(pt= active_at, 1, 0))   as dau
        , sum(if(live_counts<>0,1,0))    as dlu
        , sum(if(live_counts<>0 and pt= create_at,1,0))  as nlu
        , sum(live_times) as live_times, sum(watch_times) as watch_times
        , sum(buy_amount) as buy_amount, sum(cost_amount) as cost_amount
        , 0 as retain_1, 0 as retain_2, 0 as retain_3, 0 as retain_7, 0 as retain_14, 0 as retain_21, 0 as retain_30
    from ulive.ulive_member_alive_detail
    where pt = '${hiveconf:pt}' and active_at=pt
        and create_at is not null and create_at <> '' and create_at <> 'null' and create_at <> 'NULL'
        and active_at is not null and active_time <> '' and active_time <> 'null' and active_time <> 'NULL'
    group by geo

    union all

    select create_at as pt
         , type
         , geo
         , 0 as dnu, 0 as dau, 0 as dlu, 0 as nlu
         , 0 as live_times, 0 as watch_times, 0 as buy_amount, 0 as cost_amount
         , sum(if(type = '1', 1, 0)) as retain_1
         , sum(if(type = '2', 1, 0)) as retain_2
         , sum(if(type = '3', 1, 0)) as retain_3
         , sum(if(type = '7', 1, 0)) as retain_7
         , sum(if(type = '14', 1, 0)) as retain_14
         , sum(if(type = '21', 1, 0)) as retain_21
         , sum(if(type = '30', 1, 0)) as retain_30
    from
    (
        select geo
            , create_at
            , cast(datediff(active_time, create_at) as bigint) as type
        from ulive.ulive_member_alive_detail
        where pt = '${hiveconf:pt}' and active_at = pt
        and create_at is not null and create_at <> '' and create_at <> 'null' and create_at <> 'NULL'
        and active_at is not null and active_time <> '' and active_time <> 'null' and active_time <> 'NULL'
    )x2 where type in ('1','2','3','7','14','21','30')
    group by create_at, type, geo
) bb
group by geo, pt, type;
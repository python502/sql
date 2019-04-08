set hive.execution.engine=mr;
set mapreduce.reduce.shuffle.input.buffer.percent=0.6;
use ulive;

set pt=<?=date('Y-m-d', $worker_pt);?>;

set zero0=<?=date('Y-m-d', $worker_pt-0*86400);?>;
set zero1=<?=date('Y-m-d', $worker_pt-1*86400);?>;
set zero2=<?=date('Y-m-d', $worker_pt-2*86400);?>;
set zero3=<?=date('Y-m-d', $worker_pt-3*86400);?>;
set zero7=<?=date('Y-m-d', $worker_pt-7*86400);?>;
set zero14=<?=date('Y-m-d', $worker_pt-14*86400);?>;
set zero21=<?=date('Y-m-d', $worker_pt-21*86400);?>;
set zero30=<?=date('Y-m-d', $worker_pt-30*86400);?>;

set one0=<?=date('Y-m-d', $worker_pt-1*86400);?>;
set one1=<?=date('Y-m-d', $worker_pt-2*86400);?>;
set one2=<?=date('Y-m-d', $worker_pt-3*86400);?>;
set one3=<?=date('Y-m-d', $worker_pt-4*86400);?>;
set one7=<?=date('Y-m-d', $worker_pt-8*86400);?>;
set one14=<?=date('Y-m-d', $worker_pt-15*86400);?>;
set one21=<?=date('Y-m-d', $worker_pt-22*86400);?>;
set one30=<?=date('Y-m-d', $worker_pt-31*86400);?>;


set two0=<?=date('Y-m-d', $worker_pt-2*86400);?>;
set two1=<?=date('Y-m-d', $worker_pt-3*86400);?>;
set two2=<?=date('Y-m-d', $worker_pt-4*86400);?>;
set two3=<?=date('Y-m-d', $worker_pt-5*86400);?>;
set two7=<?=date('Y-m-d', $worker_pt-9*86400);?>;
set two14=<?=date('Y-m-d', $worker_pt-16*86400);?>;
set two21=<?=date('Y-m-d', $worker_pt-23*86400);?>;
set two30=<?=date('Y-m-d', $worker_pt-32*86400);?>;


set thr0=<?=date('Y-m-d', $worker_pt-3*86400);?>;
set thr1=<?=date('Y-m-d', $worker_pt-4*86400);?>;
set thr2=<?=date('Y-m-d', $worker_pt-5*86400);?>;
set thr3=<?=date('Y-m-d', $worker_pt-6*86400);?>;
set thr7=<?=date('Y-m-d', $worker_pt-10*86400);?>;
set thr14=<?=date('Y-m-d', $worker_pt-17*86400);?>;
set thr21=<?=date('Y-m-d', $worker_pt-24*86400);?>;
set thr30=<?=date('Y-m-d', $worker_pt-33*86400);?>;

select "pt" as pt
    , "geo"                as geo
    , "dnu"                as dnu
    , "dau"                as dau
    , "dlu"                as dlu
    , "nlu"                as nlu
    , "live_times"         as live_times
    , "watch_times"        as watch_times
    , "buy_amount"         as buy_amount
    , "cost_amount"        as cost_amount
    , "retains_1"          as retains_1
    , "retains_2"          as retains_2
    , "retains_3"          as retains_3
    , "retains_7"          as retains_7
    , "retains_14"         as retains_14
    , "retains_21"         as retains_21
    , "retains_30"         as retains_30
from avazu.t_header limit 1;


select   concat(pt,' 00:00:00')
        ,geo
        ,sum(if(dnu is not null,dnu,0)) as dnu
        ,sum(if(dau is not null,dau,0)) as dau
        ,sum(if(dlu is not null,dlu,0)) as dlu
        ,sum(if(nlu is not null,nlu,0)) as nlu
        ,sum(if(live_times is not null,live_times,0)) as live_times
        ,sum(if(watch_times is not null,watch_times,0)) as watch_times
        ,sum(if(buy_amount is not null,buy_amount,0)) as buy_amount
        ,sum(if(cost_amount is not null,cost_amount,0)) as cost_amount
        ,sum(if(retains_1 is not null,retains_1,0)) as retains_1
        ,sum(if(retains_2 is not null,retains_2,0)) as retains_2
        ,sum(if(retains_3 is not null,retains_3,0)) as retains_3
        ,sum(if(retains_7 is not null,retains_7,0)) as retains_7
        ,sum(if(retains_14 is not null,retains_14,0)) as retains_14
        ,sum(if(retains_21 is not null,retains_21,0)) as retains_21
        ,sum(if(retains_30 is not null,retains_30,0)) as retains_30
from ulive_geo_users_daily
where pt in ('${hiveconf:zero0}', '${hiveconf:zero1}', '${hiveconf:zero2}', '${hiveconf:zero3}', '${hiveconf:zero7}', '${hiveconf:zero14}', '${hiveconf:zero21}', '${hiveconf:zero30}'
              ,'${hiveconf:one0}', '${hiveconf:one1}', '${hiveconf:one2}', '${hiveconf:one3}', '${hiveconf:one7}', '${hiveconf:one14}', '${hiveconf:one21}', '${hiveconf:one30}'
               ,'${hiveconf:two0}', '${hiveconf:two1}', '${hiveconf:two2}', '${hiveconf:two3}', '${hiveconf:two7}', '${hiveconf:two14}', '${hiveconf:two21}', '${hiveconf:two30}'
               ,'${hiveconf:thr0}', '${hiveconf:thr1}', '${hiveconf:thr2}', '${hiveconf:thr3}', '${hiveconf:thr7}', '${hiveconf:thr14}', '${hiveconf:thr21}', '${hiveconf:thr30}')
group by pt,geo;


set hive.execution.engine=mr;
set mapreduce.reduce.shuffle.input.buffer.percent=0.6;
use ulive;

set pt=<?=date('Y-m-d', $worker_pt);?>;

select 'pt'          as pt
     , 'id'           as id
     , 'member_id'    as member_id
     , 'geo'          as geo
     , 'pay_type'     as pay_type
     , 'order_no'     as order_no
     , 'buy_amount'   as buy_amount
     , 'money'        as money
     , 'create_time'  as create_time
     , 'status'       as status
from avazu.t_header limit 1;

select /*+ MAPJOIN(b)*/ concat('${hiveconf:pt}',' 00:00:00')
,a.id as id
,a.member_id as member_id
,b.abbreviation as geo
,a.pay_type as pay_type
,a.order_no as order_no
,a.buy_amount as buy_amount
,a.money as money
,a.create_time as create_time
,a.status as status
from
(
  select id, member_id, pay_type ,order_no ,buy_amount ,money ,create_time,status,language from ulive_member_charge where pt = '${hiveconf:pt}'
) a
left join
(
  select language,abbreviation from ulive_country where pt = '${hiveconf:pt}'
) b
on a.language=b.language
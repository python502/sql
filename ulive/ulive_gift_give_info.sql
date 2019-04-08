set hive.execution.engine=mr;
set mapreduce.reduce.shuffle.input.buffer.percent=0.6;
use ulive;

set pt=<?=date('Y-m-d', $worker_pt);?>;

select 'pt'                       as pt
     , 'id'                        as id
     , 'give_member_id'            as give_member_id
     , 'geo'                       as geo
     , 'receive_member_id'         as receive_member_id
     , 'gift_id'                   as gift_id
     , 'gift_name'                 as gift_name
     , 'give_amount'               as give_amount
     , 'gain_gold_amount'          as gain_gold_amount
     , 'cost_diamond_amount'       as cost_diamond_amount
     , 'create_time'               as create_time

from avazu.t_header limit 1;

select /*+ MAPJOIN(b)*/ concat('${hiveconf:pt}',' 00:00:00')
,a.id as id
,a.give_member_id as give_member_id
,b.abbreviation as geo
,a.receive_member_id as receive_member_id
,a.gift_id as gift_id
,a.gift_name as gift_name
,a.give_amount as give_amount
,a.gain_gold_amount as gain_gold_amount
,a.cost_diamond_amount as cost_diamond_amount
,a.create_time as create_time
from
(
  select id, give_member_id, receive_member_id ,gift_id ,gift_name ,give_amount ,gain_gold_amount, cost_diamond_amount, create_time,language from ulive_gift_give_info where pt = '${hiveconf:pt}'
) a
left join
(
  select language,abbreviation from ulive_country where pt = '${hiveconf:pt}'
) b
on a.language=b.language
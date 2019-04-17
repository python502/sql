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
     , 'receive_member_geo'        as receive_member_geo
     , 'registered_time'             as registered_time
from avazu.t_header limit 1;

select concat('${hiveconf:pt}',' 00:00:00')
,a.id as id
,a.give_member_id as give_member_id
,b.abbreviation as geo
,a.receive_member_id as receive_member_id
,a.gift_id as gift_id
,c.gift_name as gift_name
,a.give_amount as give_amount
,a.gain_gold_amount as gain_gold_amount
,a.cost_diamond_amount as cost_diamond_amount
,a.create_time as create_time
,d.geo as receive_member_geo
,d.create_at as registered_at
from
(
  select id, give_member_id, receive_member_id ,gift_id ,gift_name ,give_amount ,gain_gold_amount, cost_diamond_amount, create_time,language from ulive_gift_give_info where pt = '${hiveconf:pt}'
) a
left join
(
  select language,abbreviation from ulive_country where pt = '${hiveconf:pt}'
) b
on a.language=b.language
left join
(
  select id,style_name as gift_name from ulive_gift_info where pt = '${hiveconf:pt}'
) c
on a.gift_id=c.id
left join
(
  select id,geo,create_at from ulive_member_alive_detail where pt = '${hiveconf:pt}'
) d
on a.receive_member_id=d.id
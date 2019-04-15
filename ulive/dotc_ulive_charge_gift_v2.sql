TEMP tablejoin1 =  SELECT

from bdp_dotc_app_geo_users_daily
where app in ['UnicoLiveAndroidTEMP tablejoin1 =
select
A.[pt] as pt,
A.[用户国家] as geo,
A.[money] as charge_money,
A.[buy_amount] as charge_diamond_amount,
0 as transfer_diamond_amount,
0 as transfer_gold_amount,
0 as cost_diamond_amount,
0 as gain_gold_amount
from [dotc_ulive_member_charge] as A
where A.[支付状态] = 1 and A.[支付类型] in (1,2)

union all
select
B.[pt] as pt,
B.[用户国家] as geo,
0 as charge_money,
0 as charge_diamond_amount,
B.[buy_amount] as transfer_diamond_amount,
B.[money] as transfer_gold_amount,
0 as cost_diamond_amount,
0 as gain_gold_amount
from [dotc_ulive_member_charge] as B
where B.[支付状态] = 1 and B.[支付类型] = 0


union all
select
C.[pt] as pt,
C.[用户国家] as geo,
0 as charge_money,
0 as charge_diamond_amount,
0 as transfer_diamond_amount,
0 as transfer_gold_amount,
C.[消费钻石] as cost_diamond_amount,
0 as gain_gold_amount
from [dotc_ulive_gift_give_info] as C

union all

select
D.[pt] as pt,
D.[主播国家] as geo,
0 as charge_money,
0 as charge_diamond_amount,
0 as transfer_diamond_amount,
0 as transfer_gold_amount,
0 as cost_diamond_amount,
D.[获得金币] as gain_gold_amount
from [dotc_ulive_gift_give_info] as D




OUTPUT select
A.[pt],
A.[geo],
sum(COALESCE(A.[charge_money],0)) as charge_money,
sum(COALESCE(A.[charge_diamond_amount],0)) as charge_diamond_amount,
sum(COALESCE(A.[transfer_diamond_amount],0)) as transfer_diamond_amount,
sum(COALESCE(A.[transfer_gold_amount],0)) as transfer_gold_amount,
sum(COALESCE(A.[cost_diamond_amount],0)) as cost_diamond_amount,
sum(COALESCE(A.[gain_gold_amount],0)) as gain_gold_amount
from [tablejoin1] as A
group by
A.[pt],
A.[geo]']
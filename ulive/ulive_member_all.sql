set hive.execution.engine=mr;
set mapreduce.reduce.shuffle.input.buffer.percent=0.6;
create temporary function fromUnixTimestamp as  'com.avazu.dc.hive.udf.UDFFromUnixTimestamp';

use ulive;

create table if not exists ulive_member_all
(
    id                   string,
    create_at            string,
    create_time          string,
    geo                  string
) partitioned by (pt string)
STORED AS ORC;

set pt=<?=date('Y-m-d', $worker_pt);?>;
set pt_one=<?=date('Y-m-d', $worker_pt-86400);?>;
set ct=<?=date('Y-m-d', $worker_pt - 30*86400);?>;

insert overwrite table ulive_member_all partition (pt='${hiveconf:pt}')

select if(today.id is not null, today.id, yestoday.id) as id
,if(today.id is not null, today.create_at, yestoday.create_at) as create_at
,if(today.id is not null, today.create_time, yestoday.create_time) as create_time
,if(today.id is not null, today.geo, yestoday.geo) as geo
from
(
  select /*+ MAPJOIN(b)*/ a.id, a.create_at,a.create_time,b.abbreviation as geo
  from
  (
    select id, pt as create_at, fromUnixTimestamp(cast(create_time as bigint), 'yyyy-MM-dd hh:mm:ss') as create_time, language from ulive_member where pt = '${hiveconf:pt}'
  ) a
  left join
  (
    select language,abbreviation from ulive_country where pt = '${hiveconf:pt}'
  ) b
  on a.language=b.language
) today
full outer join
(
  select id, create_at, create_time, geo from ulive_member_all  where pt = '${hiveconf:pt_one}'
) yestoday
on today.id=yestoday.id;


alter table ulive_member_all drop partition (pt = '${hiveconf:ct}');
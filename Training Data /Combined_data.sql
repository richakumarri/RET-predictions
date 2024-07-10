

create  table scratch.riders.final_prediction_feat_set_training  as
with base as
(
select 
 distinct 
 zone_code
 ,city_name
 ,DATE_TRUNC(month, Date (START_OF_PERIOD_LOCAL)) AS order_month
 ,CAST(EXTRACT(HOUR FROM START_OF_PERIOD_LOCAL) AS INT) as hour_of_day
 ,DAYNAME ( START_OF_PERIOD_LOCAL) as week_of_Day
 ,round(COALESCE(SUM(RET_MINS_SUM ), 0) / NULLIF(COALESCE(SUM(ret_mins_cnt ), 0), 0),2) AS RET_AVG_june -- remove this in future 
from 
PRODUCTION.AGGREGATE.AGG_ZONE_DELIVERY_METRICS_HOURLY
where
date (start_of_period_local) between   '2023-12-01' and '2024-05-30'
and is_within_zone_hours =true
and CNT_ERAT >0 
 group by 1,2,3,4,5
)
-- select count(*), count(distinct zone_code, hour_of_day, week_of_day, order_month) from base;--146511

select
base.* 
,day_feat.* exclude (city_name, zone_code,order_month )
, hour_feat.* exclude (city_name, zone_code, order_month,DELIVERED_HOUR_OF_DAY )
, WOD_feat.* exclude (city_name, zone_code, order_month,DELIVERED_HOUR_OF_DAY,WEEK_OF_DAY )
from 

base 
left join
scratch.riders.prediction_query_day_level_train as day_feat
on base.ZONE_CODE =day_feat.zone_code
and base.order_month =day_feat.order_month

left join
scratch.riders.prediction_query_hour_day_level_train as hour_feat
on base.zone_code =hour_feat.zone_code
and base.HOUR_OF_DAY =hour_feat.DELIVERED_HOUR_OF_DAY
and base.order_month =hour_feat.order_month

left join
scratch.riders.prediction_query_WOD_hour_day_level_training as WOD_feat
on base.zone_code =WOD_feat.zone_code
and base.HOUR_OF_DAY =WOD_feat.DELIVERED_HOUR_OF_DAY
and base.WEEK_OF_DAY =WOD_feat.WEEK_OF_DAY
and base.order_month=WOD_feat.order_month
;


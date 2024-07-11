

create or replace table scratch.riders.final_prediction_feat_set as
with base as
(
select 
 distinct 
 zone_code
 ,city_name
 ,CAST(EXTRACT(HOUR FROM START_OF_PERIOD_LOCAL) AS INT) as hour_of_day
 ,DAYNAME ( START_OF_PERIOD_LOCAL) as week_of_Day
from 
PRODUCTION.AGGREGATE.AGG_ZONE_DELIVERY_METRICS_HOURLY
where
date (start_of_period_local) between   current_date-10 and current_date
and is_within_zone_hours =true
 group by 1,2,3,4
)

select
base.* 
,day_feat.* exclude (city_name, zone_code,order_month )
, hour_feat.* exclude (city_name, zone_code, order_month,DELIVERED_HOUR_OF_DAY )
, WOD_feat.* exclude (city_name, zone_code, order_month,DELIVERED_HOUR_OF_DAY,WEEK_OF_DAY )
from 

base 
left join
scratch.riders.prediction_query_day_level as day_feat
on base.ZONE_CODE =day_feat.zone_code

left join
scratch.riders.prediction_query_hour_day_level as hour_feat
on base.zone_code =hour_feat.zone_code
and base.HOUR_OF_DAY =hour_feat.DELIVERED_HOUR_OF_DAY

left join
scratch.riders.prediction_query_WOD_hour_day_level as WOD_feat
on base.zone_code =WOD_feat.zone_code
and base.HOUR_OF_DAY =WOD_feat.DELIVERED_HOUR_OF_DAY
and base.WEEK_OF_DAY =WOD_feat.WEEK_OF_DAY
;

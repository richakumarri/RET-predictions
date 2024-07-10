
SET start_date =  '2023-08-01' ;--2024-04-01;
SET current_month ='2024-05-01' ;--2024-07-01

create  table scratch.riders.prediction_query_WOD_hour_day_level_training as
with base as
(
select
   ID
   ,date(LOCAL_TIME_CREATED_AT)as order_dt   
   ,CAST(EXTRACT(HOUR FROM TO_TIMESTAMP(LOCAL_TIME_CREATED_AT )) AS INT)as order_created_hour
   , DATE_TRUNC(month, Date (LOCAL_TIME_CREATED_AT)) AS order_month 
  ,DAYNAME ( LOCAL_TIME_CREATED_AT) as week_of_Day
    ,CAST(EXTRACT(HOUR FROM TO_TIMESTAMP_NTZ(coalesce(LOCAL_TIME_TARGET_READY_AT, LOCAL_TIME_DELIVERED_AT,LOCAL_TIME_CREATED_AT) )) AS INT) as Delivered_hour_of_day --use this for TP
   ,ZONE_CODE
   ,City_name
   ,ASAP_ORDER_DURATION as AOD 
   ,ASAP_ESTIMATED_ORDER_DURATION  as EOD
   ,DATEDIFF(minute, LOCAL_TIME_RA_ACKNOWLEDGED_AT,LOCAL_TIME_RA_CONFIRMED_AT ) as rider_to_restaurant_mins
   ,DATEDIFF(minute, LOCAL_TIME_SUBMITTED_AT,LOCAL_TIME_TARGET_READY_AT ) as FPT
   ,WAIT_AT_CUSTOMER
   ,DATEDIFF(minute, LOCAL_TIME_OA_RECEIVED_AT,LOCAL_TIME_DELIVERED_AT ) resto_customer_mins
   ,LATENESS -- (difference between EOD and AOD,ASAP_ESTIMATED_ORDER_DURATION-ASAP_ORDER_DURATION)
   ,case when LATENESS>=5 then 1 else 0 end as late_by_5_mins
   ,case when LATENESS>=10 then 1 else 0 end as late_by_10_mins
   ,TEMPERATURE
   ,ERAT  
   from 
    PRODUCTION.denormalised.ORDERS as ordrs 
where  
    status ='DELIVERED'  
    and order_type != 'REDELIVERY'   
    and fulfillment_type='Deliveroo' 
    AND order_fulfillment = 'Deliveroo Rider' 
    and order_date between  $start_date and $current_month
   and  date (LOCAL_TIME_PREP_FOR)between  $start_date and $current_month
)
, distinct_hour as
( 
select distinct Delivered_hour_of_day , week_of_day
from base
)
,master_table_WOD as
(
   select 
   distinct 
   city_name
   ,zone_code
   ,order_month
   ,b.Delivered_hour_of_day 
   ,b.week_of_day 
   from base as a
   left join distinct_hour  as b
    on 1=1
)
, rolling_180_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,a.week_of_day as WOD
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.week_of_day as week_of_day_b
,b.ID
,b.EOD
,b.Rider_to_restaurant_mins
from master_table_WOD as a
left join
base as b
on b.order_dt > DATEADD(day,-181, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.week_of_day=b.week_of_day
and a.zone_code=b.zone_code

)

,final_180_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,WOD
,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY EOD)as p50_EOD_last_180_dys_same_hr_WOD
,PERCENTILE_CONT( 0.2) WITHIN GROUP (ORDER BY RIDER_TO_RESTAURANT_MINS)as p20_RIDER_TO_RESTAURANT_MINS_last_180_dys_same_hr_WOD
from 
rolling_180_days
group by 1,2,3,4,5
)
, rolling_90_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,a.week_of_day as WOD
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.week_of_day as week_of_day_b
,b.ID
,b.RESTO_CUSTOMER_MINS
,b.ERAT
from master_table_WOD as a
left join
base as b
on b.order_dt > DATEADD(day,-91, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.week_of_day=b.week_of_day
and a.zone_code=b.zone_code
)
,final_90_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,WOD
,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY ERAT)as p10_ERAT_last_90_dys_same_hr_WOD
,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY RESTO_CUSTOMER_MINS)as p70_RESTO_CUSTOMER_MINS_last_90_dys_same_hr_WOD
from 
rolling_90_days
group by 1,2,3,4,5
)
, rolling_14_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,a.week_of_day as WOD
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.week_of_day as week_of_day_b
,b.ID
,b.ERAT
,b.RIDER_TO_RESTAURANT_MINS
from master_table_WOD as a
left join
base as b
on b.order_dt > DATEADD(day,-15, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.week_of_day=b.week_of_day
and a.zone_code=b.zone_code
)

,final_14_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,WOD
,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY RIDER_TO_RESTAURANT_MINS)as p10_RIDER_TO_RESTAURANT_MINS_last_14_dys_same_hr_WOD
from 
rolling_14_days
group by 1,2,3,4,5
)
, rolling_5_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,a.week_of_day as WOD
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.week_of_day as week_of_day_b
,b.ID
,b.FPT
from master_table_WOD as a
left join
base as b
on b.order_dt > DATEADD(day,-6, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.week_of_day=b.week_of_day
and a.zone_code=b.zone_code
)


,final_5_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,WOD
,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY FPT)as p10_FPT_last_5_dys_same_hr_WOD
from 
rolling_5_days
group by 1,2,3,4,5
)

,summary as(
select
base.* 
,p50_EOD_last_180_dys_same_hr_WOD
,p20_RIDER_TO_RESTAURANT_MINS_last_180_dys_same_hr_WOD
,p10_ERAT_last_90_dys_same_hr_WOD
,p70_RESTO_CUSTOMER_MINS_last_90_dys_same_hr_WOD
,p10_RIDER_TO_RESTAURANT_MINS_last_14_dys_same_hr_WOD
,p10_FPT_last_5_dys_same_hr_WOD
from 
master_table_WOD as base

 left join
 final_180_days as last_180_days
 on base.ZONE_CODE= last_180_days.ZONE_CODE
 and base.ORDER_MONTH=last_180_days.ORDER_MONTH
 and base.DELIVERED_HOUR_OF_DAY=last_180_days.delivered_hour
 and base.WEEK_OF_DAY =last_180_days.WOD
 
 left join
 final_90_days as last_90_days
 on base.ZONE_CODE= last_90_days.ZONE_CODE
 and base.ORDER_MONTH=last_90_days.ORDER_MONTH
 and base.DELIVERED_HOUR_OF_DAY=last_90_days.delivered_hour
 and base.WEEK_OF_DAY =last_90_days.WOD
 
 left join
 final_14_days as last_14_days
  on base.ZONE_CODE= last_14_days.ZONE_CODE
 and base.ORDER_MONTH=last_14_days.ORDER_MONTH
 and base.DELIVERED_HOUR_OF_DAY=last_14_days.delivered_hour
 and base.WEEK_OF_DAY =last_14_days.WOD
 
 left join
 final_5_days as last_5_days
  on base.ZONE_CODE= last_5_days.ZONE_CODE
 and base.ORDER_MONTH=last_5_days.ORDER_MONTH
 and base.DELIVERED_HOUR_OF_DAY=last_5_days.delivered_hour
 and base.WEEK_OF_DAY =last_5_days.WOD
)
select * from summary where order_month >=' 2023-12-01' ;



-- select count(*), count(distinct zone_code, order_month,DELIVERED_HOUR_OF_DAY, WEEK_OF_DAY ) from scratch.riders.prediction_query_WOD_hour_day_level_training  order by 1-- 202776


SET start_date =  '2023-08-01' ;--2024-04-01;
SET current_month ='2024-05-01' ;--2024-07-01

create  table scratch.riders.prediction_query_hour_day_level_train as

with base as
(
select
   ID
   ,date(LOCAL_TIME_CREATED_AT)as order_dt   
   ,CAST(EXTRACT(HOUR FROM TO_TIMESTAMP(LOCAL_TIME_CREATED_AT )) AS INT)as order_created_hour
   , DATE_TRUNC(month, Date (LOCAL_TIME_CREATED_AT)) AS order_month 
    ,DATE_TRUNC(week, Date (LOCAL_TIME_CREATED_AT)) AS order_week  
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
select distinct Delivered_hour_of_day 
from base
)
,master_table_hourly as
(
select 
distinct 
city_name
,zone_code
,order_month
,b.Delivered_hour_of_day 
from base as a
left join distinct_hour  as b
on 1=1
)
,rolling_180_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.ID
,b.AOD
from master_table_hourly as a
left join
base as b
on b.order_dt > DATEADD(day,-181, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)


,final_180_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,PERCENTILE_CONT( 0.9) WITHIN GROUP (ORDER BY AOD)as p90_AOD_last_180_dys_same_hr
from 
rolling_180_days
group by 1,2,3,4
)

,rolling_90_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.ID
,b.ERAT
,b.Rider_to_restaurant_mins
from master_table_hourly as a
left join
base as b
on b.order_dt > DATEADD(day,-91, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)
, final_90_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,avg(ERAT)as avg_ERAT_last_90_dys_same_hr
,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY RIDER_TO_RESTAURANT_MINS)as p70_RIDER_TO_RESTAURANT_MINS_last_90_dys_same_hr
from 
rolling_90_days
group by 1,2,3,4
)
,rolling_21_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.ID
,b.ERAT
from master_table_hourly as a
left join
base as b
on b.order_dt > DATEADD(day,-22, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)
,final_21_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,avg(ERAT)as avg_ERAT_last_21_dys_same_hr
from 
rolling_21_days
group by 1,2,3,4
)
,rolling_14_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.ID
,b.ERAT
,b.AOD
,b.RESTO_CUSTOMER_MINS
,b.TEMPERATURE
from master_table_hourly as a
left join
base as b
on b.order_dt > DATEADD(day,-15, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)
,final_14_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY AOD)as p50_AOD_last_14_dys_same_hr
,avg(ERAT)as avg_ERAT_last_14_dys_same_hr
,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY RESTO_CUSTOMER_MINS)as p70_RESTO_CUSTOMER_MINS_last_14_dys_same_hr
,avg(TEMPERATURE)as avg_TEMPERATURE_last_14_dys_same_hr
from 
rolling_14_days
group by 1,2,3,4
)
,rolling_5_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.ID
,b.TEMPERATURE
from master_table_hourly as a
left join
base as b
on b.order_dt > DATEADD(day,-6, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)
,final_5_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,avg(TEMPERATURE)as avg_TEMPERATURE_last_5_dys_same_hr
from 
rolling_5_days
group by 1,2,3,4
)
,rolling_3_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,a.Delivered_hour_of_day  as delivered_hour
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.Delivered_hour_of_day as Delivered_hour_b
,b.ID
,b.WAIT_AT_CUSTOMER
from master_table_hourly as a
left join
base as b
on b.order_dt > DATEADD(day,-4, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.Delivered_hour_of_day=b.Delivered_hour_of_day
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)
,final_3_days as
(
select
zone_code
,city_name
,order_month
,delivered_hour
,avg(WAIT_AT_CUSTOMER)as avg_WAIT_AT_CUSTOMER_last_3_dys_same_hr
from 
rolling_3_days
group by 1,2,3,4
)

,summary as
(
select
base.* 
,p90_AOD_last_180_dys_same_hr
,avg_ERAT_last_90_dys_same_hr
,p70_RIDER_TO_RESTAURANT_MINS_last_90_dys_same_hr
,avg_ERAT_last_21_dys_same_hr
,p50_AOD_last_14_dys_same_hr
,avg_ERAT_last_14_dys_same_hr
,p70_RESTO_CUSTOMER_MINS_last_14_dys_same_hr
,avg_TEMPERATURE_last_14_dys_same_hr
,avg_TEMPERATURE_last_5_dys_same_hr
,avg_WAIT_AT_CUSTOMER_last_3_dys_same_hr

from 
master_table_hourly as base

left join
final_180_days as last_180_days
on base.zone_code =last_180_days.zone_code
and base.ORDER_MONTH =last_180_days.ORDER_MONTH
and base.DELIVERED_HOUR_OF_DAY =last_180_days.delivered_hour

left join
final_90_days as last_90_days
on base.zone_code =last_90_days.zone_code
and base.ORDER_MONTH =last_90_days.ORDER_MONTH
and base.DELIVERED_HOUR_OF_DAY =last_90_days.delivered_hour

left join
final_21_days as last_21_days
on base.zone_code =last_21_days.zone_code
and base.ORDER_MONTH =last_21_days.ORDER_MONTH
and base.DELIVERED_HOUR_OF_DAY =last_21_days.delivered_hour

left join
final_14_days as last_14_days
on base.zone_code =last_14_days.zone_code
and base.ORDER_MONTH =last_14_days.ORDER_MONTH
and base.DELIVERED_HOUR_OF_DAY =last_14_days.delivered_hour

left join
final_5_days as last_5_days
on base.zone_code =last_5_days.zone_code
and base.ORDER_MONTH =last_5_days.ORDER_MONTH
and base.DELIVERED_HOUR_OF_DAY =last_5_days.delivered_hour

left join
final_3_days as last_3_days
on base.zone_code =last_3_days.zone_code
and base.ORDER_MONTH =last_3_days.ORDER_MONTH
and base.DELIVERED_HOUR_OF_DAY =last_3_days.delivered_hour
)

 select * from summary  where order_month >=' 2023-12-01' -- remove the data prior to this



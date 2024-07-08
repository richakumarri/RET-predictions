create  table scratch.riders.prediction_query_day_level as

SET start_date =  dateadd('month', -3, date_trunc('month',  current_date())) ;--2024-04-01;
SET end_date   = LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH);
SET current_month =DATE_TRUNC('MONTH', CURRENT_DATE) ;--2024-07-01


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
, master_table_daily as
(
select 
distinct 
city_name
,zone_code
,order_month
from base
)
, rolling_14_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.ID
,b.late_by_5_mins
from master_table_daily as a
left join
base as b
on b.order_dt > DATEADD(day,-15, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)

, rolling_90_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.ID
,b.AOD
,b.ERAT
,b.wait_at_customer
,b.Rider_to_restaurant_mins
,b.resto_customer_mins
,b.temperature
from master_table_daily as a
left join
base as b
on b.order_dt > DATEADD(day,-91, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)

, rolling_30_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.ID
,b.Rider_to_restaurant_mins
from master_table_daily as a
left join
base as b
on b.order_dt > DATEADD(day,-31, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)

, rolling_21_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.ID
,b.FPT
from master_table_daily as a
left join
base as b
on b.order_dt > DATEADD(day,-22, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)

, rolling_5_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.ID
,b.ERAT
from master_table_daily as a
left join
base as b
on b.order_dt > DATEADD(day,-6, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)

, rolling_3_days as
(
select 
a.zone_code as zone_code
,a.city_name as city_name
,a.order_month as order_month
,b.zone_code as zone_code_b
,b.city_name as city_name_b
,b.order_dt as ordr_dt_b
,b.ID
,b.late_by_10_mins
from master_table_daily as a
left join
base as b
on b.order_dt > DATEADD(day,-4, a.order_month)
and b.order_dt <=DATEADD(day,-1, a.order_month)
and a.zone_code=b.zone_code
and a.city_name =b.city_name
)

, final_14_days as
( 
select
a.zone_code
,a.city_name
,a.order_month    
,sum(LATE_BY_5_MINS)/ count(distinct ID)as late_by_5_mins_perc_last_14_dys
from 
rolling_14_days  as a
group by 1,2,3
)
, final_90_days as
(
select
a.zone_code
,a.city_name
,a.order_month    
,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY RESTO_CUSTOMER_MINS)as p50_RESTO_CUSTOMER_MINS_last_90_dys
,avg(WAIT_AT_CUSTOMER)as avg_WAIT_AT_CUSTOMER_last_90_dys
,avg(RIDER_TO_RESTAURANT_MINS)as avg_RIDER_TO_RESTAURANT_MINS_last_90_dys
,avg(TEMPERATURE)as avg_TEMPERATURE_last_90_dys
from 
rolling_90_days  as a
group by 1,2,3
)
, final_30_days as
(
select
a.zone_code
,a.city_name
,a.order_month    
,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY RIDER_TO_RESTAURANT_MINS)as p10_RIDER_TO_RESTAURANT_MINS_last_30_dys
from 
rolling_30_days  as a
group by 1,2,3
)
, final_21_days as
(
select
a.zone_code
,a.city_name
,a.order_month    
,avg(FPT)as avg_FPT_last_21_dys
from 
rolling_21_days  as a
group by 1,2,3
)
, final_5_days as
(
select
a.zone_code
,a.city_name
,a.order_month    
,avg(ERAT)as avg_ERAT_last_5_dys
from 
rolling_5_days  as a
group by 1,2,3
)
, final_3_days as
(
select
a.zone_code
,a.city_name
,a.order_month    
,sum(LATE_BY_10_MINS)/ count(distinct ID)as late_by_10_mins_perc_last_3_dys
from 
rolling_3_days  as a
group by 1,2,3
)
, summary as
(
select
base.* 
,late_by_5_mins_perc_last_14_dys
,p50_RESTO_CUSTOMER_MINS_last_90_dys
,avg_WAIT_AT_CUSTOMER_last_90_dys
,avg_RIDER_TO_RESTAURANT_MINS_last_90_dys
,avg_TEMPERATURE_last_90_dys
,p10_RIDER_TO_RESTAURANT_MINS_last_30_dys
,avg_FPT_last_21_dys
,avg_ERAT_last_5_dys
,late_by_10_mins_perc_last_3_dys
from
master_table_daily as base
left join
final_14_days as last_14_days
on base.zone_code =last_14_days.zone_code
and base.ORDER_MONTH =last_14_days.ORDER_MONTH
left join
final_90_days as last_90_days
on base.zone_code =last_90_days.zone_code
and base.ORDER_MONTH =last_90_days.ORDER_MONTH
left join
final_30_days as last_30_days
on base.zone_code =last_30_days.zone_code
and base.ORDER_MONTH =last_30_days.ORDER_MONTH
left join
final_21_days as last_21_days
on base.zone_code =last_21_days.zone_code
and base.ORDER_MONTH =last_21_days.ORDER_MONTH
left join
final_5_days as last_5_days
on base.zone_code =last_5_days.zone_code
and base.ORDER_MONTH =last_5_days.ORDER_MONTH
left join
final_3_days as last_3_days
on base.zone_code =last_3_days.zone_code
and base.ORDER_MONTH =last_3_days.ORDER_MONTH
  
)
select * from summary where order_month =$current_month; -- remove the data prior to this


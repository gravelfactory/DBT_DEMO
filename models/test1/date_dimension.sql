with cte as (
select
to_timestamp(started_at) as started_at,
date(to_timestamp(started_at)) as date_started_at,
hour(to_timestamp(started_at)) as hour_started_at,
case when dayname(to_timestamp(started_at)) in ('Sat','Sun')
then 'WEEKEND'
else 'BUSINESSDAY' end as Day_Type,
month(to_timestamp(started_at)) as month_started_at,
case when month(to_timestamp(started_at)) in (12,1,2) then 'WINTER'
     when month(to_timestamp(started_at)) in (3,4,5) then 'SPRINT'
     when month(to_timestamp(started_at)) in (6,7,8) then 'SUMMER'
else 'AUUMN' end as station_of_year

from {{ ref('stage_bike') }}
where started_at <>'started_at'
)

select * from cte
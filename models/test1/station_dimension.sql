with bike as (

select distinct
start_statio_id as station_id,
start_station_name as station_name,
start_lat,
start_lng
from {{ ref('stage_bike') }}
where ride_id!='ride_id'

)

select * from bike
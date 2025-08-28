with daily_weather as (
select
date(time) as date_weather,
weather,
temp,
pressure,
humidity,
clouds

from {{ source('DEMO', 'WEATHER') }}
)


, daily_weather_agg as (
select
date_weather,
weather,
round(avg(temp),2),
round(avg(pressure),2),
round(avg(humidity),2),
round(avg(clouds),2),
count(weather),
--row_number() over (partition by date_weather order by count(weather) desc) as row_number,
from daily_weather
group by date_weather, weather

qualify row_number() over (partition by date_weather order by count(weather) desc)=1

)
select * from daily_weather_agg
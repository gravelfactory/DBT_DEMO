{{ config(materialized='table') }}


select top 10 
* 
from {{ ref('my_second_dbt_model') }}
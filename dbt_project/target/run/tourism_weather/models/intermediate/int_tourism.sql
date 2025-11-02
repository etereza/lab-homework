
  create view "tereza_tourist"."core"."int_tourism__dbt_tmp"
    
    
  as (
    select * from "tereza_tourist"."core"."stg_tourism"
  );
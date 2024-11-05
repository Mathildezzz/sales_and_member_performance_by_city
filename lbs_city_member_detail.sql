delete from tutorial.mz_lbs_city_member_detail;  
insert into tutorial.mz_lbs_city_member_detail

with seg_latest as (
    select base.*,
          mapping.city_cn
    from stg.gio_user_local base
    INNER JOIN tutorial.mz_LBS_city_segment_mapping_v2 mapping  -- city 和cdp包id mapping关系
          ON base.prop_key = mapping.prop_key
    qualify rank() over(partition by base.prop_key order by update_time desc) = 1 and prop_value = '1'
)

, seg as (
    select distinct prop_key, city_cn, gio_id
    from seg_latest
    where 1=1
    qualify row_number() over(partition by prop_key, gio_id order by update_time desc) = 1
)

, members as (
    select distinct gio_id, prop_value as crm_member_id
    from stg.gio_user_local
    where 1=1
    and prop_key = 'usr_crm_member_id'
    qualify row_number() over (partition by gio_id order by update_time desc) = 1
)

select distinct prop_key, city_cn, crm_member_id
from members m
join seg s
on m.gio_id = s.gio_id;
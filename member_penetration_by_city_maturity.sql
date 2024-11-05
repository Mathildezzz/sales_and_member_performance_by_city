delete from tutorial.mz_member_penetration_by_city_maturity;  -- for the subsequent update
insert into tutorial.mz_member_penetration_by_city_maturity

------------------------------------------ member base by city
WITH member_base AS (
SELECT CASE WHEN union_table.city_maturity_type IS NULL THEN '4_unspecified' ELSE union_table.city_maturity_type END AS city_maturity_type,
       COUNT(DISTINCT union_table.omni_channel_member_id) AS ttl_member_base
FROM (
              -- 注册
            SELECT  CASE WHEN ps.city_maturity_type IS NULL THEN '4_unspecified' ELSE ps.city_maturity_type END AS city_maturity_type,
                    CAST(member_detail_id AS varchar) AS omni_channel_member_id
              FROM edw.d_member_detail mbr
              LEFT JOIN   ( SELECT DISTINCT lego_store_code, city_cn, city_en,province_cn,province_en,city_maturity.city_type AS city_maturity_type
                        FROM  edw.d_dl_phy_store store
                        LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                              ON city_maturity.city_chn = store.city_cn
                              ) ps
                    ON ps.lego_store_code = mbr.eff_reg_store
             WHERE eff_reg_channel LIKE '%LCS%'
            --   AND ps.city_maturity_type IS NOT NULL --- only look at mature, core & uprising
               
             UNION
             
              -- transaction
             SELECT 
                   CASE WHEN ps.city_maturity_type IS NULL THEN '4_unspecified' ELSE ps.city_maturity_type END AS city_maturity_type,
                   case
                    when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar))
                    else null end as omni_channel_member_id
              FROM edw.f_omni_channel_order_detail tr
         left join edw.f_crm_member_detail as mbr
                on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
         LEFT JOIN   ( SELECT DISTINCT lego_store_code, city_cn, city_en,province_cn,province_en,city_maturity.city_type AS city_maturity_type
                FROM  edw.d_dl_phy_store store
                LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                      ON city_maturity.city_chn = store.city_cn
                      ) ps
            ON tr.order_city = ps.city_cn
             where 1 = 1
               and source_channel in ('LCS', 'TMALL', 'DOUYIN', 'DOUYIN_B2B')
               and date(tr.order_paid_date) < current_date
               and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
               AND is_member_order = TRUE
               AND if_eff_order_tag = TRUE
            --   AND ps.city_maturity_type IS NOT NULL --- only look at mature, core & uprising
               
              UNION
               
              -- -- wmp注册未购的人 但有lbs信息
              
              SELECT  CASE WHEN ps.city_maturity_type IS NULL THEN '4_unspecified' ELSE ps.city_maturity_type END AS city_maturity_type,
                      CAST(crm_member_id::integer AS VARCHAR) AS omni_channel_member_id
                FROM tutorial.mz_lbs_city_member_detail lbs_city
                LEFT JOIN   ( SELECT DISTINCT lego_store_code, city_cn, city_en,province_cn,province_en,city_maturity.city_type AS city_maturity_type
                                FROM  edw.d_dl_phy_store store
                                LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                      ON city_maturity.city_chn = store.city_cn
                      ) ps
            ON lbs_city.city_cn = ps.city_cn
                WHERE CAST(crm_member_id::integer AS VARCHAR) IN 
                           (
                                SELECT  DISTINCT CAST(mbr.member_detail_id AS varchar) AS omni_channel_member_id
                              FROM edw.d_member_detail mbr
                              LEFT JOIN   ( SELECT DISTINCt CASE when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar)) else null end as omni_channel_member_id
                                              FROM edw.f_omni_channel_order_detail tr
                                          left join edw.f_crm_member_detail as mbr
                                                on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
                                              WHERE 1=1
                                              and source_channel in ('LCS', 'TMALL', 'DOUYIN', 'DOUYIN_B2B')
                                              and date(tr.order_paid_date) < current_date
                                              and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
                                              AND is_member_order = TRUE
                                              AND if_eff_order_tag = TRUE
                                              ) trans
                                    ON mbr.member_detail_id::text = trans.omni_channel_member_id
                             WHERE eff_reg_channel LIKE '%WMP%'
                              AND trans.omni_channel_member_id IS NULL
                        )-- wmp注册未购的人
               ) union_table
    GROUP BY 1
    ),
    
member_transaction AS (
                SELECT DISTINCT
                   CASE WHEN ps.city_maturity_type IS NULL THEN '4_unspecified' ELSE ps.city_maturity_type END AS city_maturity_type,
                   case
                    when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar))
                    else null end as omni_channel_member_id,
                    lego_sku_id
              FROM edw.f_omni_channel_order_detail tr
         left join edw.f_crm_member_detail as mbr
                on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
        LEFT JOIN (
                    SELECT DISTINCT  store.city_cn, city_maturity.city_type AS city_maturity_type
                    FROM  edw.d_dl_phy_store store
                    LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                          ON city_maturity.city_chn = store.city_cn
                  ) as ps 
              ON tr.order_city = ps.city_cn
             where 1 = 1
               and source_channel in ('LCS', 'TMALL', 'DOUYIN', 'DOUYIN_B2B')
               and date(tr.order_paid_date) < current_date
               and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
               AND is_member_order = TRUE
               AND if_eff_order_tag = TRUE
            --   AND ps.city_maturity_type IS NOT NULL --- only look at mature, core & uprising
),

kids_vs_adult_member_shopper AS (             
SELECT city_maturity_type,
       COUNT(DISTINCT CASE WHEN lego_sku_id::text NOT IN (SELECT DISTINCT lego_sku_id::text FROM tutorial.portfolio_base_table_LCS WHERE product_kids_vs_adult_sku = 'adult product') THEN omni_channel_member_id ELSE NULL END)           AS ttl_kids_member_shopper,
       COUNT(DISTINCT CASE WHEN lego_sku_id::text IN (SELECT DISTINCT lego_sku_id::text FROM tutorial.portfolio_base_table_LCS WHERE product_kids_vs_adult_sku = 'adult product' ) THEN omni_channel_member_id ELSE NULL END)               AS ttl_adult_member_shopper
FROM member_transaction
    GROUP BY 1
),



member_base_by_city AS (
SELECT member_base.city_maturity_type,
       member_base.ttl_member_base,
       ttl_kids_member_shopper,
       ttl_adult_member_shopper
     FROM member_base
LEFT JOIN kids_vs_adult_member_shopper
       ON member_base.city_maturity_type = kids_vs_adult_member_shopper.city_maturity_type
),

population_base AS (
SELECT CASE WHEN city_maturity_level = 'Mature' THEN '1_mature'
            WHEN city_maturity_level = 'Core' THEN '2_core'
            WHEN city_maturity_level = 'Uprising' THEN '3_uprising'
        ELSE '4_unspecified' END AS city_maturity_level,
       SUM(target_kids_population)   AS target_kids_population,
       SUM(adult_shopper_population) AS adult_shopper_population
FROM tutorial.mz_population_by_city_base_table
GROUP BY 1
)


SELECT member_base_by_city.city_maturity_type,
       population_base.target_kids_population,
       population_base.adult_shopper_population,
       ---------- member penetration
       member_base_by_city.ttl_member_base,
       CAST(member_base_by_city.ttl_member_base AS FLOAT)/population_base.target_kids_population     AS member_base_penetration_target_kids_shopper,
       CAST(member_base_by_city.ttl_member_base AS FLOAT)/population_base.adult_shopper_population   AS member_base_penetration_Y18_45_without_0_14_kids,
       member_base_by_city.ttl_kids_member_shopper,
       member_base_by_city.ttl_adult_member_shopper,
       CAST(member_base_by_city.ttl_kids_member_shopper AS FLOAT)/population_base.target_kids_population  AS Kids_member_shopper_penetration,
       CAST(member_base_by_city.ttl_adult_member_shopper AS FLOAT)/population_base.adult_shopper_population AS adult_member_shopper_penetration,
       to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date,
       getdate()                                                   AS dl_load_time
  FROM member_base_by_city 
  LEFT JOIN population_base
         ON population_base.city_maturity_level = member_base_by_city.city_maturity_type
UNION ALL
SELECT 
      'all_have_city_information' AS city_maturity_type,
      NULL  AS target_kids_population,
      NULL  AS adult_shopper_population,
      ---------- member penetration
      SUM(member_base_by_city.ttl_member_base) AS ttl_member_base,
      NULL AS member_base_penetration_target_kids_shopper,
      NULL AS member_base_penetration_Y18_45_without_0_14_kids,
      SUM(member_base_by_city.ttl_kids_member_shopper) AS ttl_kids_member_shopper,
      SUM(member_base_by_city.ttl_adult_member_shopper) AS ttl_adult_member_shopper,
      NULL AS Kids_member_shopper_penetration,
      NULL AS adult_member_shopper_penetration,
      to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date,
      getdate()                                                   AS dl_load_time
FROM member_base_by_city

UNION ALL
SELECT  'TTL_member_base' AS city_maturity_type,
      NULL  AS target_kids_population,
      NULL  AS adult_shopper_population,
      SUM(member_count) AS ttl_member_base,
      NULL AS member_base_penetration_target_kids_shopper,
      NULL AS member_base_penetration_Y18_45_without_0_14_kids,
      NULL AS ttl_kids_member_shopper,
      NULL AS ttl_adult_member_shopper,
      NULL AS Kids_member_shopper_penetration,
      NULL AS adult_member_shopper_penetration,
      to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date,
      getdate()                                                   AS dl_load_time
FROM (
SELECT COUNT(member_detail_id) AS member_count
FROM edw.d_member_detail
UNION ALL
      SELECT COUNT(DISTINCT CASE WHEN member_detail_id IS NULL THEN platform_id_value ELSE NULL END) AS member_count
        FROM edw.d_belong_channel_inc_ec_his_mbr 
      WHERE belong_type = 'registerOrBind'
        AND eff_belong_channel = 'TMall'
UNION ALL
      SELECT 
             COUNT(DISTINCT CASE WHEN member_detail_id IS NULL THEN platform_id_value ELSE NULL END) AS member_count
        FROM edw.d_belong_channel_inc_ec_his_mbr 
      WHERE belong_type = 'registerOrBind'
        AND eff_belong_channel = 'Douyin'
        )
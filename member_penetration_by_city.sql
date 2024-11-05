delete from tutorial.mz_member_penetration_by_city;  -- for the subsequent update
insert into tutorial.mz_member_penetration_by_city

WITH member_base AS (
SELECT union_table.city_cn,
       COUNT(DISTINCT union_table.omni_channel_member_id) AS ttl_member_base
FROM (
              -- 注册
            SELECT  mbr.city_cn,
                    CAST(member_detail_id AS varchar) AS omni_channel_member_id
              FROM edw.d_member_detail mbr
              LEFT JOIN   ( SELECT DISTINCT lego_store_code, city_cn, city_en,province_cn,province_en,city_maturity.city_type AS city_maturity_type
                        FROM  edw.d_dl_phy_store store
                        LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                              ON city_maturity.city_chn = store.city_cn
                              ) ps
                    ON ps.lego_store_code = mbr.eff_reg_store
             WHERE eff_reg_channel LIKE '%LCS%'
               AND ps.city_maturity_type IS NOT NULL --- only look at mature, core & uprising
               
             UNION
             
              -- transaction
             SELECT 
                   order_city AS city_cn,
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
               AND ps.city_maturity_type IS NOT NULL --- only look at mature, core & uprising
               
              UNION
               
              -- -- wmp注册未购的人 但有lbs信息
              
              SELECT  lbs_city.city_cn,
                      CAST(crm_member_id::integer AS VARCHAR) AS omni_channel_member_id
                FROM tutorial.mz_lbs_city_member_detail lbs_city
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
    
    
 lego_calendar_fact as
    (
        select
        date_id,
        lego_year,
        lego_month,
        lego_week,
        lego_quarter,
        d_day_of_lego_week
        from edw.d_dl_calendar
        where 1 = 1
        and date_type = 'day'
        and date_id < current_date
    ),
    
member_transaction AS (
                SELECT DISTINCT
                   order_city AS city_cn,
                   case
                    when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar))
                    else null end as omni_channel_member_id,
                    lego_sku_id,
                    cm1.lego_year as trans_lego_year,
                    cm2.lego_year as reg_lego_year    
              FROM edw.f_omni_channel_order_detail tr
         left join edw.f_crm_member_detail as mbr
                on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
          left join lego_calendar_fact as cm1 -- cm1 for mapping of transaction date
                 on date(tr.order_paid_time) = date(cm1.date_id)
        left join lego_calendar_fact as cm2 -- cm1 for mapping of registration date
               on coalesce(date(mbr.join_time), date(tr.first_bind_time)) = date(cm2.date_id) -- 优先取CRM注册时间，缺失情况下取渠道内绑定时间
        LEFT JOIN (
                    SELECT DISTINCT lego_store_code, store.city_cn, city_en,province_cn,province_en,city_maturity.city_type AS city_maturity_type
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
               AND ps.city_maturity_type IS NOT NULL --- only look at mature, core & uprising
),


kids_vs_adult_member_shopper AS (             
SELECT city_cn,
       CAST(COUNT(DISTINCT CASE WHEN (trans_lego_year = extract('year' FROM current_date)) AND (trans_lego_year = reg_lego_year) then omni_channel_member_id else null end) AS FLOAT)/  COUNT(DISTINCT CASE WHEN (trans_lego_year = extract('year' FROM current_date)) then omni_channel_member_id else null end)     AS new_member_shopper_share_YTD,
       COUNT(DISTINCT CASE WHEN lego_sku_id::text NOT IN (SELECT DISTINCT lego_sku_id
                                                                 FROM edw.d_dl_product_info_latest
                                                                WHERE 
                                                                  -- Filter out rows that don't start with a number or a fraction like '1 1/2'
                                                                  TRIM(age_mark) ~ '^[0-9]+([ ]*[0-9]*/[0-9]+)?'
                                                                  
                                                                  -- Handle '1 1/2' by converting it to a decimal value
                                                                  AND COALESCE(
                                                                      -- Convert fraction '1 1/2' to a decimal value like '1.5'
                                                                      CASE 
                                                                          WHEN TRIM(age_mark) ~ '^[0-9]+ [0-9]+/[0-9]+' THEN
                                                                              CAST(SPLIT_PART(TRIM(age_mark), ' ', 1) AS NUMERIC) + 
                                                                              CAST(SPLIT_PART(SPLIT_PART(TRIM(age_mark), ' ', 2), '/', 1) AS NUMERIC) / 
                                                                              CAST(SPLIT_PART(SPLIT_PART(TRIM(age_mark), ' ', 2), '/', 2) AS NUMERIC)
                                                                          -- Remove non-numeric characters after leading digits (like '+', '-' etc.)
                                                                          ELSE CAST(REGEXP_REPLACE(TRIM(age_mark), '[^0-9]+.*', '') AS NUMERIC)
                                                                      END, 
                                                                      0) >= 13
                                                            ) THEN omni_channel_member_id ELSE NULL END)                                AS ttl_kids_member_shopper,
       COUNT(DISTINCT CASE WHEN lego_sku_id::text IN (SELECT DISTINCT lego_sku_id
                                                                 FROM edw.d_dl_product_info_latest
                                                                WHERE 
                                                                  -- Filter out rows that don't start with a number or a fraction like '1 1/2'
                                                                  TRIM(age_mark) ~ '^[0-9]+([ ]*[0-9]*/[0-9]+)?'
                                                                  
                                                                  -- Handle '1 1/2' by converting it to a decimal value
                                                                  AND COALESCE(
                                                                      -- Convert fraction '1 1/2' to a decimal value like '1.5'
                                                                      CASE 
                                                                          WHEN TRIM(age_mark) ~ '^[0-9]+ [0-9]+/[0-9]+' THEN
                                                                              CAST(SPLIT_PART(TRIM(age_mark), ' ', 1) AS NUMERIC) + 
                                                                              CAST(SPLIT_PART(SPLIT_PART(TRIM(age_mark), ' ', 2), '/', 1) AS NUMERIC) / 
                                                                              CAST(SPLIT_PART(SPLIT_PART(TRIM(age_mark), ' ', 2), '/', 2) AS NUMERIC)
                                                                          -- Remove non-numeric characters after leading digits (like '+', '-' etc.)
                                                                          ELSE CAST(REGEXP_REPLACE(TRIM(age_mark), '[^0-9]+.*', '') AS NUMERIC)
                                                                      END, 
                                                                      0) >= 13) THEN omni_channel_member_id ELSE NULL END)               AS ttl_adult_member_shopper
FROM member_transaction
    GROUP BY 1
),


current_open_store AS (
      SELECT city_cn, COUNT(DISTINCT lego_store_code) AS current_LCS_store_count
                        FROM  edw.d_dl_phy_store store
                        WHERE store_status = '营业中' AND distributor LIKE '%LCS%'
                        GROUP BY 1
),

traffic AS (
     SELECT city_cn,
            SUM(CASE WHEN extract('year' FROM CAST(traffic_table.date_id AS date)) = extract('year' from current_date) THEN traffic_amt ELSE 0 END)               AS traffic_TY_YTD,
            SUM(CASE WHEN extract('year' FROM CAST(traffic_table.date_id AS date)) = extract('year' from current_date) - 1 
                      AND CAST(traffic_table.date_id AS date) < current_date - 365 THEN traffic_amt ELSE 0 END)                                                         AS traffic_LY_YTD
  FROM dm.agg_final_sales_by_store_daily traffic_table
  WHERE agg_type = 'LEGO'
    AND distributor LIKE '%LCS%'                              -- distributor_name
    GROUP BY 1
        ),
        
new_reg_in_LCS AS (
          SELECT  ps.city_cn,
                  COUNT(DISTINCT member_detail_id) AS LCS_new_reg
              FROM edw.d_member_detail mbr
               LEFT JOIN   ( SELECT DISTINCT lego_store_code, city_cn, city_en,province_cn,province_en,city_maturity.city_type AS city_maturity_type
                        FROM  edw.d_dl_phy_store store
                        LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                              ON city_maturity.city_chn = store.city_cn
                              ) ps
                    ON ps.lego_store_code = mbr.eff_reg_store
             WHERE eff_reg_channel LIKE '%LCS%'
               AND extract('year' from DATE(join_time)) = extract('year' from current_date)  -- 看今年YTD注册的人
             GROUP BY 1
),



member_base_by_city AS (
SELECT member_base.city_cn,
       member_base.ttl_member_base,
       ttl_kids_member_shopper,
       ttl_adult_member_shopper,


       current_open_store.current_LCS_store_count,
       CAST(traffic.traffic_TY_YTD AS FLOAT)/traffic.traffic_LY_YTD - 1             AS LCS_YTD_traffic_growth,
       CAST(new_reg_in_LCS.LCS_new_reg AS FLOAT)/traffic.traffic_TY_YTD             AS LCS_YTD_regisration_rate,
       kids_vs_adult_member_shopper.new_member_shopper_share_YTD
     FROM member_base
LEFT JOIN kids_vs_adult_member_shopper
       ON member_base.city_cn = kids_vs_adult_member_shopper.city_cn
LEFT JOIN current_open_store
       ON member_base.city_cn = current_open_store.city_cn
LEFT JOIN traffic
       ON member_base.city_cn = traffic.city_cn
LEFT JOIN new_reg_in_LCS
       ON member_base.city_cn = new_reg_in_LCS.city_cn
)


SELECT CASE WHEN population_base.city_maturity_level = 'Mature' THEN '1_mature'
            WHEN population_base.city_maturity_level = 'Core' THEN '2_core'
            WHEN population_base.city_maturity_level = 'Uprising' THEN '3_uprising'
        ELSE 'Others' END AS city_maturity_level,
       population_base.city_cn,
       population_base.target_kids_population,
       population_base.adult_shopper_population,
       ---------- member penetration
       member_base_by_city.ttl_member_base,
       CAST(member_base_by_city.ttl_member_base AS FLOAT)/population_base.target_kids_population     AS member_base_penetration_target_kids_shopper,
       CAST(member_base_by_city.ttl_member_base AS FLOAT)/population_base.adult_shopper_population   AS member_base_penetration_Y18_45_without_0_14_kids,
       member_base_by_city.ttl_kids_member_shopper,
       member_base_by_city.ttl_adult_member_shopper,
       CAST(member_base_by_city.ttl_kids_member_shopper AS FLOAT)/population_base.target_kids_population  AS Kids_member_shopper_penetration,
       CAST(member_base_by_city.ttl_adult_member_shopper AS FLOAT)/population_base.adult_shopper_population AS adult_member_shopper_penetration
  FROM tutorial.mz_population_by_city_base_table population_base
  LEFT JOIN member_base_by_city
         ON population_base.city_cn = member_base_by_city.city_cn
  
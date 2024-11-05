delete from tutorial.mz_member_performance_by_city_tmall;  -- for the subsequent update
insert into tutorial.mz_member_performance_by_city_tmall

------------------------------------- TTL sales ----------------
with lego_calendar_fact as
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

omni_trans_fact as
    ( 
        select
        order_city,
        date(tr.order_paid_date) as order_paid_date,
        kyid,
        case
        when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar))
        else null end as omni_channel_member_id, -- 优先取member_detail_id，缺失情况下再取渠道内部id
        tr.parent_order_id,
        tr.sales_qty, -- 用于为LCS判断正负单
        tr.if_eff_order_tag, -- 该字段仅对LCS有true / false之分，对于其余渠道均为true
        tr.is_member_order,
        tr.order_rrp_amt,
        cm1.lego_year as trans_lego_year,
        cm1.lego_quarter as trans_lego_quarter,
        cm1.lego_month as trans_lego_quarter,
        cm2.lego_year as reg_lego_year,
        cm2.lego_quarter as reg_lego_quarter,
        cm2.lego_month as reg_lego_quarter     
        from edw.f_omni_channel_order_detail as tr
        left join edw.f_crm_member_detail as mbr
        on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
        left join lego_calendar_fact as cm1 -- cm1 for mapping of transaction date
        on date(tr.order_paid_date) = date(cm1.date_id)
        left join lego_calendar_fact as cm2 -- cm1 for mapping of registration date
        on coalesce(date(mbr.join_time), date(tr.first_bind_time)) = date(cm2.date_id) -- 优先取CRM注册时间，缺失情况下取渠道内绑定时间
        where 1 = 1
        and source_channel in ('TMALL')
        and date(tr.order_paid_date) < current_date
        and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
    ),

  
  ttl_sales_TY AS (
    SELECT     
         CASE WHEN trans.order_city IS NULL THEN 'no order city' 
          WHEN trans.order_city IS NOT NULL AND ps.city_maturity_type IS NULL THEN 'unspecified'
          ELSE ps.city_maturity_type  END AS city_maturity_type,
          trans.order_city,
          sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) as sales_rrp,
          count(distinct case when if_eff_order_tag = true then parent_order_id else null end) as transactions
    from omni_trans_fact trans
     LEFT JOIN   ( SELECT DISTINCT city_cn,city_maturity.city_type AS city_maturity_type
                FROM  edw.d_dl_phy_store store
                LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                      ON city_maturity.city_chn = store.city_cn
              ) ps
    ON trans.order_city = ps.city_cn
    where 1 = 1
    and extract('year' FROM DATE(order_paid_date)) = extract('year' FROM current_date)  
    GROUP BY 1,2
    ),
    
  
ttl_sales_LY AS (
   SELECT    CASE WHEN trans.order_city IS NULL THEN 'no order city' 
              WHEN trans.order_city IS NOT NULL AND ps.city_maturity_type IS NULL THEN 'unspecified'
              ELSE ps.city_maturity_type  END AS city_maturity_type,
           trans.order_city,
           sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) as sales_rrp,
           count(distinct case when if_eff_order_tag = true then parent_order_id else null end) as transactions
    from omni_trans_fact trans
LEFT JOIN   ( SELECT DISTINCT city_cn,city_maturity.city_type AS city_maturity_type
                FROM  edw.d_dl_phy_store store
                LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                      ON city_maturity.city_chn = store.city_cn
              ) ps
    ON trans.order_city = ps.city_cn
WHERE 1 = 1 
   AND extract('year' FROM DATE(order_paid_date)) = extract('year' FROM current_date) - 1 -- 年份去年
   AND DATE(order_paid_date) < (current_date - interval '1 year')::date -- 小于去年同一天
   GROUP BY 1,2
  ),
  
sales AS (
SELECT ttl_sales_TY.city_maturity_type,
       ttl_sales_TY.order_city,

       ttl_sales_TY.sales_rrp,
       ttl_sales_TY.transactions,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/ttl_sales_TY.transactions                AS atv,

       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/ttl_sales_LY.sales_rrp - 1               AS sales_vs_LY,
       CAST(ttl_sales_TY.transactions AS FLOAT)/ttl_sales_LY.transactions - 1         AS transactions_vs_LY,
       (CAST(ttl_sales_TY.sales_rrp AS FLOAT)/ttl_sales_TY.transactions)/(CAST(ttl_sales_LY.sales_rrp AS FLOAT)/ttl_sales_LY.transactions) -1 AS atv_vs_LY
FROM ttl_sales_TY
LEFT JOIN ttl_sales_LY
       ON ttl_sales_TY.order_city = ttl_sales_LY.order_city
),
  
---------------------------------------------------------------------

new_member_ty AS (
    select DISTINCT platform_id_value AS kyid
    from edw.d_ec_b2c_member_shopper_detail_latest
    where 1 = 1
    and platform_id_type = 'kyid' -- platform_id_type: opendi / kyid
    and platformid = 'taobao' -- platformid: douyin / taobao
    and extract('year' FROM DATE(first_bind_time)) = extract('year' from current_date)
  ),
  
 member_KPI_TY AS (
   SELECT  CASE WHEN trans.order_city IS NULL THEN 'no order city' 
              WHEN trans.order_city IS NOT NULL AND ps.city_maturity_type IS NULL THEN 'unspecified'
              ELSE ps.city_maturity_type  END AS city_maturity_type,
          trans.order_city,
            
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then parent_order_id else null end),0) AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.kyid else null end)) ,0)                                                                     AS member_frequency,
          
          ----------- new ------------
          CAST((sum(case when new_member_ty.kyid IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.kyid IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as new_mbr_sales_share,
          
          CAST((count(distinct case when new_member_ty.kyid IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.kyid else null end)) AS FLOAT)/NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.kyid else null end)),0)                                                                                                       AS new_member_shopper_share,
          
          CAST((sum(case when new_member_ty.kyid IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.kyid IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.kyid IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then parent_order_id else null end),0) AS new_member_atv,
          CAST((count(distinct case when new_member_ty.kyid IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when new_member_ty.kyid IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.kyid else null end)),0)                                                                                                                                     AS new_member_frequency,
           
          ----------- existing ------------
          CAST((sum(case when new_member_ty.kyid IS NULL AND is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.kyid IS NULL AND is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as existing_member_sales_share,
          
          CAST((count(distinct case when new_member_ty.kyid IS NULL AND is_member_order = TRUE AND if_eff_order_tag = true then trans.kyid else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND is_member_order = TRUE AND if_eff_order_tag = true then trans.kyid else null end)) ,0)                                                                                 AS existing_member_shopper_share,
          
          CAST((sum(case when new_member_ty.kyid IS NULL AND is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.kyid IS NULL AND is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.kyid IS NULL AND is_member_order = TRUE AND if_eff_order_tag = true then parent_order_id else null end),0) AS existing_member_atv,
          CAST((count(distinct case when new_member_ty.kyid IS NULL AND is_member_order = TRUE AND if_eff_order_tag = true then parent_order_id else null end)) AS FLOAT) /NULLIF( (count(distinct case when new_member_ty.kyid IS NULL AND is_member_order = TRUE AND if_eff_order_tag = true then trans.kyid else null end)),0)                                                                                   AS existing_member_frequency
    from omni_trans_fact trans
     LEFT JOIN   ( SELECT DISTINCT city_cn,city_maturity.city_type AS city_maturity_type
                FROM  edw.d_dl_phy_store store
                LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                      ON city_maturity.city_chn = store.city_cn
              ) ps
            ON trans.order_city = ps.city_cn
     LEFT JOIN new_member_ty
            ON trans.kyid = new_member_ty.kyid
    
    where 1 = 1
    and extract('year' FROM DATE(order_paid_date)) = extract('year' FROM current_date)  
    GROUP BY 1,2
    ),

 



------------------------------------------------------------------------------

      
new_member_LY_YTD AS (
    select DISTINCT platform_id_value AS kyid
        from edw.d_ec_b2c_member_shopper_detail_latest
        where 1 = 1
       and platform_id_type = 'kyid' -- platform_id_type: opendi / kyid
       and platformid = 'taobao' -- platformid: douyin / taobao
       AND extract('year' FROM DATE(first_bind_time)) = extract('year' FROM current_date) - 1 -- 年份去年
       AND DATE(first_bind_time) < (current_date - interval '1 year')::date -- 小于去年同一天
  ),
 
  
  member_KPI_LY_YTD AS (
 SELECT    CASE WHEN trans.order_city IS NULL THEN 'no order city' 
              WHEN trans.order_city IS NOT NULL AND ps.city_maturity_type IS NULL THEN 'unspecified'
              ELSE ps.city_maturity_type  END AS city_maturity_type,
          trans.order_city,
          
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then parent_order_id else null end),0) AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.kyid else null end)) ,0)                                                                     AS member_frequency,
         
          ----------- new ------------
          CAST((sum(case when new_member_LY_YTD.kyid IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_LY_YTD.kyid IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as new_mbr_sales_share,
          
          CAST((count(distinct case when new_member_LY_YTD.kyid IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.kyid else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.kyid else null end))  ,0)                                                                                                     AS new_member_shopper_share,
          
          CAST((sum(case when new_member_LY_YTD.kyid IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_LY_YTD.kyid IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_LY_YTD.kyid IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then parent_order_id else null end) ,0) AS new_member_atv,
          CAST((count(distinct case when new_member_LY_YTD.kyid IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when new_member_LY_YTD.kyid IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.kyid else null end))  ,0)                                                                                                                                   AS new_member_frequency,
        
          ----------- existing ------------
          CAST((sum(case when new_member_LY_YTD.kyid IS NULL AND is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_LY_YTD.kyid IS NULL AND is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as existing_member_sales_share,
          
          CAST((count(distinct case when new_member_LY_YTD.kyid IS NULL AND is_member_order = TRUE AND if_eff_order_tag = true then trans.kyid else null end)) AS FLOAT)/NULLIF( (count(distinct case when is_member_order = TRUE AND is_member_order = TRUE AND if_eff_order_tag = true then trans.kyid else null end))  ,0)                                                                                                     AS existing_member_shopper_share,
          
          CAST((sum(case when new_member_LY_YTD.kyid IS NULL AND is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_LY_YTD.kyid IS NULL AND is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_LY_YTD.kyid IS NULL AND is_member_order = TRUE AND if_eff_order_tag = true then parent_order_id else null end),0) AS existing_member_atv,
          CAST((count(distinct case when new_member_LY_YTD.kyid IS NULL AND is_member_order = TRUE AND if_eff_order_tag = true then parent_order_id else null end)) AS FLOAT) /NULLIF( (count(distinct case when new_member_LY_YTD.kyid IS NULL AND is_member_order = TRUE AND if_eff_order_tag = true then trans.kyid else null end)) ,0)                                                                                  AS existing_member_frequency
 
   from omni_trans_fact trans
     LEFT JOIN   ( SELECT DISTINCT city_cn,city_maturity.city_type AS city_maturity_type
                FROM  edw.d_dl_phy_store store
                LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                      ON city_maturity.city_chn = store.city_cn
              ) ps
            ON trans.order_city = ps.city_cn
     LEFT JOIN new_member_LY_YTD
            ON trans.kyid = new_member_LY_YTD.kyid
    where 1 = 1
       AND extract('year' FROM DATE(order_paid_date)) = extract('year' FROM current_date) - 1 -- 年份去年
       AND DATE(order_paid_date) < (current_date - interval '1 year')::date -- 小于去年同一天
       GROUP BY 1,2
    ),
    
    
    member_KPI AS (
    SELECT member_KPI_TY.city_maturity_type,
           member_KPI_TY.order_city,
           
           member_KPI_TY.member_atv,
           member_KPI_TY.member_atv/member_KPI_LY_YTD.member_atv - 1                              AS member_atv_vs_LY,
           member_KPI_TY.member_frequency,
           member_KPI_TY.member_frequency/member_KPI_LY_YTD.member_frequency - 1                  AS member_frequency_vs_LY,
           
           member_KPI_TY.new_mbr_sales_share,
           member_KPI_TY.new_mbr_sales_share - member_KPI_LY_YTD.new_mbr_sales_share                         AS new_mbr_sales_share_vs_LY,
           member_KPI_TY.new_member_shopper_share,
           member_KPI_TY.new_member_shopper_share - member_KPI_LY_YTD.new_member_shopper_share               AS new_member_shopper_share_vs_LY,
           member_KPI_TY.new_member_atv,
           member_KPI_TY.new_member_atv/NULLIF(member_KPI_LY_YTD.new_member_atv,0) - 1                       AS new_member_atv_vs_LY,
           member_KPI_TY.new_member_frequency,
           member_KPI_TY.new_member_frequency/NULLIF(member_KPI_LY_YTD.new_member_frequency,0) - 1             AS new_member_frequency_vs_LY,
           member_KPI_TY.existing_member_atv,
           member_KPI_TY.existing_member_atv/NULLIF(member_KPI_LY_YTD.existing_member_atv,0) -1                AS existing_member_atv_vs_LY,
           member_KPI_TY.existing_member_frequency,
           member_KPI_TY.existing_member_frequency/NULLIF(member_KPI_LY_YTD.existing_member_frequency,0) -1    AS existing_member_frequency_vs_LY
      FROM member_KPI_TY
      LEFT JOIN member_KPI_LY_YTD
             ON member_KPI_TY.order_city = member_KPI_LY_YTD.order_city
    )

             

SELECT 
        ----------------sales TY vs.LY
       sales.city_maturity_type,
       sales.order_city,
     
       sales.sales_rrp,
       sales.transactions,
       sales.atv,

       CAST(sales.sales_vs_LY AS FLOAT),
       CAST(sales.transactions_vs_LY AS FLOAT),
       CAST(sales.atv_vs_LY AS FLOAT),
       
       ------------ member KPI TY vs LY\
       member_KPI.member_atv,
       member_KPI.member_atv_vs_LY,
       member_KPI.member_frequency,
       member_KPI.member_frequency_vs_LY,
       
       
       member_KPI.new_mbr_sales_share,
       member_KPI.new_mbr_sales_share_vs_LY,
       member_KPI.new_member_shopper_share,
       member_KPI.new_member_shopper_share_vs_LY,
       member_KPI.new_member_atv,
       member_KPI.new_member_atv_vs_LY,
       member_KPI.new_member_frequency,
       member_KPI.new_member_frequency_vs_LY,
       member_KPI.existing_member_atv,
       member_KPI.existing_member_atv_vs_LY,
       member_KPI.existing_member_frequency,
       member_KPI.existing_member_frequency_vs_LY
  FROM sales
  LEFT JOIN member_KPI
         ON member_KPI.order_city = sales.order_city;
 
 
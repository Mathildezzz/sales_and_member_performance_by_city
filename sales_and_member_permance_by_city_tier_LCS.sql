delete from tutorial.mz_member_performance_by_city_tier_LCS;  -- for the subsequent update
insert into tutorial.mz_member_performance_by_city_tier_LCS



------------------------------------- TTL sales ----------------
WITH ttl_sales_TY AS (
SELECT CASE WHEN city_tier_list.city_tier IS NULL THEN 'Others' ELSE  city_tier_list.city_tier END AS city_tier,
       SUM(transactions) AS transactions, SUM(traffic_amt) AS traffic, SUM(gmv_rsp) - SUM(return_gmv_rsp) AS sales_rrp
FROM dm_view.offline_lcs_cs__fnl sales
LEFT JOIN edw.d_dl_city_tier city_tier_list
      ON sales.city_cn= city_tier_list.city_chn
WHERE 1 = 1 
  AND extract('year' FROM DATE(date_id)) = extract('year' from current_date)
  GROUP BY 1
 UNION ALL
 SELECT 'TTL' AS city_tier,
       SUM(transactions) AS transactions, SUM(traffic_amt) AS traffic, SUM(gmv_rsp) - SUM(return_gmv_rsp) AS sales_rrp
FROM dm_view.offline_lcs_cs__fnl sales
WHERE 1 = 1 
  AND extract('year' FROM DATE(date_id)) = extract('year' from current_date)
  GROUP BY 1
  ),
  
ttl_sales_LY AS (
SELECT CASE WHEN city_tier_list.city_tier IS NULL THEN 'Others' ELSE  city_tier_list.city_tier END AS city_tier,
       SUM(transactions) AS transactions, SUM(traffic_amt) AS traffic, SUM(gmv_rsp) - SUM(return_gmv_rsp) AS sales_rrp
FROM dm_view.offline_lcs_cs__fnl sales
LEFT JOIN edw.d_dl_city_tier city_tier_list
      ON sales.city_cn= city_tier_list.city_chn
WHERE 1 = 1 
   AND extract('year' FROM DATE(date_id)) = extract('year' FROM current_date) - 1 -- 年份去年
   AND DATE(date_id) < (current_date - interval '1 year')::date -- 小于去年同一天
   GROUP BY 1
   UNION ALL
 SELECT 'TTL' AS city_tier,
       SUM(transactions) AS transactions, SUM(traffic_amt) AS traffic, SUM(gmv_rsp) - SUM(return_gmv_rsp) AS sales_rrp
FROM dm_view.offline_lcs_cs__fnl sales
WHERE 1 = 1 
   AND extract('year' FROM DATE(date_id)) = extract('year' FROM current_date) - 1 -- 年份去年
   AND DATE(date_id) < (current_date - interval '1 year')::date -- 小于去年同一天
   GROUP BY 1
  ),
  
  
sales AS (
SELECT ttl_sales_TY.city_tier,
       ttl_sales_TY.traffic,
       ttl_sales_TY.sales_rrp,
       ttl_sales_TY.transactions,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_TY.transactions,0)                AS atv,
       CAST(ttl_sales_TY.traffic AS FLOAT)/NULLIF(ttl_sales_LY.traffic,0) - 1                   AS traffic_vs_LY,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_LY.sales_rrp,0) - 1               AS sales_vs_LY,
       CAST(ttl_sales_TY.transactions AS FLOAT)/NULLIF(ttl_sales_LY.transactions,0) - 1         AS transactions_vs_LY,
       (CAST(ttl_sales_TY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_TY.transactions,0))/(CAST(ttl_sales_LY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_LY.transactions,0)) -1 AS atv_vs_LY
FROM ttl_sales_TY
LEFT JOIN ttl_sales_LY
       ON ttl_sales_TY.city_tier = ttl_sales_LY.city_tier
),
  
---------------------------------------------------------------------

new_member_TY_YTD AS (
   SELECT
           member_detail_id
     FROM edw.d_member_detail
     WHERE  1= 1
       AND extract('year' FROM DATE(join_time)) = extract('year' from current_date)
       AND eff_reg_channel LIKE '%LCS%'
  ),
 
  
  member_KPI_TY AS (
  
  SELECT CASE WHEN city_tier_list.city_tier IS NULL THEN 'Others' ELSE  city_tier_list.city_tier END AS city_tier,
  
         CAST((sum(case when crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END) AS member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN trans.crm_member_id ELSE NULL END) AS member_frequency,
         ---------- new
         CAST((sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT) /    (sum(case when crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end))     AS new_mbr_sales_share,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN crm_member_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE THEN crm_member_id ELSE NULL END)     AS new_member_shopper_share,

 
         CAST((sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN original_order_id ELSE NULL END) AS new_member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN trans.crm_member_id ELSE NULL END) AS new_member_frequency,
              
         ----------- existing
         CAST((sum(case when new_member.member_detail_id IS NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT) / COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS  NULL THEN original_order_id ELSE NULL END) AS existing_member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NULL THEN trans.crm_member_id ELSE NULL END) AS existing_member_frequency
              
     FROM edw.f_member_order_detail trans
     LEFT JOIN new_member_TY_YTD new_member
            ON trans.crm_member_id::integer = new_member.member_detail_id::integer
     LEFT JOIN edw.d_dl_city_tier city_tier_list
      ON trans.city_cn = city_tier_list.city_chn
     WHERE is_rrp_sales_type = 1
       AND distributor_name <> 'LBR'
       AND extract('year' FROM date_id) = extract('year' from current_date)
       AND crm_member_id IS NOT NULL   -- member sales
       GROUP BY 1
    UNION ALL
    SELECT 'TTL' city_tier,
            
         CAST((sum(case when crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END) AS member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN trans.crm_member_id ELSE NULL END) AS member_frequency,
        -------- new
         CAST((sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT) /    (sum(case when crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end))     AS new_mbr_sales_share,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN crm_member_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE THEN crm_member_id ELSE NULL END)     AS new_member_shopper_share,

 
         CAST((sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN original_order_id ELSE NULL END) AS new_member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN trans.crm_member_id ELSE NULL END) AS new_member_frequency,
              
         -------- existing
         CAST((sum(case when new_member.member_detail_id IS NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT) / COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS  NULL THEN original_order_id ELSE NULL END) AS existing_member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NULL THEN trans.crm_member_id ELSE NULL END) AS existing_member_frequency
              
     FROM edw.f_member_order_detail trans
     LEFT JOIN new_member_TY_YTD new_member
            ON trans.crm_member_id::integer = new_member.member_detail_id::integer
     WHERE is_rrp_sales_type = 1
       AND distributor_name <> 'LBR'
       AND extract('year' FROM date_id) = extract('year' from current_date)
       AND crm_member_id IS NOT NULL   -- member sales
       GROUP BY 1
    ),
      
      
new_member_LY_YTD AS (
   SELECT member_detail_id
     FROM edw.d_member_detail
     WHERE  1= 1
       AND  eff_reg_channel LIKE '%LCS%'
       AND extract('year' FROM DATE(join_time)) = extract('year' FROM current_date) - 1 -- 年份去年
       AND DATE(join_time) < (current_date - interval '1 year')::date -- 小于去年同一天
  ),
 
  
  member_KPI_LY_YTD AS (
  
  SELECT CASE WHEN city_tier_list.city_tier IS NULL THEN 'Others' ELSE  city_tier_list.city_tier END AS city_tier,
           
         CAST((sum(case when crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END) AS member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN trans.crm_member_id ELSE NULL END) AS member_frequency,
         
         ------------- new
          CAST((sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT) /    (sum(case when crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end))     AS new_mbr_sales_share,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN crm_member_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE THEN crm_member_id ELSE NULL END)     AS new_member_shopper_share,

         CAST((sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN original_order_id ELSE NULL END) AS new_member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN trans.crm_member_id ELSE NULL END) AS new_member_frequency,
              
         ----------- existing
         CAST((sum(case when new_member.member_detail_id IS NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT) / COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS  NULL THEN original_order_id ELSE NULL END) AS existing_member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NULL THEN trans.crm_member_id ELSE NULL END) AS existing_member_frequency
              
     FROM edw.f_member_order_detail trans
     LEFT JOIN new_member_LY_YTD new_member
            ON trans.crm_member_id::integer = new_member.member_detail_id::integer
   LEFT JOIN edw.d_dl_city_tier city_tier_list
      ON trans.city_cn = city_tier_list.city_chn
     WHERE is_rrp_sales_type = 1
       AND distributor_name <> 'LBR'
       AND extract('year' FROM DATE(date_id)) = extract('year' FROM current_date) - 1 -- 年份去年
       AND DATE(date_id) < (current_date - interval '1 year')::date -- 小于去年同一天
       AND crm_member_id IS NOT NULL   -- member sales
       GROUP BY 1
      UNION ALL
       SELECT 
         'TTL' AS city_tier,
         
         CAST((sum(case when crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END) AS member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND crm_member_id IS NOT NULL THEN trans.crm_member_id ELSE NULL END) AS member_frequency,
       ---------- new
          CAST((sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT) /    (sum(case when crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end))     AS new_mbr_sales_share,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN crm_member_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE THEN crm_member_id ELSE NULL END)     AS new_member_shopper_share,
         CAST((sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/ COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN original_order_id ELSE NULL END) AS new_member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NOT NULL THEN trans.crm_member_id ELSE NULL END) AS new_member_frequency,
              
         ---------- existing
         CAST((sum(case when new_member.member_detail_id IS NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT) / COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS  NULL THEN original_order_id ELSE NULL END) AS existing_member_atv,
         CAST(COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NULL THEN original_order_id ELSE NULL END) AS FLOAT)/ COUNT(DISTINCT CASE WHEN  if_eff_order_tag IS TRUE AND new_member.member_detail_id IS NULL THEN trans.crm_member_id ELSE NULL END) AS existing_member_frequency
              
     FROM edw.f_member_order_detail trans
     LEFT JOIN new_member_LY_YTD new_member
            ON trans.crm_member_id::integer = new_member.member_detail_id::integer
     WHERE is_rrp_sales_type = 1
       AND distributor_name <> 'LBR'
       AND extract('year' FROM DATE(date_id)) = extract('year' FROM current_date) - 1 -- 年份去年
       AND DATE(date_id) < (current_date - interval '1 year')::date -- 小于去年同一天
       AND crm_member_id IS NOT NULL   -- member sales
       GROUP BY 1
    ),
    
    
    member_KPI AS (
    SELECT member_KPI_TY.city_tier,
           member_KPI_TY.member_atv,
           member_KPI_TY.member_atv/member_KPI_LY_YTD.member_atv - 1                              AS member_atv_vs_LY,
           member_KPI_TY.member_frequency,
           member_KPI_TY.member_frequency/member_KPI_LY_YTD.member_frequency - 1                  AS member_frequency_vs_LY,
           member_KPI_TY.new_mbr_sales_share,
           member_KPI_TY.new_mbr_sales_share - member_KPI_LY_YTD.new_mbr_sales_share               AS new_mbr_sales_share_vs_LY,
           member_KPI_TY.new_member_shopper_share,
           member_KPI_TY.new_member_shopper_share - member_KPI_LY_YTD.new_member_shopper_share     AS new_member_shopper_share_vs_LY,
           member_KPI_TY.new_member_atv,
           member_KPI_TY.new_member_atv/member_KPI_LY_YTD.new_member_atv - 1                       AS new_member_atv_vs_LY,
           member_KPI_TY.new_member_frequency,
           member_KPI_TY.new_member_frequency/member_KPI_LY_YTD.new_member_frequency - 1           AS new_member_frequency_vs_LY,
           member_KPI_TY.existing_member_atv,
           member_KPI_TY.existing_member_atv/member_KPI_LY_YTD.existing_member_atv   -1              AS existing_member_atv_vs_LY,
           member_KPI_TY.existing_member_frequency,
           member_KPI_TY.existing_member_frequency/member_KPI_LY_YTD.existing_member_frequency  -1   AS existing_member_frequency_vs_LY
      FROM member_KPI_TY
      LEFT JOIN member_KPI_LY_YTD
             ON member_KPI_TY.city_tier = member_KPI_LY_YTD.city_tier
    )

             

SELECT 
        ----------------sales TY vs.LY
       sales.city_tier,
       sales.traffic,
       sales.sales_rrp,
       sales.transactions,
       sales.atv,
       CAST(sales.traffic_vs_LY AS FLOAT),
       CAST(sales.sales_vs_LY AS FLOAT),
       CAST(sales.transactions_vs_LY AS FLOAT),
       CAST(sales.atv_vs_LY AS FLOAT),
       
       ------------ member KPI TY vs LY
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
         ON member_KPI.city_tier = sales.city_tier;
 
 
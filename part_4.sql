--- Part 4. Ad-hoc analysis

--- a. Demographic differences

WITH active AS (
select 
    a.lga_code,
	b.lga_name,
    sum((30 - availability_30) * price)/count(DISTINCT a.listing_id) as est_rev_per_listing
from gold.facts a
left join gold.dim_lga b on a.lga_code = b.lga_code 
WHERE a.has_availability=1
group BY 1, 2
),
ranked as (
SELECT 
	lga_code, lga_name, est_rev_per_listing,
	row_number() over(order by est_rev_per_listing DESC) AS rk_top,
	row_number() over(order by est_rev_per_listing ASC) AS rk_bottom
FROM active
)
SELECT 
	a.lga_name, a.est_rev_per_listing, a.rk_top, a.rk_bottom,
	-- total population
	Tot_P_P,
	-- gender population
	100.00 * Tot_P_M/Tot_P_P AS percent_M,
	100.00 * Tot_P_F/Tot_P_P AS percent_F,
	-- age group population
	100.00 * Age_0_4_yr_P/Tot_P_P AS percent_0_4yr,
	100.00 * Age_5_14_yr_P/Tot_P_P AS percent_5_14yr,
	100.00 * Age_15_19_yr_P/Tot_P_P AS percent_15_19yr,
	100.00 * Age_20_24_yr_P/Tot_P_P AS percent_20_24yr,
	100.00 * Age_25_34_yr_P/Tot_P_P AS percent_25_34yr,
	100.00 * Age_35_44_yr_P/Tot_P_P AS percent_35_44yr,
	100.00 * Age_45_54_yr_P/Tot_P_P AS percent_45_54yr,
	100.00 * Age_55_64_yr_P/Tot_P_P AS percent_55_64yr,
	100.00 * Age_65_74_yr_P/Tot_P_P AS percent_65_74yr,
	100.00 * Age_75_84_yr_P/Tot_P_P AS percent_75_84yr,
	100.00 * Age_85ov_P/Tot_P_P AS percent_85ov,
	-- birthplace
	100.00 * Birthplace_Australia_P/Tot_P_P AS percent_bir_aus,
	100.00 * Birthplace_Elsewhere_P/Tot_P_P AS percent_bir_elsw,
	-- indigenous population
	100.00 * Indigenous_P_Tot_P/Tot_P_P AS percent_indig,
	-- language spoken at home
	100.00 * Lang_spoken_home_Eng_only_P/Tot_P_P AS percent_lang_home_Eng,
	100.00 * Lang_spoken_home_Oth_Lang_P/Tot_P_P AS percent_lang_home_Oth,
	-- nationality
	100.00 * Australian_citizen_P/Tot_P_P AS percent_aus_citizen,
	-- education
	100.00 * High_yr_schl_comp_Yr_12_eq_P/Tot_P_P AS percent_comp_yr12,
	100.00 * High_yr_schl_comp_Yr_11_eq_P/Tot_P_P AS percent_comp_yr11,
	100.00 * High_yr_schl_comp_Yr_10_eq_P/Tot_P_P AS percent_comp_yr10,
	100.00 * High_yr_schl_comp_Yr_9_eq_P/Tot_P_P AS percent_comp_yr9,
	100.00 * High_yr_schl_comp_Yr_8_belw_P/Tot_P_P AS percent_comp_yr8,
	100.00 * High_yr_schl_comp_D_n_g_sch_P/Tot_P_P AS percent_Dn_g_sch,
 	-- median age
	c2.Median_age_persons,
	-- household size
	c2.Average_household_size
FROM ranked a
LEFT JOIN gold.census_g01 c1 ON a.lga_code = c1.lga_code
LEFT JOIN gold.census_g02 c2 ON a.lga_code = c2.lga_code
where (rk_top <=3 or rk_bottom <=3)
order by rk_top

--- demographic difference: gender, age group distribution, household size, birthplace, language, citizen, education

--- g01 -> gender, age, birthplace, education, birthplace, citizen, language
--- g02 -> median age, avg household size 



-- b. exploring the correlation between median age of a neighbourhood and estimated revenue per listing

WITH active AS (
select 
    a.lga_code,
	b.lga_name,
    sum((30 - availability_30) * price)/count(DISTINCT a.listing_id) as est_rev_per_listing
from gold.facts a
left join gold.dim_lga b on a.lga_code = b.lga_code 
WHERE a.has_availability=1
group BY 1, 2
),
ranked as (
SELECT 
	lga_code, lga_name, est_rev_per_listing,
	row_number() over(order by est_rev_per_listing DESC) AS rk_top,
	row_number() over(order by est_rev_per_listing ASC) AS rk_bottom
FROM active
)
SELECT  
	a.lga_name, a.est_rev_per_listing, rk_top,
	c2.Median_age_persons 
FROM ranked a
LEFT JOIN gold.census_g02 c2 ON a.lga_code = c2.lga_code


--- c. What will be the best type of listing (property type, room type and accommodates for) 
-- for the top 5 “listing_neighbourhood” (in terms of estimated revenue per active listing) 
-- to have the highest number of stays?
WITH active AS (
select 
    a.lga_code,
	b.lga_name,
    sum((30 - availability_30) * price)/count(DISTINCT a.listing_id) as est_rev_per_listing
from gold.facts a
left join gold.dim_lga b on a.lga_code = b.lga_code 
WHERE a.has_availability=1
group BY 1, 2
),
ranked as (
SELECT 
	lga_code, lga_name, est_rev_per_listing,
	row_number() over(order by est_rev_per_listing DESC) AS rk_top,
	row_number() over(order by est_rev_per_listing ASC) AS rk_bottom
FROM active
)
SELECT  
	l.property_type, l.room_type, l.accommodates,
	sum(30 - a.availability_30) as num_stays
FROM gold.facts a
left join ranked b on a.lga_code = b.lga_code
left join gold.dim_listing_property l 
	on l.listing_id = a.listing_id 
	and a.scraped_date::timestamp >= l.valid_from 
	and a.scraped_date::timestamp < coalesce(l.valid_to, '9999-12-31'::timestamp)
where b.rk_top<=5
group by 1,2,3
order by num_stays desc


-- d. property concentration for host with multiple listing properties

WITH host_total as (
select 
	host_id, 
	count(DISTINCT a.listing_id) AS num_listings,
	count(distinct a.lga_code) as num_lgas
FROM 
	gold.facts a
GROUP BY 1
)
select 
	case when 1<num_listings and num_listings<3 then 'a.listing_2'
		when 3<=num_listings and num_listings<4 then 'b.listing_3'
		when 4<=num_listings and num_listings<6 then 'c.listing_4_5'
		when  5<=num_listings and num_listings<10 then 'd.listing_5_9'
		when  10<=num_listings and num_listings<30 then 'e.listing_10_29'
		when  30<=num_listings then 'f.listing_30ov'
	end as num_listing_group,
	count(distinct a.host_id) as total_host,
	(100.00 * count(distinct case when num_lgas=1 then a.host_id end) / count(distinct a.host_id)) as percent_same_lga
FROM  
	host_total a
where 
	a.num_listings>1
group by 1


-- e. For hosts with a single Airbnb listing, 
-- the estimated revenue over the last 12 months 
-- cover the annualised median mortgage repayment in the corresponding LGA? 
-- Which LGA has the highest percentage of hosts that can cover it?

WITH host_total as (
select 
	host_id,
	count(distinct a.listing_id) as num_listings
FROM 
	gold.facts a
GROUP BY 1
having count(distinct a.listing_id)=1
),
host_lga as(
select
	c.lga_code, c.lga_name,	a.host_id,
	sum((30 - availability_30) * price) as est_revenue,
	count(distinct a.month) as month_cnt
from gold.facts a
inner join host_total b on b.host_id = a.host_id
left join gold.dim_lga c on a.lga_code = c.lga_code
group by 1,2,3
)
select 
	a.lga_name,
	count(distinct a.host_id) as total_host,
	(100.00 * count(distinct case when (est_revenue - c2.Median_mortgage_repay_monthly*month_cnt)>0 then a.host_id end)/count(distinct a.host_id)) as percent_h_c_rep
from 
	host_lga a
left join gold.census_g02 c2 on c2.lga_code = a.lga_code
group by 1
order by percent_h_c_rep desc

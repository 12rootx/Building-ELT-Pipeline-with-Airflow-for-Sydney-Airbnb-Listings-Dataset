--- Part 1. Load the initial raw data into Postgres

--- Set up a Bronze schema Postgres instance 

CREATE SCHEMA IF NOT EXISTS BRONZE;

--- Create the raw tables to store the initial data (text data type)

-- LGA MAPPING
CREATE TABLE IF NOT EXISTS BRONZE.RAW_NSW_LGA_CODE (
	LGA_CODE TEXT,
	LGA_NAME TEXT
);

CREATE TABLE IF NOT EXISTS BRONZE.RAW_NSW_LGA_SUBURB (
	LGA_NAME TEXT,
	SUBURB_NAME TEXT
);

-- LGA CENSUS
CREATE TABLE IF NOT EXISTS BRONZE.RAW_2016CENSUS_G01_NSW_LGA (
	LGA_CODE_2016 text,
	Tot_P_M TEXT,
	Tot_P_F TEXT,
	Tot_P_P TEXT,
	Age_0_4_yr_M TEXT,
	Age_0_4_yr_F TEXT,
	Age_0_4_yr_P TEXT,
	Age_5_14_yr_M TEXT,
	Age_5_14_yr_F TEXT,
	Age_5_14_yr_P TEXT,
	Age_15_19_yr_M TEXT,
	Age_15_19_yr_F TEXT,	
	Age_15_19_yr_P TEXT,	
	Age_20_24_yr_M TEXT,	
	Age_20_24_yr_F TEXT,	
	Age_20_24_yr_P TEXT,		
	Age_25_34_yr_M TEXT,		
	Age_25_34_yr_F TEXT,		
	Age_25_34_yr_P TEXT,		
	Age_35_44_yr_M TEXT,		
	Age_35_44_yr_F TEXT,		
	Age_35_44_yr_P TEXT,		
	Age_45_54_yr_M TEXT,		
	Age_45_54_yr_F TEXT,		
	Age_45_54_yr_P TEXT,		
	Age_55_64_yr_M TEXT,		
	Age_55_64_yr_F TEXT,		
	Age_55_64_yr_P TEXT,		
	Age_65_74_yr_M TEXT,		
	Age_65_74_yr_F TEXT,		
	Age_65_74_yr_P TEXT,		
	Age_75_84_yr_M TEXT,		
	Age_75_84_yr_F TEXT,		
	Age_75_84_yr_P TEXT,		
	Age_85ov_M TEXT,		
	Age_85ov_F TEXT,		
	Age_85ov_P TEXT,		
	Counted_Census_Night_home_M TEXT,	
	Counted_Census_Night_home_F TEXT,	
	Counted_Census_Night_home_P TEXT,	
	Count_Census_Nt_Ewhere_Aust_M TEXT,	
	Count_Census_Nt_Ewhere_Aust_F TEXT,	
	Count_Census_Nt_Ewhere_Aust_P TEXT,	
	Indigenous_psns_Aboriginal_M TEXT,	
	Indigenous_psns_Aboriginal_F TEXT,	
	Indigenous_psns_Aboriginal_P TEXT,	
	Indig_psns_Torres_Strait_Is_M TEXT,	
	Indig_psns_Torres_Strait_Is_F TEXT,	
	Indig_psns_Torres_Strait_Is_P TEXT,	
	Indig_Bth_Abor_Torres_St_Is_M TEXT,	
	Indig_Bth_Abor_Torres_St_Is_F TEXT,	
	Indig_Bth_Abor_Torres_St_Is_P TEXT,	
	Indigenous_P_Tot_M TEXT,	
	Indigenous_P_Tot_F TEXT,	
	Indigenous_P_Tot_P TEXT,	
	Birthplace_Australia_M TEXT,	
	Birthplace_Australia_F TEXT,	
	Birthplace_Australia_P TEXT,	
	Birthplace_Elsewhere_M TEXT,	
	Birthplace_Elsewhere_F TEXT,	
	Birthplace_Elsewhere_P TEXT,	
	Lang_spoken_home_Eng_only_M TEXT,	
	Lang_spoken_home_Eng_only_F TEXT,	
	Lang_spoken_home_Eng_only_P TEXT,	
	Lang_spoken_home_Oth_Lang_M TEXT,	
	Lang_spoken_home_Oth_Lang_F TEXT,	
	Lang_spoken_home_Oth_Lang_P TEXT,	
	Australian_citizen_M TEXT,	
	Australian_citizen_F TEXT,	
	Australian_citizen_P TEXT,	
	Age_psns_att_educ_inst_0_4_M TEXT,	
	Age_psns_att_educ_inst_0_4_F TEXT,	
	Age_psns_att_educ_inst_0_4_P TEXT,	
	Age_psns_att_educ_inst_5_14_M TEXT,	
	Age_psns_att_educ_inst_5_14_F TEXT,	
	Age_psns_att_educ_inst_5_14_P TEXT,	
	Age_psns_att_edu_inst_15_19_M TEXT,	
	Age_psns_att_edu_inst_15_19_F TEXT,	
	Age_psns_att_edu_inst_15_19_P TEXT,	
	Age_psns_att_edu_inst_20_24_M TEXT,	
	Age_psns_att_edu_inst_20_24_F TEXT,	
	Age_psns_att_edu_inst_20_24_P TEXT,	
	Age_psns_att_edu_inst_25_ov_M TEXT,	
	Age_psns_att_edu_inst_25_ov_F TEXT,	
	Age_psns_att_edu_inst_25_ov_P TEXT,	
	High_yr_schl_comp_Yr_12_eq_M TEXT,	
	High_yr_schl_comp_Yr_12_eq_F TEXT,	
	High_yr_schl_comp_Yr_12_eq_P TEXT,	
	High_yr_schl_comp_Yr_11_eq_M TEXT,	
	High_yr_schl_comp_Yr_11_eq_F TEXT,	
	High_yr_schl_comp_Yr_11_eq_P TEXT,	
	High_yr_schl_comp_Yr_10_eq_M TEXT,	
	High_yr_schl_comp_Yr_10_eq_F TEXT,	
	High_yr_schl_comp_Yr_10_eq_P TEXT,	
	High_yr_schl_comp_Yr_9_eq_M TEXT,	
	High_yr_schl_comp_Yr_9_eq_F TEXT,	
	High_yr_schl_comp_Yr_9_eq_P TEXT,	
	High_yr_schl_comp_Yr_8_belw_M TEXT,	
	High_yr_schl_comp_Yr_8_belw_F TEXT,	
	High_yr_schl_comp_Yr_8_belw_P TEXT,	
	High_yr_schl_comp_D_n_g_sch_M TEXT,	
	High_yr_schl_comp_D_n_g_sch_F TEXT,	
	High_yr_schl_comp_D_n_g_sch_P TEXT,	
	Count_psns_occ_priv_dwgs_M TEXT,	
	Count_psns_occ_priv_dwgs_F TEXT,	
	Count_psns_occ_priv_dwgs_P TEXT,	
	Count_Persons_other_dwgs_M TEXT,	
	Count_Persons_other_dwgs_F TEXT,	
	Count_Persons_other_dwgs_P TEXT
);

CREATE TABLE IF NOT EXISTS BRONZE.RAW_2016CENSUS_G02_NSW_LGA (
	LGA_CODE_2016 TEXT,
	Median_age_persons TEXT,	
	Median_mortgage_repay_monthly TEXT,		
	Median_tot_prsnl_inc_weekly TEXT,		
	Median_rent_weekly TEXT,		
	Median_tot_fam_inc_weekly TEXT,		
	Average_num_psns_per_bedroom TEXT,	
	Median_tot_hhd_inc_weekly TEXT,		
	Average_household_size TEXT
);

--- Listing DATA
CREATE TABLE IF NOT EXISTS BRONZE.RAW_LISTINGS(
	LISTING_ID TEXT,
	SCRAPE_ID TEXT,
	SCRAPED_DATE TEXT,
	HOST_ID TEXT,
	HOST_NAME TEXT,
	HOST_SINCE TEXT,
	HOST_IS_SUPERHOST TEXT,
	HOST_NEIGHBOURHOOD TEXT,
	LISTING_NEIGHBOURHOOD TEXT,
	PROPERTY_TYPE TEXT,
	ROOM_TYPE TEXT,
	ACCOMMODATES TEXT,
	PRICE TEXT,
	HAS_AVAILABILITY TEXT,
	AVAILABILITY_30 TEXT,
	NUMBER_OF_REVIEWS TEXT,
	REVIEW_SCORES_RATING TEXT,
	REVIEW_SCORES_ACCURACY TEXT,
	REVIEW_SCORES_CLEANLINESS TEXT,
	REVIEW_SCORES_CHECKIN TEXT,
	REVIEW_SCORES_COMMUNICATION TEXT,
	REVIEW_SCORES_VALUE TEXT
);

--- Check the schema and tables created

SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'bronze';

--- Check the data

select 'raw_nsw_lga_code' as table_name, count(*) as num_rows from bronze.raw_nsw_lga_code
union all
select 'raw_nsw_lga_suburb' as table_name, count(*) as num_rows from bronze.raw_nsw_lga_suburb
union all
select 'raw_2016census_g01_nsw_lga' as table_name, count(*) as num_rows from bronze.raw_2016census_g01_nsw_lga
union all
select 'raw_2016census_g02_nsw_lga' as table_name, count(*) as num_rows from bronze.raw_2016census_g02_nsw_lga
union all
select 'raw_listings' as table_name, count(*) as num_rows from bronze.raw_listings

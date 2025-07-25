WITH wage_share_by_county as (
	SELECT
		LPAD(a.area_fips, 5, '0') as area_fips,
		replace(replace(lower(regexp_split_to_array(a.industry_title, '\d+ ')[2]), ' ', '_'), ',', '') as industry_title,
		a.total_annual_wages / c.total_annual_wages as sector_wage_share
	FROM "ppp_loan_analysis"."bronze"."qcew_annual_average" a
	JOIN silver.county_qcew_summary c
	ON c.area_fips::INT = a.area_fips
	WHERE a.agglvl_code = 74
	AND a.own_code = 5
	AND a.year = 2019
)
SELECT *
FROM (
	PIVOT wage_share_by_county
	ON industry_title
	USING
		ifnull(MAX(sector_wage_share), 0) as wage_share
)
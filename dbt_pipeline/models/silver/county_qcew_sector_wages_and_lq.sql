WITH wage_share_by_county as (
	SELECT
		LPAD(a.area_fips, 5, '0') as area_fips,
		replace(replace(lower(regexp_split_to_array(a.industry_title, '\d+ ')[2]), ' ', '_'), ',', '') as industry_title,
		a.total_annual_wages / SUM(a.total_annual_wages) OVER (PARTITION BY a.area_fips) as sector_wage_share
	FROM {{ source('qcew', 'qcew_annual_average') }} a
	WHERE a.agglvl_code = 73
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
SELECT
	LPAD(area_fips, 5, '0') as area_fips,
	SUM(total_annual_wages) as total_annual_wages,
	ROUND(SUM(total_annual_wages) / SUM(annual_avg_emplvl), 2) as wages_per_job
FROM {{ source('qcew', 'qcew_annual_average') }}
WHERE agglvl_code = '74' OR industry_code = '10'
AND own_code = 5
AND year = 2019
GROUP BY area_fips
ORDER BY area_fips
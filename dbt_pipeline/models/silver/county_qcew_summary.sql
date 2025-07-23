SELECT
	LPAD(area_fips, 5, '0') as area_fips,
	total_annual_wages as total_annual_wages,
	oty_annual_avg_emplvl_chg / annual_avg_emplvl as yoy_job_change_pct
FROM {{ source('qcew', 'qcew_annual_average') }}
WHERE agglvl_code = 71
AND own_code = 5
AND total_annual_wages > 0
AND year = 2019
SELECT
	LPAD(fips::STRING, 5, '0') as area_fips,
	CASE
		WHEN rucc_2013 IN (1, 2, 3)
			THEN 'urban'
		WHEN rucc_2013 IN (4, 5, 6, 7)
			THEN 'suburban'
		WHEN rucc_2013 IN (8, 9)
			THEN 'urban'
	END as urbanicity
FROM {{ source('usda', 'rural_urban_2013_codes' ) }}
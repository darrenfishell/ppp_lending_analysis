SELECT
	LPAD(state::STRING, 2, '0') || LPAD(county::STRING, 3, '0') as area_fips,
	tot_pop as total_population,
	wa_male + wa_female as white_population,
	ba_male + ba_female as black_population,
	ia_male + ia_female as native_american_population,
	aa_male + aa_female as asian_population,
	na_male + na_female as native_hawaiian_pacific_islander_population,
	tom_male + tom_female as multi_ethnic_population,
	h_male + h_female as hispanic_population
FROM {{ source('census_bureau', 'census_2019_estimates') }}
WHERE year = 12 AND agegrp = 0
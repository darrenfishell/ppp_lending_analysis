SELECT
	b.census_county_name AS county,
	b.state_name as state,
	u.urbanicity as urbanicity,
	pp.current_approval_amount as ppp_loan_amount,
	q.total_annual_wages as total_annual_wages,
	q.yoy_job_change_pct as yoy_job_change_pct,
	pp.current_approval_amount / q.total_annual_wages as wage_adjusted_ppp_loan_amount,
	c.white_population / c.total_population as white_pop_share,
	c.black_population / c.total_population as black_pop_share,
	c.asian_population / c.total_population as asian_pop_share,
	(c.native_american_population + c.native_hawaiian_pacific_islander_population) / c.total_population as native_pop_share,
	c.hispanic_population / c.total_population as hispanic_share,
	e.republican / e.total as trump_share
FROM {{ ref('county_level_ppp_loans') }} pp
JOIN {{ ref('census_ppp_county_bridge') }} b
ON pp.project_county_name = b.project_county_name
AND pp.state_name = b.state_name
JOIN {{ ref('county_census_2019') }} c
ON b.area_fips = c.area_fips
JOIN {{ ref('county_qcew_summary') }} q
ON c.area_fips = q.area_fips
JOIN {{ ref('county_2016_election_results') }} e
ON b.area_fips = e.area_fips
JOIN {{ ref('urban_rural_designations') }} u
ON b.area_fips = u.area_fips
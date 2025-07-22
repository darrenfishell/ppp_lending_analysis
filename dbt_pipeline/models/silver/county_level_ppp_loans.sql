SELECT
	p.project_county_name,
	s.state_name,
	sum(initial_approval_amount) as initial_approval_amount,
	sum(current_approval_amount) as current_approval_amount,
	sum(payroll_proceed) as payroll_proceed,
	sum(forgiveness_amount) as forgiveness_amount
FROM {{ source('sba', 'paycheck_protection_loans' ) }} p
JOIN {{ source('census_bureau', 'state_crosswalk') }} s
ON p.project_state = s.stusab
WHERE p.project_county_name IS NOT NULL
GROUP BY ALL
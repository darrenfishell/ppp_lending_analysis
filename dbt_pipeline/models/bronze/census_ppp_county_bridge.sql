SELECT DISTINCT
    s.state_name,
    p.project_county_name,
    c.ctyname as census_county_name,
    LPAD(c.state::STRING, 2, '0') || LPAD(c.county::STRING, 3, '0') as area_fips
FROM {{ source('sba', 'paycheck_protection_loans') }} p
LEFT JOIN {{ source('census_bureau', 'state_crosswalk') }} s
ON
    p.project_state = s.stusab
LEFT JOIN {{ source('census_bureau', 'census_2019_estimates') }} c
ON
    s.state_name = c.stname
        AND LOWER(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        CASE
                            WHEN p.project_state = 'VA'
                            AND p.project_county_name IN ('BRISTOL', 'RADFORD', 'EMPORIA', 'SALEM')
                                THEN p.project_county_name || ' city'
                            WHEN p.project_state = 'SD'
                            AND p.project_county_name = 'PINE RIDGE'
                                THEN 'OGLALA LAKOTA'
                            WHEN p.project_county_name = 'DONA ANA'
                                THEN 'Doña Ana'
                            ELSE p.project_county_name
                        END,
                        '^(ST |SAINTE)', 'SAINT', 'i'),
                '[ -]|''', '', 'g')
            ) =
        LOWER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(c.ctyname, ' (County|Parish|City and Borough|Borough|Municipality|Census Area)', '', 'i'),
                '(St\. |Ste\. |^St )', 'SAINT', 'i'),
            '[ -]|''', '', 'g')
        )
WHERE
    c.ctyname IS NOT NULL
    AND p.project_county_name IS NOT NULL
    AND p.project_state NOT IN ('PR', 'MP', 'AE', 'VI', 'AS', 'GU')
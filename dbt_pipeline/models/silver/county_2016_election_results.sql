SELECT
    LPAD(LEFT(county_fips, 2) || RIGHT(county_fips, 3), 5, '0') as area_fips,
    year,
    REPUBLICAN as republican,
    DEMOCRAT as democrat,
    OTHER as other,
    REPUBLICAN + DEMOCRAT + OTHER as total
FROM (
    PIVOT(
        SELECT *
        FROM {{ source('harvard_elections', 'county_election_results') }}
        WHERE year = 2016
    )
    ON party
    USING SUM(candidatevotes)
    GROUP BY county_fips, year
)
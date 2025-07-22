SELECT
    TRY_CAST(RIGHT(county_fips, 3) AS INTEGER) AS county_fips,
    TRY_CAST(LEFT(county_fips, 2) AS INTEGER) AS state_fips,
    * EXCLUDE (county_fips)
FROM (
    PIVOT {{ source('harvard_elections', 'county_election_results') }}
    ON party
    USING SUM(candidatevotes)
    GROUP BY county_fips, year
)
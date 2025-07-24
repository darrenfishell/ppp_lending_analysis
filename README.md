# Summary

This project refactors the data pipeline originally written in R for a modeling and statistical analysis project exploring demographic and political variable relationships to the distribution of loans in the federal Paycheck Protection Program. 

The analysis normalized the PPP loan amount target by adjusting for local wages, using the Quarterly Census of Employment and Wages, to get a total payroll average for the baseline year of 2019, the year before the pandemic. 

The analysis found modest explantory power in a model that included 2016 election results, county racial makeup and urban/rural designations.

Most concerningly, there was a strong negative relationship between wage-adjusted PPP loan amounts and a higher share of Asian residents in a county. This is of particular concern because of the [increase in reports of discrimination and violence](https://www.pewresearch.org/race-and-ethnicity/2023/11/30/asian-americans-and-discrimination-during-the-covid-19-pandemic/) against Asian Americans following the coronavirus outbreak, according to the Pew Research Center. 

## Detailed analysis

Details of the original modeling and results are available [here](notebooks/project_regression_analysis.ipynb). 

## Future directions

The overall explanatory power (Adj. R^2 of 0.223) in the project analysis is relatively modest and further feature collection and feature engineering should be explored to better understand the relationships detected from this initial analysis. Readily available continuations include the industry makeup of a county (based on QCEW totals). 

Additionally, trends within the PPP data itself warrants exploration for assessment of overall data quality, highlighted by reporting such as [ProPublica's 2021 reporting that found a raft of apparently fraudulent loans](https://www.propublica.org/article/ppp-farms), identified by further research into loan recipients in the data. The reporting "found that Kabbage appears to have originated the most loans to businesses that don’t appear to exist and the only concentration of loans to phantom farms." Kabbage orginated around 180,000 loans, according to SBA data, and any such issues could dramatically skew the results.

## Further feature engineering and results

To start, the existing QCEW data also includes industry-level detail at the county level and is a starting point for further feature engineering. DuckDB pivot operations make this [quick work](dbt_pipeline/models/silver/county_qcew_sector_wages_and_lq.sql), breaking out each sector's wages as a share of total county wages as a new feature.


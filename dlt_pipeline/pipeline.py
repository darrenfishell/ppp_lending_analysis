import bs4
import dlt
import pandas as pd
import re
import requests
import os
import duckdb

from tqdm import tqdm
from pathlib import Path
from urllib.parse import urljoin
from dlt.sources.rest_api import RESTClient

db_name = 'ppp_loan_analysis' + '.duckdb'
raw_data_dir = Path(__file__).parents[1] / 'data' / 'raw_data'

def retrieve_csv(url, verify=True, filename=None, sep=','):
    filename = Path(url).name
    filepath = raw_data_dir / filename

    if not filepath.exists():
        session = requests.Session()
        session.verify = verify
        client = RESTClient(url, session)

        r = client.get(path='', session=session)
        r.raise_for_status()
        with open(filepath, 'w', encoding='utf-8') as outfile:
            outfile.write(r.text)

    df = pd.read_csv(filepath, sep=sep, low_memory=False)
    return df.to_dict(orient='records')

@dlt.source
def small_business_administration():

    # Create local directory
    os.makedirs(raw_data_dir, exist_ok=True)
    local_files = set(os.listdir(raw_data_dir))

    def get_remote_files():
        ppp_base_url = 'https://data.sba.gov'
        ppp_data_url = urljoin(ppp_base_url, 'dataset/ppp-foia')
        page_content = requests.get(ppp_data_url).content
        soup = bs4.BeautifulSoup(page_content, 'html.parser')
        page_csv = soup.find_all('a', attrs={'title': re.compile(r'\.csv$')})
        for csv in page_csv:
            filename = csv.get('title')
            url = urljoin(ppp_base_url, '/'.join([csv.get('href'), 'download', filename]))
            yield {
                'url': url,
                'filename': filename
            }

    def get_new_ppp_files():
        for file_info in get_remote_files():
            url = file_info['url']
            filename = file_info['filename']
            filepath = raw_data_dir / filename

            if filename not in local_files:
                print(f'Downloading {filename}')
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    with open(filepath, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024):
                            f.write(chunk)
                yield filepath

    def loaded_ppp_files():
        with duckdb.connect(db_name) as db:
            query = '''
                SELECT DISTINCT filename
                FROM bronze.paycheck_protection_loans
            '''
            files = db.sql(query).fetchall()
            file_list = [file for file in files[0]]
            return file_list

    @dlt.resource(
        parallelized=True,
        primary_key=['LoanNumber', 'filename'],
        write_disposition='merge'
    )
    def paycheck_protection_loans():
        for filepath in get_new_ppp_files():
            filename = filepath.name
            if filename not in loaded_ppp_files():
                df = pd.read_csv(filepath, encoding_errors='replace', low_memory=False)
                print(f'Downloaded {filepath} locally | {len(df)} rows')
                df['filename'] = filepath.name
                yield df.to_dict(orient='records')

    return paycheck_protection_loans

@dlt.source()
def census_bureau():

    @dlt.resource(write_disposition='replace')
    def state_crosswalk():
        url = 'https://www2.census.gov/geo/docs/reference/state.txt'
        records = retrieve_csv(url, verify=False)
        yield records

    @dlt.resource(write_disposition='replace')
    def census_2020_estimates():
        url = r'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/asrh/cc-est2019-alldata.csv'
        records = retrieve_csv(url)
        yield records

    return state_crosswalk, census_2020_estimates

@dlt.source()
def harvard_elections():

    @dlt.resource(write_disposition='replace')
    def county_election_results():
        url = 'https://dataverse.harvard.edu/api/access/datafile/11739050?format=original&gbrecs=true'
        records = retrieve_csv(url, filename='election_results_2000-2024.csv')
        yield records

    return county_election_results

@dlt.source()
def qcew():

    qcew_years = [2019, 2020]

    @dlt.resource(
        write_disposition='replace'
    )
    def qcew_area_codes():
        url = 'https://data.bls.gov/cew/doc/titles/area/area_titles.csv'
        records = retrieve_csv(url, filename='qcew_area_titles.csv')
        print(f'Collected QCEW area titles')
        yield records

    @dlt.transformer(
        write_disposition='merge',
        primary_key=['area_fips', 'own_code', 'industry_code', 'year', 'qtr'],
        parallelized=True
    )
    def qcew_annual_average(area_list):
        """
        :param area_list:
        :return: Industry totals and 2-digit NAICS totals
        from QCEW source.
        """
        for area in area_list:
            area_fips = area['area_fips']
            for year in qcew_years:
                url = f'http://data.bls.gov/cew/data/api/{year}/a/area/{area_fips}.csv'
                try:
                    yield (duckdb.execute("""
                        SELECT *
                        FROM read_csv_auto($url) q
                        WHERE (agglvl_code = 74 OR industry_code = '10')
                        AND own_code = 5
                    """, {'url': url}).df().to_dict(orient='records'))
                except Exception as e:
                    tqdm.write(f"❌ Error collecting {area_fips}-{year}: {e}")

    return (qcew_area_codes,
            qcew_area_codes | qcew_annual_average)

def main(dev_mode=False):

    pipeline = dlt.pipeline(
        pipeline_name='ppp_loan_analysis',
        destination=dlt.destinations.duckdb(Path(__file__).parents[1] / 'data' / db_name),
        progress=dlt.progress.tqdm(colour='blue'),
        dataset_name='bronze',
        dev_mode=dev_mode
    )

    sources = [
        small_business_administration(),
        # census_bureau(),
        # harvard_elections(),
        qcew()
    ]

    bronze_load_info = pipeline.run(sources)
    print(bronze_load_info)

if __name__ == '__main__':
    main(dev_mode=False)
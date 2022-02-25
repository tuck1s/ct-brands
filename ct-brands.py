#!/usr/bin/env python3
import os, sys, csv, argparse, time
from competitivetracker import CompetitiveTracker

WAIT_PERIOD = 30

def rate_limiting_in(res):
    return (res.get('code') == 503) and (res.get('message') == 'Account Over Rate Limit')

def get_company_info(co):
    """
    Get information for a company. TODO: add rate-limiting and other exception handling
    """
    while True:
        try:
            company_results = ct.core.discover.search_companies(q=co)
            break
        except ct.exceptions.CompetitiveTrackerAPIException as err:
            if rate_limiting_in(company_results):
                time.sleep(WAIT_PERIOD)
                continue
            else:
                eprint(err)
                return None

    top_company = company_results.get('companies')
    if len(top_company) < 1:
        return None

    # Choose the first entry as the top company match
    top_company_id = top_company[0].get('id')
    top_company_name = top_company[0].get('name')
    if not top_company_id:
        return None

    brand_results = ct.core.companies.get_all_company_brands(companyId=top_company_id)
    if len(brand_results) < 1:
        return None

    # Loop through all brands for the company
    result = []
    for brand in brand_results:
        brand_id = brand.get('id')
        brand_name = brand.get('name')
        # Get all the domains for the brand, including total volume
        domains = ct.intelligence.brand.get_top_domains(brandId=brand_id)
        if domains:
            domain_name_vol = { d['name']: d['projectedVolume'] for d in domains }
            domain_name_list = domain_name_vol.keys()
            # Get all ESPs for the list of sending domains
            query_period = 90
            volume_avg_and_esps = ct.domain_info.get_brand_volume_and_esps(domains=domain_name_list, timePeriod=query_period)
            for d, proj_vol in domain_name_vol.items():
                i = volume_avg_and_esps[d] # results are indexed by name
                for j in i: # and contain a list
                    esplist = [ n['name'] for n in j['esps'] ]
                    espString = ','.join(esplist)
                    result.append( {
                        'company': top_company_name,
                        'brand': brand_name,
                        'domain': d,
                        'volume': proj_vol,
                        'ESPs': espString })
    return result


def eprint(*args, **kwargs):
    """
    Print to stderr - see https://stackoverflow.com/a/14981125/8545455
    """
    print(*args, file=sys.stderr, **kwargs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Simple command-line tool to fetch company brand, domain, volume and ESPs information from Competitive Tracker')

    parser.add_argument('files', metavar='file', type=argparse.FileType('r'), default='-', nargs="*", help='files containing a list of companies to process. If omitted, reads from stdin.')
    parser.add_argument('-o', '--outfile', metavar='outfile.csv', type=argparse.FileType('w'), default='-', help='output filename (CSV format), must be writeable. If omitted, prints to stdout.')
    args = parser.parse_args()

    key = os.getenv('CT_API_KEY')
    if key == None:
        print('Please define CT_API_KEY env variable before running.')
        exit(1)
    ct = CompetitiveTracker(key)

    # can have more than one input file
    for infile in args.files:
        if infile.isatty():
            eprint('Awaiting input from {}'.format(infile.name)) # show the user we're waiting for input, without touching the stdout stream
        inh = csv.reader(infile)
        done_header = False
        for line in inh:
            for company in line:
                result = get_company_info(company)
                if result:
                    if not done_header:
                        # Write CSV file header - once only
                        eprint('Writing to {}'.format(args.outfile.name))
                        fh = csv.DictWriter(args.outfile, fieldnames=result[0].keys(), restval='', extrasaction='ignore')
                        fh.writeheader()
                        done_header = True
                    fh.writerows(result)

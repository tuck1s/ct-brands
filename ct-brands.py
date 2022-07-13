#!/usr/bin/env python3
import os, sys, csv, argparse, time, inspect
from competitivetracker import CompetitiveTracker
from competitivetracker.exceptions import CompetitiveTrackerAPIException
from competitivetracker.base import Resource

def eprint(*args, **kwargs):
    """
    Print to stderr - see https://stackoverflow.com/a/14981125/8545455
    """
    print(*args, file=sys.stderr, **kwargs)


def rate_limiting_in(self, err):
    if err.status == 503 or err.status == 429:
        msg = err.errors[0]
        if 'Account Over Rate Limit' in msg:
            eprint('Hit account daily limit! Please request more capacity')
            # Treat this as a fatal error for now
            return False
        if 'Account Over Queries Per Second Limit' in msg:
            eprint('.. pausing for per-second query rate-limiting ..')
            time.sleep(self.retry_wait_secs)
            return True
    # Any other
    return False


# Child class, with (some of) the methods of the parent class, but automatically handling rate-limiting retries
class RetryingCompetitiveTracker():
    def __init__(self, *args, **kwargs):

        self.call_log = {}
        self.retry_wait_secs = 2 # Seconds to back off when rate-limiting seen
        self.log_call(inspect.currentframe().f_code.co_name)
        # Create an upstream service to call
        self.up = CompetitiveTracker(api_key=key)
        self.core = self.Core(self.up.core) # Expose the inner classes as per https://www.geeksforgeeks.org/inner-class-in-python/
        self.intelligence = self.Intelligence(self.up.intelligence)
        self.domain_info = self.Domain_info(self.up.domain_info)

    # Log the number of calls made to each function
    def log_call(self, name):
        if name in self.call_log:
            self.call_log[name] += 1
        else:
            self.call_log[name] = 1

    def call_stats(self):
        return self.call_log

    class Core:
        def __init__(self, up):
            self.up = up
            self.discover = self.Discover(self.up.discover)
            self.companies = self.Companies(self.up.companies)

        class Discover:
            def __init__(self, up):
                self.up = up

            def search_companies(self, **kwargs):
                # self.log_call(inspect.currentframe().f_code.co_name)
                return self.up.search_companies(**kwargs)

        class Companies:
            def __init__(self, up):
                self.up = up

            def get_all_company_brands(self, *args, **kwargs):
                # self.log_call(inspect.currentframe().f_code.co_name)
                return self.up.get_all_company_brands(*args, **kwargs)

    class Intelligence:
        def __init__(self, up):
            self.up = up
            self.brand = self.Brand(self.up.brand)

        class Brand:
            def __init__(self, up):
                self.up = up

            def Get_top_domains(self, *args, **kwargs):
                # self.log_call(inspect.currentframe().f_code.co_name)
                return self.up.get_top_domains(*args, **kwargs)

    class Domain_info:
        def __init__(self, up):
            self.up = up

        def Get_brand_volume_and_esps(self, *args, **kwargs):
            # self.log_call(inspect.currentframe().f_code.co_name)
            return self.up.get_brand_volume_and_esps(*args, **kwargs)


# -----------------------------------------------------------------------------------------

def get_company_info(ct, co):
    """
    Get information for a company.
    """
    while True:
        try:
            global api_sc
            api_sc += 1

            company_results = ct.core.discover.search_companies(q=co)
            break
        except CompetitiveTrackerAPIException as err:
            if rate_limiting_in(err):
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

    while True:
        try:
            global api_cb
            api_cb += 1
            brand_results = ct.core.companies.get_all_company_brands(ct, companyId=top_company_id)
            break
        except CompetitiveTrackerAPIException as err:
            if rate_limiting_in(err):
                continue
            else:
                eprint(err)
                return None

    if len(brand_results) < 1:
        return None

    # Loop through all brands for the company
    result = []
    for brand in brand_results:
        brand_id = brand.get('id')
        brand_name = brand.get('name')
        # Get all the domains for the brand, including total volume
        while True:
            try:
                global api_td
                api_td += 1
                domains = ct.intelligence.brand.get_top_domains(brandId=brand_id)
                break
            except CompetitiveTrackerAPIException as err:
                if rate_limiting_in(err):
                    continue
                else:
                    eprint(err)
                    return None

        if domains:
            domain_name_vol = { d['name']: d['projectedVolume'] for d in domains }
            domain_name_list = domain_name_vol.keys()
            # Get all ESPs for the list of sending domains
            query_period = 90
            while True:
                try:
                    global api_bv
                    api_bv += 1
                    volume_avg_and_esps = ct.domain_info.get_brand_volume_and_esps(domains=domain_name_list, timePeriod=query_period)
                    break
                except CompetitiveTrackerAPIException as err:
                    if rate_limiting_in(err):
                        continue
                    else:
                        eprint(err)
                        return None

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
        else:
            # This is a company/brand with no sending domains. Return blank results
            result.append( {
                'company': top_company_name,
                'brand': brand_name,
                'domain': None,
                'volume': None,
                'ESPs': None })

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Simple command-line tool to fetch company brand, domain, volume and ESPs information from Competitive Tracker')

    parser.add_argument('files', metavar='file', type=argparse.FileType('r'), default=[sys.stdin], nargs="*", help='files containing a list of companies to process. If omitted, reads from stdin.')
    parser.add_argument('-o', '--outfile', metavar='outfile.csv', type=argparse.FileType('w'), default=sys.stdout, help='output filename (CSV format), must be writable. If omitted, prints to stdout.')
    args = parser.parse_args()

    key = os.getenv('CT_API_KEY')
    if key == None:
        print('Please define CT_API_KEY env variable before running.')
        exit(1)
    ct = RetryingCompetitiveTracker(key)

    # TEMP: instrument the calls
    api_sc = 0
    api_cb = 0
    api_td = 0
    api_bv = 0

    # can have more than one input file
    for infile in args.files:
        if infile.isatty():
            eprint('Awaiting input from {}'.format(infile.name)) # show the user we're waiting for input, without touching the stdout stream
        inh = csv.reader(infile)
        done_header = False
        for line in inh:
            for company in line:
                result = get_company_info(ct, company)
                if result:
                    if not done_header:
                        # Write CSV file header - once only
                        eprint('Writing to {}'.format(args.outfile.name))
                        fh = csv.DictWriter(args.outfile, fieldnames=result[0].keys(), restval='', extrasaction='ignore')
                        fh.writeheader()
                        done_header = True
                    fh.writerows(result)
                else:
                    eprint('Error: company {} skipped - no results'.format(company))

    # temp
    print(api_sc, api_cb, api_td, api_bv)
    print(ct.call_stats())
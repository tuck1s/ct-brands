#!/usr/bin/env python3
import os, sys, csv, argparse, time
from competitivetracker import CompetitiveTracker
from competitivetracker.exceptions import CompetitiveTrackerAPIException
from urllib.parse import urlparse
from copy import deepcopy

def eprint(*args, **kwargs):
    """
    Print to stderr - see https://stackoverflow.com/a/14981125/8545455
    """
    print(*args, file=sys.stderr, **kwargs)


def rate_limiting_in(err):
    retry_wait_secs = 2 # Seconds to back off when rate-limiting seen
    if err.status == 503 or err.status == 429:
        msg = err.errors[0]
        if 'Account Over Rate Limit' in msg:
            eprint('Hit account daily limit! Please request more capacity')
            # Treat this as a fatal error for now
            return False
        if 'Account Over Queries Per Second Limit' in msg:
            eprint('.. pausing for per-second query rate-limiting ..')
            time.sleep(retry_wait_secs)
            return True
    # Any other
    return False


def org_domain(url):
    """
    Allow the http:// or https:// to be present or absent
    """
    parts = urlparse(url)
    if parts.scheme and parts.netloc:
        domain = parts.netloc
    else:
        # this is not strictly a "correct" URL as it's missing the scheme. Try adding it.
        parts = urlparse('http://' + url)
        if parts.scheme and parts.netloc:
            domain = parts.netloc
        else:
            return None

    # Strip back a URL with www. prefix to just the organizational domain, for better matching
    return domain.removeprefix('www.')


def is_url_like(url):
    """
    Recognize URLs if they start with http:// https://, or start with wwww.
    """
    return urlparse(url).netloc != '' or url.startswith('www.')


# -----------------------------------------------------------------------------------------

'''
def get_company_info(ct, co):
    """
    Get information for a company, from a human-readable name
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

    companies = company_results.get('companies')
    if len(companies) < 1:
        return None

    # Choose the first entry as the top company match
    top_company_id = companies[0].get('id')
    top_company_name = companies[0].get('name')
    if not top_company_id:
        return None

    x = make_company_results('name match', top_company_name, top_company_id)
    return x
'''

class CompanyDomainResult:

    def __init__(self, website, company_name,brand_name):
        self.website = website
        self.company_name = company_name
        self.brand_name = brand_name
        self.domain = None
        self.volume = 0
        self.ESPs = set()

    def to_dict(self):
        d = self.__dict__.copy()                    # need to use copy() to avoid changes to the original
        if isinstance(d['ESPs'], list):
            d['ESPs'] =  ','.join(d['ESPs'])         # flatten the list to a string
        return d


def make_company_results(ct, website, company_name, company_id):
    """
    Provide company results in a simplified format
    """
    while True:
        try:
            global api_cb
            api_cb += 1
            brand_results = ct.core.companies.get_all_company_brands(companyId=company_id)
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

        res = CompanyDomainResult(website, company_name,brand_name)

        if domains:
            domain_name_vol = { d['name']: d['projectedVolume'] for d in domains }
            domain_name_list = domain_name_vol.keys()

            volume_avg_and_esps = get_vol_avg_and_esps(ct, domain_name_list)

            for d, proj_vol in domain_name_vol.items():
                i = volume_avg_and_esps[d] # results are indexed by name
                for j in i: # and contain a list
                    esplist = [ n['name'] for n in j['esps'] ]
                    res.domain = d
                    res.volume = proj_vol
                    res.ESPs = esplist
                    result.append(res)
    return result


def get_vol_avg_and_esps(ct, domain_name_list):
    """
    Get all ESPs for the list of sending domains. The volume is averaged for the time period, over all the domains, so it's
    not as granular as other endpoints.
    """
    while True:
        try:
            global api_bv
            api_bv += 1
            # Get data for X months back to present day
            volume_avg_and_esps = ct.domain_info.get_brand_volume_and_esps(domains=domain_name_list, timePeriod=3, precision='months')
            break
        except CompetitiveTrackerAPIException as err:
            if rate_limiting_in(err):
                continue
            else:
                eprint(err)
                return None
    return volume_avg_and_esps


def get_domain_info(ct, company_site):
    """
    Get information for an organizational domain.
    """
    while True:
        try:
            company_results = ct.core.graph.get_company_from_domain(domainName=org_domain(company_site))
            break
        except CompetitiveTrackerAPIException as err:
            if rate_limiting_in(err):
                continue
            else:
                eprint(err)
                return None

    company_name = company_results['name']
    company_id = company_results['id']


    # TODO: could walk Child Companies and/or Parent Companies too - skip for now
    # company_results.get('childCompanies')
    # company_results.get('parentCompany')

    return make_company_results(ct, company_site, company_name, company_id)


def write_result(result, verbose):
    global fh # need this to persist between calls
    """
    Write out results in CSV, in verbose or non-verbose format
    """
    if verbose:
        if fh == None:
            # Write CSV file header - once only - with full details
            fh = csv.DictWriter(args.outfile, fieldnames=result[0].to_dict().keys(), restval='', extrasaction='ignore')
            fh.writeheader()
        for r in result:
            fh.writerow(r.to_dict())
    else:
        # Collect all domain results together, if they have the same set of ESPs in use
        grouped_r = None
        for r in result:
            if grouped_r == None:
                grouped_r = deepcopy(r)
                del grouped_r.domain
                grouped_r.domain_count = 1 # count them
            else:
                if r.website == grouped_r.website and r.company_name == grouped_r.company_name \
                and r.brand_name == grouped_r.brand_name and r.ESPs == grouped_r.ESPs:

                    grouped_r.volume += r.volume
                    grouped_r.domain_count += 1
                else:
                    # Found something new - output what we have so far
                    if fh == None:
                        # Write CSV file header - once only - with full details
                        fh = csv.DictWriter(args.outfile, fieldnames=grouped_r.to_dict().keys(), restval='', extrasaction='ignore')
                        fh.writeheader()

                    fh.writerow(grouped_r.to_dict())
                    grouped_r = deepcopy(r)
                    del grouped_r.domain
                    grouped_r.domain_count = 1 # count them
        # output the final group
        fh.writerow(grouped_r.to_dict())


# -----------------------------------------------------------------------------------------
# Main code
# -----------------------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Simple command-line tool to fetch company brand, domain, volume and ESPs information from Competitive Tracker')

    parser.add_argument('files', metavar='file', type=argparse.FileType('r'), default=[sys.stdin], nargs="*", help='files containing a list of company names or website(s) to process. If omitted, reads from stdin.')
    parser.add_argument('-o', '--outfile', metavar='outfile.csv', type=argparse.FileType('w'), default=sys.stdout, help='output filename (CSV format), must be writable. If omitted, prints to stdout.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show each sending domain as a separate result')
    args = parser.parse_args()

    key = os.getenv('CT_API_KEY')
    if key == None:
        print('Please define CT_API_KEY env variable before running.')
        exit(1)
    ct = CompetitiveTracker(key)

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
        fh = None # Note this is used inside write_result
        eprint('Writing to {}'.format(args.outfile.name))
        for line in inh:
            for company_site in line:
                # map URL-like names into organizational domains
                if is_url_like(company_site):
                    result = get_domain_info(ct, company_site)
                    if result:
                        write_result(result, args.verbose)
                    else:
                        eprint('! company {} skipped - no results'.format(company_site))
                else:
                    eprint('! {} is not a valid URL'.format(company_site))
                    # TODO you can fall back and use name lookups with result = get_company_info(ct, company_site)

    # temp
    print(api_sc, api_cb, api_td, api_bv)

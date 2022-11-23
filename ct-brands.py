#!/usr/bin/env python3
import os, sys, csv, argparse, time, json
from competitivetracker import CompetitiveTracker
from competitivetracker.exceptions import CompetitiveTrackerAPIException
from urllib.parse import urlparse
from copy import deepcopy
from typing import List

def eprint(*args, **kwargs):
    """
    Print to stderr - see https://stackoverflow.com/a/14981125/8545455
    """
    print(*args, file=sys.stderr, **kwargs)


def rate_limiting_in(err:CompetitiveTrackerAPIException):
    """
    Return whether a particular error value indicates rate-limiting
    """
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


def org_domain(url:str):
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


def is_url_like(url:str):
    """
    Recognize URLs if they start with http:// https://, or start with wwww.
    """
    return urlparse(url).netloc != '' or url.startswith('www.')


def get_company_info_from_website(ct:CompetitiveTracker, company_site:str, api_calls:dict):
    """
    Get information for an organizational domain.
    """
    while True:
        try:
            co = ct.core.graph.get_company_from_domain(domainName=org_domain(company_site))
            register_call(api_calls, 'ct.core.graph.domain')
            break
        except CompetitiveTrackerAPIException as err:
            if rate_limiting_in(err):
                continue
            else:
                eprint(err)
                return None

    res = make_company_results(ct, company_site, co.get('name'), co.get('id'), co.get('brands'), api_calls)
    # recursively walk any child companies
    for child_co in co.get('childCompanies'):
        res += company_info(ct, child_co.get('id'), api_calls)
    return res


def get_company_info_from_sending_domain(ct:CompetitiveTracker, sending_domain:str, api_calls:dict):
    """
    Get information from a sending domain, working back to the Company
    """
    while True:
        try:
            co = ct.core.graph.get_company_from_domain(domainName=sending_domain)
            register_call(api_calls, 'ct.core.graph.domain')
            break
        except CompetitiveTrackerAPIException as err:
            if rate_limiting_in(err):
                continue

            # check if we should quietly return no results
            if err.errors:
                response = json.loads(err.errors[0])
                if(response.get('code') == 404 and response.get('type') == 'MISSING'):
                    return None
       
            eprint(err)
            return None

    res = make_company_results(ct, sending_domain, co.get('name'), co.get('id'), co.get('brands'), api_calls)
    # recursively walk any child companies
    for child_co in co.get('childCompanies'):
        res += company_info(ct, child_co.get('id'), api_calls)
    return res


def get_company_info_from_name(ct:CompetitiveTracker, co:str, api_calls:dict):
    """
    Get information for a company, from a human-readable name
    """
    while True:
        try:
            co = ct.core.discover.search_companies(q=co)
            register_call(api_calls, 'ct.core.discover.company')

            break
        except CompetitiveTrackerAPIException as err:
            if rate_limiting_in(err):
                continue
            else:
                eprint(err)
                return None

    companies = co.get('companies')
    if len(companies) < 1:
        return None

    # Choose the first entry as the top company match
    top_company_id = companies[0].get('id')
    return company_info(ct, top_company_id, api_calls)


def company_info(ct:CompetitiveTracker, id:int, api_calls:dict):
    """
    Return a list of details for this company (from id)
    """
    while True:
        try:
            co = ct.core.graph.get_company_from_id(id)
            register_call(api_calls, 'ct.core.graph.company')

            break
        except CompetitiveTrackerAPIException as err:
            if rate_limiting_in(err):
                continue
            else:
                eprint(err)
                return None
    if not co:
        return None

    res = make_company_results(ct, co.get('url'), co.get('name'), id, co.get('brands'), api_calls)
    # recursively walk any child companies
    for child_co in co.get('childCompanies'):
        res += company_info(ct, child_co.get('id'), api_calls)
    return res


def register_call(d:dict, name:str):
    """
    Counts API calls by name, updating results in d (side-effect)
    """
    if name in d:
        d[name] += 1
    else:
        d[name] = 1


class CompanyDomainResult:
    def __init__(self, website:str, company_name:str, brand_name:str):
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


def make_company_results(ct:CompetitiveTracker, website:str, company_name:str, company_id:int, brand_results:list, api_calls:dict):
    """
    Provide company results in a simplified format
    """
    if brand_results == None:
        # Make a call to get these
        while True:
            try:
                brand_results = ct.core.companies.get_all_company_brands(companyId=company_id)
                register_call(api_calls, 'ct.core.companies.get_all_company_brands')
                break
            except CompetitiveTrackerAPIException as err:
                if rate_limiting_in(err):
                    continue
                else:
                    eprint(err)
                    return None

        if len(brand_results) < 1:
            return None

    # Loop through all brands for the company. This gives us the *sending volumes*
    result = []
    for brand in brand_results:
        brand_id = brand.get('id')
        brand_name = brand.get('name')
        # Get all the domains for the brand, including total volume
        while True:
            try:
                domains = ct.intelligence.brand.get_top_domains(brandId=brand_id)
                register_call(api_calls, 'ct.intelligence.brand.get_top_domains')
                break
            except CompetitiveTrackerAPIException as err:
                if rate_limiting_in(err):
                    continue
                else:
                    eprint(err)
                    return None

        res = CompanyDomainResult(website, company_name, brand_name)

        if domains:
            domain_name_vol = { d['name']: d['projectedVolume'] for d in domains }
            domain_name_list = domain_name_vol.keys()

            volume_avg_and_esps = get_vol_avg_and_esps(ct, domain_name_list, api_calls)

            for d, proj_vol in domain_name_vol.items():
                i = volume_avg_and_esps[d] # results are indexed by name
                for j in i: # and contain a list
                    esplist = [ n['name'] for n in j['esps'] ]
                    res.domain = d
                    res.volume = proj_vol
                    res.ESPs = esplist
                    result.append(res)
    return result


def get_vol_avg_and_esps(ct:CompetitiveTracker, domain_name_list:list, api_calls:dict):
    """
    Get all ESPs for the list of sending domains. The volume is averaged for the time period, over all the domains, so it's
    not as granular as other endpoints.

    Also register the api_calls made
    """
    while True:
        try:
            # Get data for X months back to present day
            register_call(api_calls, 'ct.domain_info.get_brand_volume_and_esps')
            volume_avg_and_esps = ct.domain_info.get_brand_volume_and_esps(domains=domain_name_list, timePeriod=3, precision='months')
            break
        except CompetitiveTrackerAPIException as err:
            if rate_limiting_in(err):
                continue
            else:
                eprint(err)
                return None
    return volume_avg_and_esps


class ResultWriter:
    def __init__(self, verbose:bool):
        self.fh = None
        self.verbose = verbose

    def write(self, result: List[CompanyDomainResult]):
        """
        Write out results in CSV, in verbose or non-verbose format
        """
        if self.verbose:
            if self.fh == None:
                # Write CSV file header - once only - with full details
                self.fh = csv.DictWriter(args.outfile, fieldnames=result[0].to_dict().keys(), restval='', extrasaction='ignore')
                self.fh.writeheader()
            for r in result:
                self.fh.writerow(r.to_dict())
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
                        if self.fh == None:
                            # Write CSV file header - once only - with full details
                            self.fh = csv.DictWriter(args.outfile, fieldnames=grouped_r.to_dict().keys(), restval='', extrasaction='ignore')
                            self.fh.writeheader()

                        self.fh.writerow(grouped_r.to_dict())
                        grouped_r = deepcopy(r)
                        del grouped_r.domain
                        grouped_r.domain_count = 1 # count them
            # output the final group
            if grouped_r and self.fh:
                self.fh.writerow(grouped_r.to_dict())


# -----------------------------------------------------------------------------------------
# Main code
# -----------------------------------------------------------------------------------------

if __name__ == "__main__":
    input_types = ['company_name', 'website', 'sending_domain']
    parser = argparse.ArgumentParser(
        description='Simple command-line tool to fetch company brand, domain, volume and ESPs information from Competitive Tracker')

    parser.add_argument('files', metavar='file', type=argparse.FileType('r'), nargs='+', help='files containing a list of company names or website(s) to process.')
    parser.add_argument('-o', '--outfile', metavar='outfile.csv', type=argparse.FileType('w'), default=sys.stdout, help='output filename (CSV format), must be writable. If omitted, prints to stdout.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show each sending domain as a separate result')
    parser.add_argument('--input_type', choices=input_types, default='company_name', help='Type of input file')
    parser.add_argument('--api_outfile', metavar='api_outfile', type=argparse.FileType('w'), nargs='?', help='output API call counts in a file')
    args = parser.parse_args()

    key = os.getenv('CT_API_KEY')
    if key == None:
        print('Please define CT_API_KEY env variable before running.')
        exit(1)
    ct = CompetitiveTracker(key)

    api_call_counts = []
    # can have more than one input file
    for infile in args.files:
        if infile.isatty():
            eprint('Awaiting input from {}'.format(infile.name)) # show the user we're waiting for input, without touching the stdout stream
        inh = csv.reader(infile)
        fh = ResultWriter(args.verbose)
        eprint('Writing to {}'.format(args.outfile.name))
        for line in inh:
            for company_site in line:
                # collect api_calls results as a side-effect of the processing (pass by reference)
                # start by tagging the input data
                api_calls = { '_company_site': company_site }
                if args.input_type == input_types[0]:
                    result = get_company_info_from_name(ct, company_site, api_calls)
                elif args.input_type == input_types[1]:
                    result = get_company_info_from_website(ct, company_site, api_calls)
                elif args.input_type == input_types[2]:
                    result = get_company_info_from_sending_domain(ct, company_site, api_calls)
                else:
                    eprint('Unrecognised input type', input_types)
                    os.exit(1)
                api_call_counts.append(api_calls)
                if result:
                    fh.write(result)
                else:
                    eprint('! entry {} skipped - no results'.format(company_site))
    
    if args.api_outfile:
        u = set().union(*api_call_counts)
        api_writer = csv.DictWriter(args.api_outfile, sorted(u))
        api_writer.writeheader()
        api_writer.writerows(api_call_counts)

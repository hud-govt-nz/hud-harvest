# Scraper
# Tools for getting data
import json, requests, re, gzip
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile
from bs4 import BeautifulSoup

def get_link(raw_page, ln_pattern, host = ""):
    soup = BeautifulSoup(raw_page, "html.parser")
    links = soup.findAll("a", { "href": re.compile(ln_pattern) })
    links = [a["href"] for a in links]
    if not links:
        raise Exception(f"Link not found! Check your ln_pattern ({ln_pattern}).")
    if len(links) > 1:
        for l in links: print(f"Matching link: '{l}'")
        raise Exception(f"More than one link found! Check your ln_pattern ({ln_pattern}).")
    return f"{host}{links[0]}"

def download(src_url, dst_fn):
    print(f"Downloading {src_url}...")
    res = requests.get(src_url, stream=True)
    dst = Path(dst_fn)
    if res.headers.get("Transfer-Encoding") == "chunked":
        print(f"...as a chunked transfer...")
        if dst.exists():
            print("CAUTION: Can't match sized on a chunked transfer, overwriting...")
    elif res.headers.get("Content-Encoding") in ["gzip"]:
        print("CAUTION: Can't match sized on a compressed transfer, overwriting...")
    elif res.headers.get("Content-Length"):
        src_size = int(res.headers["Content-Length"])
        print(f"...file is {src_size} bytes...")
        if dst.exists():
            dst_size = dst.stat().st_size
            if dst_size == 0:
                pass # Ignore empty files
            elif src_size == dst_size:
                print("Local file of the same size already exists, ignoring.")
                return
            elif "Last-Modified" in res.headers:
                src_date = datetime.strptime(res.headers["Last-Modified"], "%a, %d %b %Y %H:%M:%S %Z")
                dst_date = datetime.fromtimestamp(dst.stat().st_mtime)
                print(f"Local file exists ({dst_size} bytes, last modified {dst_date}), "
                      f"but does not match remote file ({src_size} bytes, last modified {src_date})! "
                      f"Delete local file if you want to continue.")
                raise Exception("Mismatched local file!")
    else:
        raise Exception("No 'Content-Length' in header and not chunked transfer. What kind of transfer is this??")
    with open(dst_fn, "wb") as f:
        res = requests.get(src_url)
        res.raise_for_status()
        f.write(res.content)

# Extract a specific file based on targ_pattern from src_fn, and save it as dst_fn
def unzip(src_fn, fn_pattern, dst_fn):
    print(f"Unzipping {src_fn}...")
    with ZipFile(src_fn, "r") as zf:
        fl = [f for f in zf.filelist if re.match(fn_pattern, f.filename)]
        if not fl: raise Exception(f"No matching files found in {src_fn}! Check your fn_pattern ({fn_pattern}).")
        if len(fl) > 1: raise Exception(f"More than one matching files found in {src_fn}! Check your fn_pattern ({fn_pattern}).")
        with open(dst_fn, "wb") as output:
            with zf.open(fl[0], "r") as input:
                output.write(input.read())

# gunzip is simpler since it's always just one file
def gunzip(src_fn, dst_fn = None):
    print(f"Unzipping {src_fn}...")
    if dst_fn is None: dst_fn = re.match("(.*)\.gz", src_fn)[1]
    with gzip.open(src_fn, "rb") as in_f:
        with open(dst_fn, "wb") as out_f:
            out_f.write(in_f.read())


class StatsNZ:
# Scrapes data from StatsNZ
#
# The StatsNZ website works by passing a serialised JSON object as a div
# attribute, then deserialising it and rendering the page. This means when you
# download the page, the links don't exist. So instead, we need to deserialise
# the JSON object, then read the links inside. This tool does all that for you.
#
# For the main datasets for which have comprehensive CSVs, you can get the
# links with:
# StatsNZ.get_data_url("building-consents-issued-.*-csv.zip")
#
# If it's not on the big-data page, you'll need to go through the information
# release page. You need to know the filter, e.g.:
# https://www.stats.govt.nz/publications?filters=Rental%20price%20indexes
#
# release_url = StatsNZ.get_latest_release_url("Rental price indexes")
# data_url = StatsNZ.get_data_url("rental-price-index-.*-csv.csv", release_url)

    # Combines the two steps below
    def get_latest_data_url(filters, ln_pattern):
        release_url = StatsNZ.get_latest_release_url(filters)
        data_url = StatsNZ.get_data_url(ln_pattern, release_url)
        return data_url

    # Returns the latest information release page for a given filter
    def get_latest_release_url(filters):
        url = f"https://www.stats.govt.nz/information-releases/?filters={filters}"
        data = StatsNZ.get_page_data(url)
        pages = data["PaginatedBlockPages"]
        latest = sorted(pages, key = lambda d: d["PageDate"])[-1]
        release_url = f"https://www.stats.govt.nz{latest['PageLink']}"
        return release_url

    # Returns matching link from a page
    def get_data_url(ln_pattern, release_url = "https://www.stats.govt.nz/large-datasets/csv-files-for-download/"):
        data = StatsNZ.get_page_data(release_url)
        # Extract documents
        docs = []
        for d in data["PageBlocks"]:
            if d["ClassName"] == "DocumentBlock":
                docs += d["BlockDocuments"]
        docs = [d for d in docs if re.match(ln_pattern, d["Name"])]
        # Extract link (must be one and only one match)
        links = [d["DocumentLink"] for d in docs]
        if not links:
            raise Exception(f"Link found at {release_url}! Check your ln_pattern ({ln_pattern}).")
        if len(links) > 1:
            for l in links: print(f"Matching link: '{l}'")
            raise Exception(f"More than one link found at {release_url}! Check your ln_pattern ({ln_pattern}).")
        return f"https://www.stats.govt.nz{links[0]}"

    # Extracts the pageViewData from a given page
    def get_page_data(url):
        res = requests.get(url)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        data_div = soup.find("div", { "id": "pageViewData" })
        data = json.loads(data_div["data-value"])
        return data

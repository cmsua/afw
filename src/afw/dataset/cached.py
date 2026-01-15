"""
Utilities to interface with tools only available on select platforms (eg. dasgoclient) or with credentials (xsecdb, rucio)
"""

# Attribution copied from internet - modified to include correct link
# Source - https://stackoverflow.com/questions/16463582/memoize-to-disk-python-persistent-memoization
# Posted by georg, modified by community. See post 'Timeline' for change history
# Retrieved 2025-12-03, License - CC BY-SA 3.0

import atexit
import io
import json
import logging
import os
import pickle
import re
import subprocess

import yaml

logger = logging.getLogger("Local Cache")

# For xsecdb
cookie_path = os.path.abspath("cookie.txt")
base_url = "https://xsecdb-xsdb-official.app.cern.ch"

override_file = os.path.join(os.curdir, "xsecdb-overrides.yaml")
overrides = {}
if os.path.exists(override_file):
    logger.debug(f"Loading overrides file {override_file}")
    with open(override_file, "r") as file:
        overrides = yaml.safe_load(file)

    if not isinstance(overrides, dict):
        logger.critical("Overrides is not a valid python dict object! Replacing...")
        overrides = {}

# Curl client
c = None

# Rucio client
client = None


def persist_to_file(file_name: str):
    """
    A decorator used for caching function results. Decorated functions will persist their results to pickle files (and yaml files for inspection) on program exit.

    Args:
        file_name (str): The base name (without extension) to save results to
    """
    cache_file = os.path.join(os.curdir, "cache", file_name)

    yaml_file = cache_file + ".yaml"
    pickle_file = cache_file + ".pkl"

    cache = {}
    if os.path.exists(pickle_file):
        with open(pickle_file, "rb") as file:
            cache = pickle.load(file)
    elif os.path.exists(yaml_file):
        with open(yaml_file, "r") as file:
            # Not safe-load
            # If someone's modified your yaml files, this may execute arbitrary code
            cache = yaml.full_load(file)
            logger.warning("Loaded cache from yaml file and not pickle!")

    if not isinstance(cache, dict):
        logger.critical(f"Cache loaded as non-dict, resetting: {cache}")
        cache = {}
    if "_changed" in cache:
        del cache["_changed"]

    def save_cache():
        if not cache.get("_changed", False):
            return
        with open(yaml_file, "w") as file:
            yaml.dump(cache, file)
        with open(pickle_file, "wb") as file:
            pickle.dump(cache, file)

    atexit.register(save_cache)

    def decorator(func):
        def new_func(param):
            if param not in cache:
                cache[param] = func(param)
                cache["_changed"] = True
            return cache[param]

        return new_func

    return decorator


# DASGOCLIENT
@persist_to_file("dasgoclient")
def run_dasgoclient(query: str) -> dict:
    """
    Run dasgoclient with a provided query and return the results as a json object

    This function is cached

    Args:
        query (str): The DAS query to run

    Returns:
        dict: The result from dasgoclient
    """

    logger.debug(f"Loading from dasgoclient: {query}")
    response = subprocess.check_output(
        ["/cvmfs/cms.cern.ch/common/dasgoclient", "-query", query, "-json"]
    )

    # logger.debug(f"Got entry {response}")
    return json.loads(response)


# XSecDB
# Only save authenticated CURLs
@persist_to_file("xsecdb")
def do_request(das_key: str) -> dict:
    """
    Query xsecdb for a given DAS key and returns the json result

    This requires a cookie.txt file and persists to disk

    Args:
        das_key (str): The DAS key to query xsecdb for

    Returns:
        dict: The result from xsecdb
    """
    # Only import if needed
    import pycurl

    if not os.path.exists(cookie_path):
        raise ValueError(
            "Cookie does not exist! Please create a cookies.txt file using a web extension (as CLI tools do not support 2fa)"
        )

    global c
    if c is None:
        c = pycurl.Curl()
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        # c.setopt(pycurl.COOKIEJAR, cookie_path)
        c.setopt(
            pycurl.HTTPHEADER,
            ["Content-Type: application/json", "Accept: application/json"],
        )
        c.setopt(pycurl.COOKIEFILE, cookie_path)
        c.setopt(pycurl.VERBOSE, 0)
        c.setopt(pycurl.URL, f"{base_url}/api/search")

    request = {
        "search": {"DAS": das_key},
        "orderBy": {"DAS": -1},
        "pagination": {"currentPage": 0, "pageSize": 10},
    }
    body = json.dumps(request)
    buffer = io.BytesIO()

    c.setopt(pycurl.WRITEDATA, buffer)
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, body)
    c.perform()

    response = json.loads(buffer.getvalue())
    response_code = c.getinfo(c.RESPONSE_CODE)
    return response, response_code


def get_cross_section(fileset: str) -> float:
    """
    Try and find the cross-section for a given fileset

    This will check for overrides before querying xsecdb and interpreting results

    Args:
        fileset (str): The DAS key of the fileset

    Returns:
        float: The returned cross-section
    """
    # Check first!
    if fileset in overrides:
        logger.debug(f"Using override for fileset {fileset}")
        return overrides[fileset]

    logger.debug(f"Getting cross-section for fileset {fileset}")

    # Search for part matching /TTZH_TuneCP5_13p6TeV_madgraph-pythia8/Run3Summer22EE
    search_key = re.split(r"NanoAODv\d+", fileset)[0]
    logger.debug(f"Querying xsecdb with search key {search_key}")
    result, response_code = do_request(search_key)

    xsecs = [entry["cross_section"] for entry in result]

    # Checks
    if response_code != 200:
        logger.critical(
            f"xsecdb failed with response code {response_code} - is your cookie valid?"
        )
        return

    if len(result) > 1:
        if all([xsec == xsecs[0] for xsec in xsecs]):
            logger.warning(
                f"Fileset {fileset} has more than one result in xsecdb, but they all share a cross-section - returning result"
            )
        else:
            logger.critical(f"Fileset {fileset} has more than one result in xsecdb!")
            logger.debug(result)
            return
    if len(result) == 0:
        logger.critical(f"Fileset {fileset} has no result in xsecdb!")
        return

    return float(xsecs[0])


# Get xsec from dataset
@persist_to_file("rucio")
def get_all_matching(query: str) -> list[dict]:
    """
    Find all DAS keys matching a given pattern

    This requires rucio and persists to disk

    Args:
        query (str): The query to pass to Rucio

    Returns:
        list[dict]: A list of Rucio DIDs
    """
    logger.debug(f"Querying rucio with query {query}")

    global client
    if client is None:
        from coffea.dataset_tools import rucio_utils

        client = rucio_utils.get_rucio_client()

    response = list(
        client.list_dids(scope="cms", filters={"name": query, "type": "container"})
    )
    return response

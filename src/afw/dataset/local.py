"""
Utilities for building datasets from patterns and applying metadata (eg. cross-section)
"""

import logging

from . import cached

# Load Cache
logger = logging.getLogger("Local Dataset Builder")

veto = None


def is_vetoed(file: str) -> bool:
    """
    Checks if a file is specifically vetoed

    Args:
        file (str): The path to a given file

    Returns:
        bool: Whether the file is vetoed or not
    """
    # Check for presence
    global veto
    if veto is None:
        with open("veto-files.txt", "r") as file:
            veto = [line.strip() for line in file]

    return file in veto


def build_datasets(defs, xcache_host: str = None):
    # Actually load from Rucio
    result = {}
    # Convert to filesets (aka das keys)
    for query, metadata in defs.items():
        for das_key in cached.get_all_matching(query):
            result[das_key] = {"metadata": metadata.copy()}

    # Do magic with dasgoclient
    for key, val in result.items():
        response = cached.run_dasgoclient(f"file dataset={key}")

        nevents = 0
        files = {}
        # Parse dasgoclient results
        for entry in response:
            if len(entry["file"]) != 1:
                raise ValueError(f"More than one file for file object: {entry}")
            file = entry["file"][0]

            name = file["name"]
            if is_vetoed(name):
                logger.critical(f"Skipping file due to entry in veto list: {file}")
                continue

            if "nevents" not in file:
                logger.critical(f"File is missing nevents: {file}")
                continue

            if file["nevents"] == 0:
                logger.warning(f"Skipping file due to 0 events: {file['name']}")
                continue

            # Save file, add nevents
            files[xcache_host + file["name"]] = "Events"
            nevents += file["nevents"]

        val["metadata"]["nevents"] = nevents
        val["files"] = files

    # Add xsecs
    for key, val in result.items():
        if val["metadata"].get("isData", False):
            continue
        if "xsec" in val["metadata"]:
            logger.debug(
                f"Skipping xsecdb for fileset as already present in definition: {key}"
            )
            continue
        val["metadata"]["xsec"] = cached.get_cross_section(key)

    # Check for empty
    for fileset_name, fileset in list(result.items()):
        if len(fileset["files"]) == 0:
            logger.critical(
                f"Fileset {fileset_name} (short name {fileset['metadata']['shortName']}) has zero files!"
            )
            del result[fileset_name]

    return result

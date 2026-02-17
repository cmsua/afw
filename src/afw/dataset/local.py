"""
Utilities for building datasets from patterns and applying metadata (eg. cross-section)
"""

import logging

from . import cached

# Load Cache
logger = logging.getLogger("Local Dataset Builder")

veto = None


# Remove vetoed files
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


def remove_obsolete_versions(dataset: dict) -> dict:
    """
    Removes obsolete versions with no files from a given dataset. Assumes dataset keys are in the format XXX-v1/XXX

    Params:
        defs (dict): The dataset to procecss

    Returns:
        dict: The processed dict
    """
    # List all available
    vers_avail_map = {}
    for key, fileset in list(dataset.items()):
        name, vers = key.rsplit("-", 1)
        vers = int(vers.split("/")[0].replace("v", ""))

        # No existing => continue
        if name not in vers_avail_map:
            vers_avail_map[name] = []

        vers_avail_map[name] += [vers]

    # Start pruning
    for name, vers_avail in vers_avail_map.items():
        vers_avail = list(sorted(vers_avail))
        # For each superseded version
        for i, vers in enumerate(vers_avail[:-1]):
            # Find outdated keys
            outdated_key_prefix = name + "-v" + str(vers)
            outdated_keys = [
                key for key in dataset if key.startswith(outdated_key_prefix)
            ]

            # For each outdated key
            for key in outdated_keys:
                # Prune if possible
                if len(dataset[key]["files"]) != 0:
                    logger.critical(
                        f"Outdated key still has files remaining, removing anyways! - superseded by version {vers_avail[-1]} ({key})"
                    )

                logger.debug(
                    f"Deleting outdated version {vers} as zero files are available: {key}"
                )
                del dataset[key]

    return dataset


def build_datasets(defs, xcache_host: str = None, max_files: int = None) -> dict:
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
        num_files = 0
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
            file_path = (
                xcache_host + file["name"] if xcache_host is not None else file["name"]
            )

            files[file_path] = "Events"

            num_files += 1
            nevents += file["nevents"]

            # Break if at cap
            if max_files is not None and num_files == max_files:
                break

    result = remove_obsolete_versions(result)

    # Check for empty
    for fileset_name, fileset in list(result.items()):
        if len(fileset["files"]) == 0:
            logger.critical(
                f"Fileset {fileset_name} (short name {fileset['metadata']['shortName']}) has zero files!"
            )
            del result[fileset_name]

    # Add xsecs
    for key, val in list(result.items()):
        # Data has no xsec
        if val["metadata"].get("isData", False):
            continue

        # If defined by yaml, skip
        if "xsec" in val["metadata"]:
            logger.debug(
                f"Skipping xsecdb for fileset as already present in definition: {key}"
            )
            continue

        # Assign xsec to value, skip if zero
        xsec = cached.get_cross_section(key)
        if xsec == 0:
            logger.critical(f"Cross-section is zero for key {key}, removing!")
            del result[key]

        val["metadata"]["xsec"] = xsec

    return result

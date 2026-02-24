"""
Utilities for loading definitions from dataset yaml files
"""

import logging
import yaml

from . import cached

logger = logging.getLogger("Dataset Definitions")


def build_datasets(base: str | dict, max_files: int = None) -> dict[str, dict]:
    """
    Builds a complete dataset from a given file.

    Args:
        base (str | dict): If a string, the path to any yaml file template for a dataset. Otherwise, a dict value.
        max_files (int, default None): If present, the amount of files to restrict to per section

    Returns:
        dict[str, dict]: A mapping from das keys to a dictionary containing ``files`` and ``metadata`` keys.
    """
    if isinstance(base, str):
        # Open file
        with open(base, "r") as file:
            result = yaml.safe_load(file)
    else:
        result = base

    # Build and expand templates
    result = build_templates(result)
    result = expand_templates(result)

    # Remove old datasets before populating
    result = remove_obsolete_versions(result)

    # Populate each section
    new_result = {}
    for das_key, section in result.items():
        section = populate_files(das_key, section, max_files)

        # Check for fails
        if len(section["files"]) == 0:
            logger.critical(
                f"Fileset {das_key} (short name {section['metadata']['shortName']}) has zero files!"
            )
            continue

        # Populate xsec if needed
        if section["metadata"].get("xsec", None) is None:
            xsec = cached.get_cross_section(das_key)
            if xsec == 0:
                logger.critical(f"Cross-section is zero for key {das_key}, skipping!")
            continue
            section["metadata"]["xsec"] = xsec

        # Save
        new_result[das_key] = section

    return new_result


def build_templates(defs: list[dict[str, dict]]) -> dict[str, dict]:
    """
    Loads and builds custom dataset definitions from a given file. This will return a map of the form ``[das key template, {fileset: dict, metadata: dict}]``

    Args:
        defs (list[dict]): A list of objects containing ``datasets`` (templateable) and ``metadata``

    Returns:
        dict[str, dict]: A mapping from das key templates to a dict with a "metadata" key
    """

    result = {}
    for entry in defs:
        # Copy metadata
        for dataset in entry["datasets"]:
            result[dataset] = {"metadata": entry.get("metadata", {}).copy()}

    return result


def expand_templates(templates: dict):
    """
    Expands a dict of [das key template, dict] to [das key, dict], copying the second parameter for each das key given

    Args:
        templates (dict[str, dict]): A mapping from das key templates (eg. can support wildcards) to a dict object

    Returns:
        dict[str, dict]: A mapping from das keys to a dict with a metadata key
    """
    # Actually load from Rucio
    result = {}
    # Convert to filesets (aka das keys)
    for query, section in templates.items():
        for das_key in cached.get_all_matching(query):
            result[das_key] = section.copy()

    return result


def remove_obsolete_versions(dataset: dict) -> dict:
    """
    Removes obsolete versions with no files from a given dataset. Assumes dataset keys are in the format XXX-v1/XXX

    Params:
        defs (dict): The dataset to process

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


def populate_files(das_key: str, section: dict = {}, max_files: int = None):
    """
    Populates the ``files`` key for a given das key, and computes ``nevents`` and ``nevents_total``.

    Args:
        das_key (str): The das key to look up
        section (dict, default {}): The existing section to use, for passing along pre-existing metadata
        max_files (int, default None): If present, the amount of files to restrict to

    Returns:
        dict[str, dict]: A dictionary with ``files`` and ``metadata`` keys.
    """
    # Overwrite existing files and n_events
    section["files"] = {}

    if "metadata" not in section:
        section["metadata"] = {}
    section["metadata"]["nevents"] = 0
    section["metadata"]["nevents_total"] = 0

    response = cached.run_dasgoclient(f"file dataset={das_key}")

    # Parse dasgoclient results
    for entry in response:
        if len(entry["file"]) != 1:
            raise ValueError(f"More than one file for file object: {entry}")
        file = entry["file"][0]

        name = file["name"]
        # if is_vetoed(name):
        #     logger.critical(f"Skipping file due to entry in veto list: {file}")
        #     continue

        if "nevents" not in file or file["nevents"] == 0:
            logger.warning(
                f"Skipping file due to invalid number of events: {file['name']}"
            )
            continue

        section["metadata"]["nevents_total"] += file["nevents"]

        # Skip saving file and incrementing nevents if at cap
        if max_files is not None and len(section["files"].keys()) == max_files:
            continue

        # Save file, increment nevents
        section["files"][name] = "Events"
        section["metadata"]["nevents"] += file["nevents"]

    return section

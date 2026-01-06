"""
Utilities for loading definitions from dataset yaml files
"""

import logging
import yaml

logger = logging.getLogger("Dataset Definitions")


# Convert MC/Data to a proper dataset definition
def convert_section(section: list, is_data: bool):
    """
    Loads a monte-carlo or data definition section

    Args:
        section (list): A list of objects, each of which has a shortName and datasets key. Additional keys will persist as metadata.
        is_data (bool): The value to apply to the isData metadata key

    Returns:
        dict[str, dict]: A map from DAS key pattern to the associated metadata object
    """
    result = {}
    for entry in section:
        shortName = entry["shortName"]
        if "datasets" not in entry:
            logger.critical(
                f"No datasets available for section with shortName {shortName}"
            )
            continue

        # Copy metadata, remove datasets
        metadata = entry.copy()
        del metadata["datasets"]

        # Add data tag
        metadata["isData"] = is_data

        for dataset in entry["datasets"]:
            result[dataset] = metadata

    return result


# Convert a full year defined
# Convert a dict with two sections, data and monteCarlo
def convert_year(year: dict):
    """
    Loads a set of data and applies tags based on if they are in the data or monte carlo section

    Args:
        year (dict): A mapping of data/monteCarlo to a list of entries

    Returns
        dict: A mapping of DAS key patterns to metadata
    """
    return convert_section(year["data"], True) | convert_section(
        year["monteCarlo"], False
    )


def build_definitions(filename: str) -> dict:
    """
    Loads and builds custom dataset definitions from a given file

    Args:
        filename (str): Any file that can be opened with open()

    Returns:
        dict: A mapping of era to per-year definitions, each of which is a map from DAS key patterns to metadata
    """
    # Open file
    with open(filename, "r") as file:
        result = yaml.safe_load(file)

    # Convert each year
    for year, val in result.items():
        result[year] = convert_year(val)

    return result

"""
Utilities for skimmed datasets
"""

import logging
import os

logger = logging.getLogger("Skimmed Dataset Builder")


def escape_name(dataset: str) -> str:
    """
    Escapes a dataset's DAS key to a folder path name
    """
    safe_name = dataset.replace(os.path.sep, "_")
    if safe_name.startswith("_"):
        safe_name = safe_name[1:]
    return safe_name


def convert_to_skimmed(dataset: dict, skim_dir: str) -> dict:
    """
    Replaces a dataset's list of files with a set of skimmed files on the local disk

    Use with caution: skimmed datasets are not checked for accuracy! If dataset definitions or selection code has changed, skimming must be re-ran!

    Args:
        dataset (dict): A fully-rendered dataset with files and metadata
        skim_dir (str): A local directory to check for skims in

    Returns:
        dict: A fully-rendered dataset with skims replacing root files
    """

    result = {}

    merged_dir = os.path.abspath(os.path.join(skim_dir, "merged"))
    has_merged = os.path.isdir(merged_dir)
    if has_merged:
        logger.info("Using merged skim files!")

    # For each dataset
    for dataset_name, dataset_obj in dataset.items():
        logging.debug(f"Reading dataset {dataset_name} from disk")

        # Escape the name
        safe_name = escape_name(dataset_name)

        if has_merged:
            files = [os.path.join(merged_dir, f"{safe_name}.root")]
        else:
            base_path = os.path.join(skim_dir, safe_name)
            if not os.path.isdir(base_path):
                logger.critical(
                    f"Dataset {dataset_name} does not have skims, skipping... (directory does not exist: {base_path})"
                )
                continue

            files = [
                os.path.join(base_path, file)
                for file in os.listdir(base_path)
                if file.endswith(".root")
            ]
            if len(files) == 0:
                logger.critical(
                    f"Dataset {dataset_name} does not have skims, skipping... (directory has no root files: {base_path})"
                )
                continue

        logger.debug(f"Loaded dataset {dataset_name} ({len(files)} files)")

        files_dict = {}
        for file in files:
            files_dict[file] = "Events"

        result[dataset_name] = {
            "files": files_dict,
            "metadata": dataset_obj["metadata"],
        }

    return result

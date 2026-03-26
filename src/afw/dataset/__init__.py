from . import cached
from .definitions import build_datasets

import logging

import os
from ..utils import slugify


logger = logging.getLogger("dataset")


def apply_xrd_redirector(
    fileset: dict[str, dict[str, dict]], xrd_redirector: str
) -> dict[str, dict[str, dict]]:
    """
    Add the xrd_redirector to a dataset
    """
    result = {}
    for das_key, section in fileset.items():
        new_files = {}
        for file, tree in section["files"].items():
            new_files[xrd_redirector + file] = tree
        section["files"] = new_files
        result[das_key] = section

    return result


def print_summary(fileset: dict[str, list[str]], use_short_name: bool = True) -> None:
    """
    Prints a summary of a given dataset

    Args:
        fileset (dict): A fully-populated fileset, with files and metadata
        logger (logging.Logger): A Python logger to use for printing
        use_short_name (bool): Whether to use the shortName metadata attribute or the dataset's DAS key
    """

    logger.info("Printing Dataset")
    # Display in a table to look nice

    by_name = {}
    for dataset_name, dataset in fileset.items():
        key = dataset["metadata"]["shortName"] if use_short_name else dataset_name
        num = len(dataset["files"])
        by_name[key] = by_name.get(key, 0) + num

    maxlen = max([len(name) for name in by_name.keys()])
    if maxlen < len("Category"):
        maxlen = len("Category")

    total = sum(by_name.values())

    items = list(by_name.items())
    # items = sorted(items, key=lambda item: item[1])

    logger.info(f"{'Category'.ljust(maxlen)} | {total:,}")
    logger.info("-" * maxlen + "-+-" + "-" * 5)
    for name, num in items:
        logger.info(f"{name.ljust(maxlen)} | {num:,}")


def to_skimmed(dataset: dict, skim_dir: str) -> dict:
    """
    Replaces a dataset's list of files with a set of skimmed files on the local disk

    Use with caution: skimmed datasets are not checked for accuracy! If dataset definitions or selection code has changed, skimming must be re-ran!

    Args:
        dataset (dict): A fully-rendered dataset with files and metadata
        skim_dir (str): A local directory to check for skims in. This must be an absolute path as the dask client will run from ``$HOME``, not the current working directory

    Returns:
        dict: A fully-rendered dataset with skims replacing root files
    """

    result = {}

    merged_dir = os.path.join(skim_dir, "merged")
    has_merged = os.path.isdir(merged_dir)

    # For each dataset
    for dataset_name, dataset_obj in dataset.items():
        logging.debug(f"Reading dataset {dataset_name} from disk")

        # Escape the name
        dataset_name = slugify(dataset_name)

        if has_merged:
            files = [os.path.join(merged_dir, f"{dataset_name}.root")]
        else:
            base_path = os.path.join(skim_dir, dataset_name)
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


__all__ = [print_summary, cached, build_datasets, to_skimmed, apply_xrd_redirector]

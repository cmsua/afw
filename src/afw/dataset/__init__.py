from . import cached, definitions, skimmed

import logging


def apply_xcache_host(fileset: dict[str, dict[str, dict]], xcache_host: str) ->  dict[str, dict[str, dict]]:
    """
    Add the xcache_host to a dataset
    """
    result = {}
    for das_key, section in fileset.items():
        new_files = {}
        for file, tree in section["files"].items():
            new_files[xcache_host + file] = tree
        section["files"] = new_files
        result[das_key] = new_files
    
    return result


def print_summary(
    fileset: dict[str, list[str]], logger: logging.Logger, use_short_name: bool = True
) -> None:
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

__all__ = [print_summary, cached, definitions, skimmed]
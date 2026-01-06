import logging


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

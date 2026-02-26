import logging

import yaml

from .. import dataset
from ..objects import AnalysisConfig


def call(
    configs: list[AnalysisConfig],
    xrd_redirector: str,
    dataset_name: str,
    output_file: str,
    output_file_sorted: str,
    output_file_full: str,
    **kwargs: dict,
):
    """
    Call this subcommand from the CLI

    Args:
        configs (list[afw.objects.AnalysisConfig]): The configs to save (length 1 required)
        xrd_redirector (str): The input xrootd redirector
        dataset_name (str): The name of the dataset to keep
        output_file (str): The output file containing all remote files
        output_file_sorted (str): The output file containing all remote files, picked from each dataset in a round-robin order
        output_file_full (str): The yaml file containing all metadata
        **kwargs (dict): Any additional arguments
    """

    logger = logging.getLogger("Main")
    logger.info("Loaded Program and Arguments")

    config = configs[0]
    my_fileset = config.get_dataset(xrd_redirector)

    # Use dataset arg if needed
    if dataset_name is not None:
        for key in my_fileset:
            if key not in dataset_name:
                del my_fileset[key]

    dataset.print_summary(my_fileset, logger)

    files = [list(it["files"].keys()) for it in my_fileset.values()]
    files_all = sum(files, [])

    # Save Files
    with open(output_file, "w") as file:
        file.writelines([f"{line}\n" for line in files_all])

    # Save Files Sorted
    files_ord = []
    for i in range(max([len(it) for it in files])):
        for file_list in files:
            if i >= len(file_list):
                continue
            files_ord += [file_list[i]]
    with open(output_file_sorted, "w") as file:
        file.writelines([f"{line}\n" for line in files_ord])

    # Delete keys for preview
    for key in my_fileset:
        del my_fileset[key]["files"]
    # Debug
    with open(output_file_full, "w") as file:
        yaml.dump(my_fileset, file)

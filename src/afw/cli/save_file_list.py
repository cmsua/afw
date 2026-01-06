import logging

import yaml

from .. import dataset
from . import utils

if __name__ == "__main__":
    # Setup Args
    parser = utils.get_common_args()

    # Intermediates
    parser.add_argument(
        "-o",
        "--output_file",
        default="files.txt",
        help="Save all files to copy to this file",
    )
    parser.add_argument(
        "-s",
        "--output_file_sorted",
        default="files_sorted.txt",
        help="Save all files (sorted) to copy to this file",
    )
    parser.add_argument(
        "-O",
        "--output_file_full",
        default="dataset.yaml",
        help="Save a copy of the dataset to this file",
    )

    parser.add_argument(
        "-D",
        "--dataset",
        help="Save only a given dataset",
    )

    args = parser.parse_args()

    # Setup Logging
    utils.setup_logging(args.debug)

    logger = logging.getLogger("Main")
    logger.info("Loaded Program and Arguments")

    config = utils.get_configs(args.channel)[0]
    my_fileset = config.get_dataset(args.fileset_root)

    if args.dataset:
        my_fileset = {args.dataset: my_fileset[args.dataset]}

    dataset.print_summary(my_fileset, logger)

    files = [list(it["files"].keys()) for it in my_fileset.values()]
    files_all = sum(files, [])

    # Save Files
    with open(args.output_file, "w") as file:
        file.writelines([f"{line}\n" for line in files_all])

    # Save Files Sorted
    files_ord = []
    for i in range(max([len(it) for it in files])):
        for file_list in files:
            if i >= len(file_list):
                continue
            files_ord += [file_list[i]]
    with open(args.output_file_sorted, "w") as file:
        file.writelines([f"{line}\n" for line in files_ord])

    # Delete keys for preview
    for key in my_fileset:
        del my_fileset[key]["files"]
    # Debug
    with open(args.output_file_full, "w") as file:
        yaml.dump(my_fileset, file)

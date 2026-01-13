"""Command-line utility to merge skims using ``hadd``."""

import logging
import os
import subprocess

from ..objects import AnalysisConfig
from . import utils

## SOURCE: https://github.com/scikit-hep/coffea/discussions/1100


# Merge skims together
def merge_skims(config: AnalysisConfig, skim_dir: str) -> None:
    """Merge multi-part skims to one file using ``hadd``

    Parameters:
        config (objects.AnalysisConfig): The config to create skims for
        skim_dir (str): The output directory for skims
    """
    skim_dir_config = os.path.join(skim_dir, config.name)

    # List channels
    dirs = os.listdir(skim_dir_config)
    merged_dir = os.path.join(skim_dir_config, "merged")

    # Check for merged dir
    if "merged" in dirs:
        logger.critical(f"Directory {merged_dir} exists, skipping...")
        return

    # Make output dir
    os.makedirs(merged_dir, exist_ok=True)

    # Actually merge
    for fileset in dirs:
        # List dir, check for non-empty
        fileset_path = os.path.join(skim_dir_config, fileset)
        parts = [part for part in os.listdir(fileset_path) if part.endswith(".root")]
        if len(parts) == 0:
            logger.warning(
                f"Skipping dir {fileset} as it doesn't contain any root files!"
            )
            continue

        # Make target file
        target = os.path.join(merged_dir, f"{fileset}.root")

        # Run hadd
        command = ["hadd", target] + [
            os.path.join(fileset_path, part) for part in sorted(parts)
        ]

        logger.debug(f"Running command {command}")
        out = subprocess.run(command)
        if out.returncode != 0:
            logger.critical(f"hadd returned with non-zero return code {out}")


if __name__ == "__main__":
    # Setup Args
    parser = utils.get_common_args()
    args = parser.parse_args()

    # Setup Logging
    utils.setup_logging(args.debug)

    logger = logging.getLogger("Main")
    logger.info("Loaded program")

    # Create output dir
    skim_dir = os.path.expanduser(args.skim_dir)
    logger.info(f"Writing to skim dir {skim_dir}")

    # Run on channel(s)
    for config in utils.get_configs(args.config):
        logger.info(f"Handling config {config}")
        merge_skims(
            config,
            skim_dir,
        )

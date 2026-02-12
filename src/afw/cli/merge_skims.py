"""Command-line utility to merge skims using ``hadd``."""

import logging
import os
import subprocess

from ..objects import AnalysisConfig
from . import utils

logger = logging.getLogger("Skims (Merge)")
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

    # Create merge commands
    commands = []
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

        logger.Appending(f"Running command {command}")
        commands += [command]

    # Run Commands
    try:
        import joblib

        outs = joblib.Parallel(n_jobs=-2)(
            joblib.delayed(subprocess.run)(command) for command in commands
        )
    except ImportError:
        logger.warning("Joblib not found - Merging synchronously")

        outs = []
        for command in commands:
            outs += [subprocess.run(command)]

    for out in outs:
        if out.returncode != 0:
            logger.critical(f"hadd returned with non-zero return code {out.returncode}: {out}")


def call(
    configs: list[AnalysisConfig],
    skim_dir: str,
    **kwargs: dict,
):
    """
    Call this subcommand from the CLI

    Params:
        configs (list[afw.objects.AnalysisConfig]): The configs to skim
        skim_dir (str): The output directory (absolute path) to write to
        **kwargs (dict): Any additional arguments
    """

    # Run on channel(s)
    for config in configs:
        logger.info(f"Handling config {config}")
        merge_skims(
            config,
            skim_dir,
        )

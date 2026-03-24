"""Command-line utility to create run a given :class:`objects.AnalysisConfig`."""

import logging
import os
import pickle
import re
import unicodedata

from ..objects import AnalysisConfig

logger = logging.getLogger("Runtime")

# Stolen from https://github.com/django/django/blob/master/django/utils/text.py


def slugify(value):
    """
    Convert to ASCII. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    value = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    value = re.sub(r"[^\w\s-]", "", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_")


def generate_metadata(dataset: dict) -> dict[str, dict]:
    """
    Creates metadata for a dataset

    Args:
        dataset (dict): The dataset with files/etc

    Results:
        dict[str, dict]: A dictionary between ``dataset[dasKey][metadata][shortName]`` and ``dataset[dasKey][metadata]``
    """
    metadata = {}
    for key, val in dataset.items():
        name = val["metadata"]["shortName"]
        metadata[name] = val["metadata"]

    return metadata


def call(
    config: AnalysisConfig,
    debug: bool,
    extension: str,
    output_dir: str,
    **kwargs: dict,
):
    """
    Call this subcommand from the CLI

    Args:
        config (afw.objects.AnalysisConfig): The config to run over
        debug (bool): Whether to use debug mode (crash on bad file rather than skip)
        extension (str): The file extension to use
        output_dir (str): The directory to write plots to
        **kwargs (dict): Any additional arguments
    """
    # Make output
    output_dir = os.path.abspath(os.path.join(output_dir, config.name))
    os.makedirs(output_dir, exist_ok=True)

    # Get metadata
    metadata = generate_metadata(config.get_dataset())

    # Get target
    logger.debug("Selecting steps...")
    steps_dict = config.get_steps()
    final_step_name = config.get_default_step()
    final_step = steps_dict[final_step_name]

    logger.debug("Loading pickle...")
    accumulator_file = os.path.join(output_dir, f"{final_step_name}.pkl")
    with open(accumulator_file, "rb") as file:
        accumulator = pickle.load(file)

    logger.debug("Plotting...")
    plots = accumulator["plots"]
    plot_dir = os.path.join(output_dir, "plots")
    os.makedirs(plot_dir, exist_ok=True)

    for plottable in final_step.plottables:
        output_file = os.path.join(plot_dir, f"{slugify(plottable.label)}.{extension}")
        plottable.plot_histogram(
            histogram=plots[plottable.label],
            metadata=metadata,
            title=plottable.label,
            output_file=output_file,
        )

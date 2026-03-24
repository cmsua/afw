"""Plotting utilities"""

import logging
import os
import pickle


from ..objects import AnalysisConfig, Plottable

logger = logging.getLogger("Main")


def generate_metadata(dataset: dict) -> dict[str, dict]:
    """
    Creates metadata for a dataset

    Args:
        dataset (dict): The dataset with files/etc

    Results:
        dict[str, dict]: A dictionary with keys being shortNames
    """
    metadata = {}
    for key, val in dataset.items():
        name = val["metadata"]["shortName"]
        metadata[name] = {**metadata.get(name, {}), **val["metadata"]}

    return metadata


def save_results(
    output_dir: str,
    extension: str,
    title: str,
    things: list[Plottable],
    metadata: dict,
    data: dict,
) -> None:
    """
    Save plots to a file

    Args:
        output_dir (str): the directory to save plots to (including the config name)
        extension (str): The file extension to use when saving plots
        title (str): The title to use on all things
        things (list[Plottable]): All objects used to save plots
        metadata (dict): The metadata for plotting
        data (dict): The object containing histograms
    """
    os.makedirs(output_dir, exist_ok=True)

    # Actually plot
    # Try running with joblib
    try:
        import joblib

        joblib.Parallel(n_jobs=-2)(
            joblib.delayed(thing.plot_histogram)(
                data[thing.label],
                metadata,
                title,
                os.path.join(output_dir, f"{thing.escaped_name}.{extension}"),
            )
            for thing in things
        )
    except ImportError:
        logger.warning("Joblib not found - plotting synchronously")

        for thing in things:
            thing.plot_histogram(
                data[thing.label],
                metadata,
                title,
                os.path.join(output_dir, f"{thing.escaped_name}.{extension}"),
            )

def call(
    config: AnalysisConfig,
    output_dir: str,
    extension: str,
    **kwargs: dict,
):
    """
    Call this subcommand from the CLI

    Args:
        configs (list[afw.objects.AnalysisConfig]): The configs to skim
        output_dir (str): The directory to write plots to
        extension (str): The file extension to use for plots
        **kwargs (dict): Any additional arguments
    """
    # Run on channel(s)
    output_dir = os.path.join(output_dir, config.name)
    with open(os.path.join(output_dir, "results.pkl"), "rb") as file:
        results = pickle.load(file)

    metadata = generate_metadata(config.get_dataset(None))
    save_results(
        output_dir,
        extension,
        config.name,
        config.get_things_to_plot(),
        metadata,
        results,
    )
"""Plotting utilities"""
import logging
import os
import pickle

from . import utils
from ..objects import ThingToPlot


def save_results(
    output_dir: str, extension: str, things: list[ThingToPlot], data: dict
) -> None:
    """
    Save plots to a file
    
    Params:
        output_dir (str): the directory to save plots to (including the config name)
        extension (str): The file extension to use when saving plots
        things (list[ThingToPlot]): All objects used to save plots
        data (dict): The object containing histograms
    """
    # Actually plot
    # Try running with joblib
    try:
        import joblib

        joblib.Parallel(n_jobs=-2)(
            joblib.delayed(thing.plot_histogram)(
                data[thing.title],
                os.path.join(output_dir, f"{thing.escaped_name}.{extension}"),
            )
            for thing in things
        )
    except ImportError:
        logger.warning("Joblib not found - plotting synchronously")

        for thing in things:
            thing.plot_histogram(
                data[thing.title],
                os.path.join(output_dir, f"{thing.escaped_name}.{extension}"),
            )


if __name__ == "__main__":
    # Setup Args
    parser = utils.get_common_args()

    # Intermediates
    parser.add_argument(
        "-o", "--output_dir", default="plots", help="Directory in which to save plots"
    )
    parser.add_argument(
        "-e", "--extension", default="png", help="Format in which to save plots"
    )

    args = parser.parse_args()

    # Setup Logging
    utils.setup_logging(args.debug)

    logger = logging.getLogger("Main")
    logger.info("Loaded Program and Arguments")

    # Get Dask Client
    logger.info("Creating & Saving Plots")

    # Run on channel(s)
    for config in utils.get_configs(args.channel):
        output_dir = os.path.join(args.output_dir, config.name)
        with open(os.path.join(output_dir, "results.pkl"), "rb") as file:
            results = pickle.load(file)

        save_results(
            output_dir, args.extension, config.get_things_to_plot(), results
        )

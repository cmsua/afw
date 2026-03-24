import argparse

import os
import logging


from . import plotter, runner, utils


def with_debug_and_config(parser: argparse.ArgumentParser):
    """
    Add the debug and config parameters to a parser

    Args:
        parser (argparse.ArgumentParser): The parser to modify
    """
    parser.add_argument("config", help="The config file to load", type=str)

    # Debug
    parser.add_argument(
        "-d",
        "--debug",
        default=False,
        action="store_true",
        help="Enable verbose logging",
    )


# Common arguments
def with_skim_dir(parser: argparse.ArgumentParser, required: bool = False):
    """
    Add the skim directory to an argument parser

    Args:
        parser (argparse.ArgumentParser): The parser to modify
        required (bool, default False): Whether the argument is required
    """

    parser.add_argument(
        "-S",
        "--skim_dir",
        help="Base path for reading names of data/mc files",
        type=os.path.expanduser,
        default="skims/",
        required=required,
    )


def with_xcache_redirector(parser: argparse.ArgumentParser, required: bool = False):
    """
    Add the xcache redirector to an argument parser. Defaults to the CMS Global Redirector if XCache cannot be detected.

    Args:
        parser (argparse.ArgumentParser): The parser to modify
        required (bool, default False): Whether the argument is required
    """
    parser.add_argument(
        "-x",
        "--xrd_redirector",
        help="XRootD Redirector for all data/mc files",
        default="root://" + os.environ.get("XCACHE_HOST", "cms-xrd-global.cern.ch") + "/",
        required=required,
    )


def with_output_dir(parser: argparse.ArgumentParser):
    """
    Add an output directory to an argument parser. Defaults to the CMS Global Redirector if XCache cannot be detected.

    Args:
        parser (argparse.ArgumentParser): The parser to modify
        required (bool, default True): Whether the argument is required
    """
    # Intermediates
    parser.add_argument(
        "-o",
        "--output_dir",
        type=os.path.expanduser,
        default="plots",
        help="Directory in which to save plots",
    )


def with_dask_client(parser: argparse.ArgumentParser):
    """
    Add a Dask client description to an argument parser.

    Args:
        parser (argparse.ArgumentParser): The parser to modify
    """

    # Environment settings
    parser.add_argument(
        "-C",
        "--cluster-address",
        help="Cluster to use for processing",
        default=os.environ.get("CLUSTER_ADDRESS", "tls://localhost:8786"),
    )


def with_extension(parser: argparse.ArgumentParser):
    """
    Add a plot file extension to an argument parser.

    Args:
        parser (argparse.ArgumentParser): The parser to modify
    """

    parser.add_argument(
        "-e", "--extension", default="png", help="Format in which to save plots"
    )


def run():
    """
    Entrypoint for the CLI
    """

    # Setup Args
    parser = argparse.ArgumentParser("Analysis FrameWork (UA)")

    # RUNTIME
    subparsers = parser.add_subparsers(help="Runtime Tools")

    # Run
    run_parser = subparsers.add_parser("run", help="Run the analysis and plot results")
    with_skim_dir(run_parser, required=False)
    with_xcache_redirector(run_parser, required=False)
    with_dask_client(run_parser)
    with_output_dir(run_parser)
    with_extension(run_parser)
    with_debug_and_config(run_parser)
    run_parser.add_argument(
        "-n",
        "--n-files",
        type=int,
        help="The number of input files allowed in each fileset",
    )
    run_parser.add_argument(
        "-c",
        "--chunksize",
        type=int,
        default=500_000,
        help="The chunk size to use when processing data"
    )
    run_parser.set_defaults(func=runner.call)

    # Plot
    plot_parser = subparsers.add_parser(
        "plot", help="Re-generate plots from an already-ran analysis"
    )
    with_output_dir(plot_parser)
    with_extension(plot_parser)
    with_debug_and_config(plot_parser)
    plot_parser.set_defaults(func=plotter.call)

    # Run
    args = parser.parse_args()

    if "func" not in args:
        logging.critical("Invalid function!")
        return

    # Logging
    utils.setup_logging(args.debug)

    # Dask Client
    # Do this before overriding the config
    if "cluster_address" in args:
        args.client = utils.create_dask_client(args.cluster_address, [args.config])

    # Select config
    configs = utils.get_configs(args.config)
    if len(configs) != 1:
        logging.critical("File should only specify one config!")
        return
    args.config = configs[0]

    try:
        func = args.func
        func(**vars(args))
    finally:
        if "client" in args:
            args.client.close()

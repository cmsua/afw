import argparse

import dotenv
import os
import logging


from . import merge_skims, plotter, runner, save_file_list, skim, utils


def with_debug_and_config(parser: argparse.ArgumentParser):
    """
    Add the debug and config parameters to a parser

    Params:
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

    Params:
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

    Params:
        parser (argparse.ArgumentParser): The parser to modify
        required (bool, default False): Whether the argument is required
    """
    parser.add_argument(
        "-x",
        "--xrd_redirector",
        help="XRootD Redirector for all data/mc files",
        default=os.environ.get("XCACHE_HOST", "root://cms-xrd-global.cern.ch/"),
        required=required,
    )


def with_output_dir(parser: argparse.ArgumentParser):
    """
    Add an output directory to an argument parser. Defaults to the CMS Global Redirector if XCache cannot be detected.

    Params:
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

    Params:
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

    Params:
        parser (argparse.ArgumentParser): The parser to modify
    """

    parser.add_argument(
        "-e", "--extension", default="png", help="Format in which to save plots"
    )


def run():
    """
    Entrypoint for the CLI
    """
    dotenv.load_dotenv()

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
    run_parser.set_defaults(func=runner.call)

    # Plot
    plot_parser = subparsers.add_parser(
        "plot", help="Re-generate plots from an already-ran analysis"
    )
    with_output_dir(plot_parser)
    with_extension(plot_parser)
    with_debug_and_config(plot_parser)
    plot_parser.set_defaults(func=plotter.call)

    # Plot Difference
    plot_diff_parser = subparsers.add_parser(
        "plot_difference", help="Subtract two results and save the outcome plot"
    )
    with_output_dir(plot_diff_parser)
    with_extension(plot_diff_parser)
    with_debug_and_config(plot_diff_parser)
    plot_diff_parser.add_argument("-i", "--input_dir_one", type=str, help="Input Directory 1")
    plot_diff_parser.add_argument("-I", "--input_dir_two", type=str, help="Input Directory 2")
    plot_diff_parser.set_defaults(func=plotter.call_subtr)

    # File List
    save_file_list_parser = subparsers.add_parser(
        "save_file_list",
        help="Save the file list for use in populating xcache or inspecting dataset metadata",
    )
    save_file_list_parser.add_argument(
        "-o",
        "--output_file",
        default="files.txt",
        help="Save all files to copy to this file",
    )
    save_file_list_parser.add_argument(
        "-s",
        "--output_file_sorted",
        default="files_sorted.txt",
        help="Save all files (sorted) to copy to this file",
    )
    save_file_list_parser.add_argument(
        "-O",
        "--output_file_full",
        default="dataset.yaml",
        help="Save a copy of the dataset to this file",
    )
    save_file_list_parser.add_argument(
        "-D", "--dataset", help="Save only a given dataset", type=list
    )
    with_debug_and_config(save_file_list_parser)
    save_file_list_parser.set_defaults(func=save_file_list.call)

    # SKIMS
    # Skim
    skim_parser = subparsers.add_parser(
        "skim", help="Run the analysis save skims with pre-selected events"
    )
    skim_parser.add_argument(
        "-p",
        "--parallel",
        action="store_true",
        help="Compute each dataset in parallel rather than in series",
    )
    skim_parser.add_argument(
        "-n",
        "--n-to-one",
        default=15,
        type=int,
        help="The number of input files to one output file",
    )
    with_debug_and_config(skim_parser)
    skim_parser.set_defaults(func=skim.call)

    # Merge Skims
    merge_skim_parser = subparsers.add_parser(
        "merge_skims", help="Merge skims into single files"
    )
    with_debug_and_config(merge_skim_parser)
    merge_skim_parser.set_defaults(func=merge_skims.call)

    # Run
    args = parser.parse_args()

    if "func" not in args:
        logging.critical("Invalid function!")
        return

    # Logging
    utils.setup_logging(args.debug)

    # Dask Client
    if "cluster_address" in args:
        args.client = utils.create_dask_client(args.cluster_address, [args.config])

    try:
        func = args.func
        args.configs = utils.get_configs(args.config)

        func(**vars(args))
    finally:
        if "client" in args:
            args.client.close()

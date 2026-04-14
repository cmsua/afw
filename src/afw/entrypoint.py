import argparse
import importlib
import logging
import os
import sys

import dask
import tqdm
from dask.distributed import Client

from .utils_internal import setup_logging

logger = logging.getLogger("entrypoint")


def run():
    """
    Entrypoint for the CLI
    """

    # Setup Args
    parser = argparse.ArgumentParser("Analysis FrameWork (UA)")

    # RUNTIME
    parser.add_argument("config", help="The config file to load", type=str)
    parser.add_argument(
        "option", help="The option to run in the given config", type=str
    )

    # Debug
    parser.add_argument(
        "-d",
        "--debug",
        default=False,
        action="store_true",
        help="Enable verbose logging",
    )

    # Filesystems
    parser.add_argument(
        "-S",
        "--skim_dir",
        help="Base path for reading names of data/mc files",
        type=os.path.expanduser,
        default="skims/",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=os.path.expanduser,
        default="output",
        help="Directory in which to save output accumulators/plots",
    )
    parser.add_argument(
        "-x",
        "--xrd_redirector",
        help="XRootD Redirector for all data/mc files",
        default="root://"
        + os.environ.get("XCACHE_HOST", "cms-xrd-global.cern.ch")
        + "/",
    )

    # Output
    parser.add_argument(
        "-e", "--extension", default="png", help="Format in which to save plots"
    )

    # Cluster
    parser.add_argument(
        "-C",
        "--cluster-address",
        help="Cluster to use for processing",
        default=os.environ.get("CLUSTER_ADDRESS", None),
    )
    parser.add_argument(
        "-n",
        "--n-files",
        type=int,
        help="Limit the runner to a given number of files per DAS key",
    )
    parser.add_argument(
        "-s",
        "--skip-bad-files",
        default=False,
        action="store_true",
        help="Whether to skip bad files rather than throwing an error",
    )
    parser.add_argument(
        "-c",
        "--chunk-size",
        default=100_000,
        type=int,
        help="The number of events to process per chunk",
    )

    # Run
    args = parser.parse_args()

    # Logging
    setup_logging(args.debug)

    # Select config
    config = get_config(args.config)

    # Fix paths for this specific config
    args.skim_dir = os.path.abspath(args.skim_dir)
    args.output_dir = os.path.abspath(args.output_dir)
    
    # Select option
    args_dict = vars(args)
    options = config.get_options(**args_dict)

    # Check option is valid
    if args.option not in options:
        logger.critical(f"Option {args.option} not found in config! Valid options:")
        for option in options:
            logger.critical(f"- {option}")
        return

    stages = options[args.option]

    # Run said option
    # Dask Client
    # Do this before overriding the config
    args.client = create_dask_client(args.cluster_address, [args.config])

    logger.debug(f"Using args {args_dict}")

    try:
        with tqdm.tqdm(stages) as pbar:
            for stage in pbar:
                # Logging
                pbar.set_description(stage.name)
                stage.run(**args_dict)
    finally:
        # Close client
        if "client" in args:
            args.client.close()


## Dask Cluster Util
# Returns Client, Cluster
def create_dask_client(
    cluster_address_override: str | None = None, upload_files: list[str] = []
):
    """
    Creates a Dask client and optionally uploads required Python code.

    If a cluster is not specified, this function will fall back to ``labextension.factory.module`` if set, or ``LocalCluster`` otherwise.

    The following files will be uploaded if set: configs/\*.py, processor.py

    Parameters:
        cluster_address_override (str): One of the above supported clients
        upload_files (list[str], default []):  Local python files to upload to the Dask client

    Returns:
        dask.distributed.Client: A Dask client
    """
    module = dask.config.get("labextension.factory.module", None)
    initial = dask.config.get("labextension.factory.initial", None)

    # Gateway override or gateway config'd and no override
    if cluster_address_override == "gateway" or (
        module == "dask_gateway" and cluster_address_override is None
    ):
        upload = False

        logger.debug("Connecting to gateway")
        from dask_gateway import Gateway

        gateway = Gateway()
        clusters = gateway.list_clusters()
        if len(clusters) == 0:
            raise ValueError("No cluster exists in the gateway!")

        logger.debug("Fetching cluster {cluters[0].name}")
        cluster = gateway.connect(clusters[0].name)
        client = Client(cluster, timeout=60)

    # Other overrides
    elif cluster_address_override is not None:
        # Local override
        if cluster_address_override == "local":
            upload = False
            from dask.distributed import LocalCluster

            cluster = LocalCluster()
            client = cluster.get_client()
        # TLS override
        elif "tls" in cluster_address_override:
            upload = True

            logger.debug(
                f"Connecting to cluster due to tls override at {cluster_address_override}"
            )
            client = Client(cluster_address_override)
        else:
            raise ValueError(f"Invalid override {cluster_address_override}")
    # Cluster is present
    elif initial is not None:
        upload = True

        logger.debug("Connecting to presumed present cluster at tls://localhost:8787/")
        client = Client("tls://localhost:8787/")
    else:
        raise ValueError("Could not find cluster!")

    # Upload Files
    if upload:
        logger.debug("Uploading files to workers...")
        for file in upload_files:
            logger.debug(f"Uploading file {file}")
            client.upload_file(file)
    else:
        logger.warning("Skipping upload files to workers")

    logger.info(f"Dashboard located at {client.dashboard_link}")
    return client


# Get config from ee, emu, mumu, or common (common is only used for skimming)
def get_config(file_path: str):
    """
    Returns an analysis config from a given name. The file will be imported as the given module name.

    Parameters:
        file_path (str): The path to a python module

    Returns:
        list[objects.AnalysisConfig]: All AnalysisConfigs in said module
    """
    # Code taken form importlib docs
    module_name = os.path.basename(file_path).replace(".py", "")
    spec = importlib.util.spec_from_file_location("config", file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

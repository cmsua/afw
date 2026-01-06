"""
Utilities for CLI tools
"""

import argparse
import logging
import os
import importlib

from dask.distributed import Client

from .objects import AnalysisConfig

logger = logging.getLogger("utils")


# Dask Cluster Util
# Returns Client, Cluster
def create_dask_client(cluster_address: str, upload: bool = True):
    """
    Creates a Dask client and optionally uploads required Python code.

    Supported clients:
    - 'local': Spawns a dask.distributed.LocalCluster. This cluster does not support file upload.
    - 'gateway': Connects to a dask_gateway.GatewayCluster if present (create using the Dask LabExtension interface)
    - A Dask scheduler located at tcp://your-ip-here:port, such as included in coffea.casa or SWAN

    The following files will be uploaded if set: configs/\*.py, processor.py

    Parameters:
        cluster_address(str): One of the above supported clients
        upload (bool, default True): Whether to upload local files to the cluster

    Returns:
        dask.distributed.Client: A Dask client
    """
    logger.info("Loading Dask client")
    if cluster_address == "local":
        from dask.distributed import LocalCluster

        cluster = LocalCluster()
        client = cluster.get_client()
        upload = False

    elif cluster_address == "gateway":
        logger.debug("Connecting to gateway")
        from dask_gateway import Gateway

        gateway = Gateway()
        clusters = gateway.list_clusters()
        if len(clusters) == 0:
            raise ValueError("No cluster exists in the gateway!")

        logger.debug("Fetching cluster {cluters[0].name}")
        cluster = gateway.connect(clusters[0].name)
        client = Client(cluster, timeout=60)
    else:
        logger.debug(f"Connecting to cluster at {cluster_address}")
        client = Client(cluster_address)

    if upload:
        # Upload Files
        logger.debug("Uploading files to workers...")
        files = [file for file in os.listdir() if file.endswith(".py")]
        for file in files:
            logger.debug(f"Uploading file {file}")
            client.upload_file(file)

    else:
        logger.warning("Skipping upload files to workers")

    logger.info(f"Dashboard located at {client.dashboard_link}")
    return client


# Root Host
def get_xrd_redirector():
    """
    Returns an xcache redirector. Defaults to the CMS Global Redirector if XCache cannot be detected.

    Returns:
        str: The local XCache redirector, or the CMS Global Redirector if not present
    """
    if "XCACHE_HOST" not in os.environ:
        return "root://cms-xrd-global.cern.ch/"
    return f"root://{os.environ['XCACHE_HOST']}/"


# Common Args
def get_common_args():
    """
    Load common arguments for CLI programs. Currently supports:
    - channel selection
    - skim directory specification
    - fileset root for non-skimmed files
    - debug mode
    """
    if os.path.exists(".env"):
        import dotenv

        dotenv.load_dotenv()

    # Setup Args
    parser = argparse.ArgumentParser("Analysis FrameWork (UA)")

    # Analysis settings
    parser.add_argument("-c", "--config", help="The config file to load", type=str)
    parser.add_argument(
        "-S",
        "--skim_dir",
        help="Base path for reading names of data/mc files",
        default=os.environ.get("SKIM_LOCATION", "skims"),
    )

    # Environment settings
    parser.add_argument(
        "-C",
        "--cluster-address",
        help="Cluster to use for processing",
        default=os.environ.get("CLUSTER_ADDRESS", "tls://localhost:8786"),
    )
    parser.add_argument(
        "-x",
        "--xrd_redirector",
        help="XRootD Redirector for all data/mc files",
        default=get_xrd_redirector(),
    )

    # Debug
    parser.add_argument(
        "-d",
        "--debug",
        default=False,
        action="store_true",
        help="Enable verbose logging",
    )
    return parser


# Get config from ee, emu, mumu, or common (common is only used for skimming)
def get_configs(name: str) -> list[AnalysisConfig]:
    """
    Returns an analysis config from a given name

    Parameters:
        name (str): The path to a python module

    Returns:
        list[objects.AnalysisConfig]: All AnalysisConfigs in said module
    """
    obj = importlib.import_module(name)
    return [i for i in obj if isinstance(i, AnalysisConfig)]


# LOGGING
class Formatter(logging.Formatter):
    """
    A formatter for output logging
    """

    grey = "\x1b[37m"
    yellow = "\x1b[33m"
    red = "\x1b[31m"
    bold_red = "\x1b[1;31m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)-10s - %(levelname)-7s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: logging.Formatter(grey + format + reset),
        logging.INFO: logging.Formatter(format),
        logging.WARNING: logging.Formatter(yellow + format + reset),
        logging.ERROR: logging.Formatter(red + format + reset),
        logging.CRITICAL: logging.Formatter(bold_red + format + reset),
    }

    def format(self, record):
        return self.FORMATS.get(record.levelno).format(record)


def setup_logging(debug: bool = False):
    """
    Sets up a custom formatter for output logs

    Parameters:
        debug (bool, default False): Whether to set a logging level of logging.DEBUG
    """
    ch = logging.StreamHandler()
    ch.setFormatter(Formatter())

    logging.basicConfig(
        handlers=[ch],
        level=logging.DEBUG if debug else logging.INFO,
    )

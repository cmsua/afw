"""
Utilities for CLI tools
"""
import importlib.util
import logging
import os
import sys
import awkward as ak
import dask_awkward as dak
from dask.distributed import Client

from ..objects import AnalysisConfig, AnalysisStep

logger = logging.getLogger("utils")

## Dependencies
def solve_dependency_chain(requested_step: str, steps: dict[str, AnalysisStep]):
    """
    Solves a dependency chain for a given step

    Args:
        requested_step (str): The name of the step to request
        steps (dict[str, AnalysisStep]): A dictionary of steps to results
    """
    logger.debug(f"Solving dependency chain for {requested_step}")

    # Build list of all steps
    queue = [requested_step]
    all_steps = []
    while len(queue) != 0:
        # Add to order
        current_step_name = queue.pop(0)
        all_steps += [current_step_name]

        # Add next + dependencies
        for dependency in steps[current_step_name].dependencies:
            if dependency not in all_steps and dependency not in queue:
                logger.debug(f"Adding dependency {dependency} from {current_step_name}")
                queue += [dependency]

    logger.debug(f"Found final list of all steps required to process {requested_step}: {all_steps}")

    # Run in reverse - do as much as possible with currently present dependencies, and repeat till every step is done
    # List of list of steps - each sublist will run in series, with their components in parallel
    all_chains = []
    current_chain = []
    # List of all steps which have been accumulated
    processed_steps = []
    accumulated_steps = []

    # First load cached steps
    for step_name in list(all_steps):
        step = steps[step_name]
        # TODO load cache

    # Do as much as possible with currently present dependencies, and repeat till every step is done
    while len(all_steps) != 0:
        logger.debug(f"Running solver iteration! Current chain: {current_chain}, total chain: {all_chains}")
        # Check dependencies
        current_step = []
        current_step_modifies = []

        # Find what steps we can do at the moment
        for step_name in list(all_steps):
            step = steps[step_name]
            # Only add ones for which we satisfy all present requirements
            if any([it not in processed_steps for it in steps[step_name].dependencies]):
                logger.debug(f"Skipping {step_name} as dependency not in {processed_steps}")
                continue

            if step.modifies_events:
                current_step_modifies += [step_name]
            else:
                current_step += [step_name]

        logger.debug(f"Can run non-modifying {current_step}")
        logger.debug(f"Can run modifying {current_step_modifies}")

        # Select what we add to the order
        steps_to_add = None

        # Add non-modifying if we can
        if len(current_step) != 0:
            logger.debug(f"Adding non-modifying steps {current_step}")
            steps_to_add = current_step
        # Add the one modifying
        elif len(current_step_modifies) == 1:
            logger.debug(f"Adding event modifying steps {current_step_modifies}")
            steps_to_add = current_step_modifies
        elif len(current_step_modifies) > 0:
            raise ValueError("Trying to require two event-modifying objects, failing!")
        else:
            raise ValueError("Infinite dependency chain!")
        
        # Add all steps - check if any requires a dependency that must first be accumulated
        required_split = False
        for step_name in steps_to_add:
            all_steps.remove(step_name)

            # For each dependency, make sure it's accumulated if needed
            step = steps[step_name]
            for dependency_name in step.dependencies:
                dependency = steps[dependency_name]
                # If it requires a split and one hasn't been done
                if not dependency.split_required:
                    continue
                if dependency_name not in accumulated_steps:
                    required_split = True
                    break

        # If we need a split, split the current chain off and start a new one
        if required_split:
            all_chains += [current_chain]
            current_chain = []
            accumulated_steps = processed_steps

        # Add the actual steps to current chain
        processed_steps += steps_to_add
        current_chain += steps_to_add

    # Add last chain        
    all_chains += [current_chain]

    logger.debug(f"Returning final list of chains {all_chains}")
    return all_chains

## For Skimming
def is_rootcompat(a: ak.Array) -> bool:
    """Returns whether an array can be written to a Root file

    Parameters:
        a (ak.Array): Any Awkward Array

    returns:
        bool: Whether the parameter is a flat or 1d jagged array
    """
    t = dak.type(a)
    if isinstance(t, ak.types.NumpyType):
        return True
    if isinstance(t, ak.types.ListType) and isinstance(t.content, ak.types.NumpyType):
        return True

    return False


def uproot_writeable(events: ak.Array) -> ak.Array:
    """Restrict to columns that uproot can write compactly

    Parameters:
        events (ak.Array): Any given Awkward Array

    Returns:
        ak.Array: An Awkward Array without any incompatible fields
    """
    out_event = events[list(x for x in events.fields if not events[x].fields)]
    for bname in events.fields:
        if events[bname].fields:
            out_event[bname] = ak.zip(
                {
                    n: ak.without_parameters(events[bname][n])
                    for n in events[bname].fields
                    if is_rootcompat(events[bname][n])
                }
            )
    return out_event

## Dask Cluster Util
# Returns Client, Cluster
def create_dask_client(cluster_address: str, upload_files: list[str] = []):
    """
    Creates a Dask client and optionally uploads required Python code.

    Supported clients:
    - 'local': Spawns a dask.distributed.LocalCluster. This cluster does not support file upload.
    - 'gateway': Connects to a dask_gateway.GatewayCluster if present (create using the Dask LabExtension interface)
    - A Dask scheduler located at tcp://your-ip-here:port, such as included in coffea.casa or SWAN

    The following files will be uploaded if set: configs/\*.py, processor.py

    Parameters:
        cluster_address(str): One of the above supported clients
        upload_files (list[str], default []):  Local python files to upload to the Dask client

    Returns:
        dask.distributed.Client: A Dask client
    """
    logger.info("Loading Dask client")
    if cluster_address == "local":
        upload = False

        from dask.distributed import LocalCluster

        cluster = LocalCluster()
        client = cluster.get_client()
    elif cluster_address == "gateway":
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
    else:
        upload = True

        logger.debug(f"Connecting to cluster at {cluster_address}")
        client = Client(cluster_address)

    if upload:
        # Upload Files
        logger.debug("Uploading files to workers...")
        for file in upload_files:
            logger.debug(f"Uploading file {file}")
            client.upload_file(file)

    else:
        logger.warning("Skipping upload files to workers")

    logger.info(f"Dashboard located at {client.dashboard_link}")
    return client



# Get config from ee, emu, mumu, or common (common is only used for skimming)
def get_configs(file_path: str) -> list[AnalysisConfig]:
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

    # Inspect module
    return [cls() for cls in module.__all__]
    

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

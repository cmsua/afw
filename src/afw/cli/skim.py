"""Command-line utility to create skims for a given :class:`objects.AnalysisConfig`."""

import logging
import os

import awkward as ak
import dask
import dask_awkward as dak
import uproot
from coffea.dataset_tools import apply_to_fileset, preprocess
from coffea.nanoevents import NanoAODSchema
from dask.distributed import Client

from ..dataset import apply_xcache_host, print_summary, skimmed
from ..objects import AnalysisConfig

## SOURCE: https://github.com/scikit-hep/coffea/discussions/1100

logger = logging.getLogger("Main")
logger.info("Loaded program")


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


def handle_config(
    config: AnalysisConfig,
    xrd_redirector: str,
    skim_dir: str,
    run_combined: bool = False,
    skip_bad_files: bool = False,
    n_to_one: int = 15,
) -> None:
    """Create and save skims for a given :class:`objects.AnalysisConfig`

    Parameters:
        config (objects.AnalysisConfig): The config to create skims for
        xrd_redirector (str): The host of the XRootD Redirector to use
        skim_dir (str): The output directory for skims
        run_combined (bool, default False): Whether to submit preprocessing and skimming to the Dask Client as one compute or to run each dataset in series
        skip_bad_files (bool, default False): Whether or not to skip bad files in the dataset
        n_to_one (int, default 15): If non-negative, the n_to_one value to use when repartitioning
    """
    # Load dataset, with preskims if needed
    my_dataset = config.get_dataset()
    my_dataset = apply_xcache_host(my_dataset, xrd_redirector)

    # Print
    print_summary(my_dataset, logger, use_short_name=False)

    # Check for directories to run on
    for dataset_name in list(my_dataset.keys()):
        dataset_dir = os.path.join(
            skim_dir, config.name, skimmed.escape_name(dataset_name)
        )
        # Only run on existing directories
        if not os.path.isdir(dataset_dir):
            continue

        # Check for root files - if there are none, we can run on this dataset
        root_files = [
            file for file in os.listdir(dataset_dir) if file.endswith(".root")
        ]
        if len(root_files) == 0:
            logger.warning(f"Empty output directory, continuing: {dataset_dir}")
        else:
            logger.critical(
                f"Output directory already exists, skipping: {dataset_dir})"
            )
            del my_dataset[dataset_name]

    ## Setup for running
    # Create skimmed events from events
    def skim(events):
        # Only define objects if this is the first skim
        events = config.define_objects(events)
        events = config.preselect_events(events)
        return config.minify(events)

    # Preprocess Params
    preprocess_params = {
        "align_clusters": False,
        "step_size": 100_000,  # You may want to set this to something slightly smaller to avoid loading too much in memory
        "files_per_batch": 1,
        "skip_bad_files": skip_bad_files,
        "save_form": False,
    }

    # Preprocess in bulk if needed
    if run_combined:
        logger.info("Preprocessing filesets")
        dataset_runnable, _ = preprocess(my_dataset, **preprocess_params)

        logger.info("Computing Task Graph")
        skimmed_dict = apply_to_fileset(
            skim, dataset_runnable, schemaclass=NanoAODSchema
        )

    # Run
    to_run = []
    for fileset_name, fileset in my_dataset.items():
        logger.info(f"Handling fileset {fileset_name}")
        if not run_combined:
            logger.debug(f"Preprocessing fileset {fileset_name}")
            dataset_runnable, _ = preprocess(
                {fileset_name: fileset}, **preprocess_params
            )

            logger.debug(f"Computing Task Graph for {fileset_name}")
            skimmed_dict = apply_to_fileset(
                skim, dataset_runnable, schemaclass=NanoAODSchema
            )

        skimmed_writable = uproot_writeable(skimmed_dict[fileset_name])
        if n_to_one > 0:
            skimmed_writable = skimmed_writable.repartition(
                n_to_one=n_to_one,
            )  # Reparititioning so that output file contains ~100_000 eventspartition

        # Output directory
        destination = os.path.join(
            skim_dir, config.name, skimmed.escape_name(fileset_name)
        )

        # Return so that compute can be called
        logger.debug("Writing...")
        result = uproot.dask_write(
            skimmed_writable,
            compute=not run_combined,
            tree_name="Events",
            destination=destination,
        )

        to_run += [result]
    if run_combined:
        logger.info("Computing all...")
        dask.compute(*to_run)


def call(
    client: Client,
    configs: list[AnalysisConfig],
    skim_dir: str,
    xrd_redirector: str,
    debug: bool,
    parallel: bool,
    n_to_one: int,
    **kwargs: dict,
):
    """
    Call this subcommand from the CLI

    Args:
        client (dask.distributed.Client): The dask client to use
        configs (list[afw.objects.AnalysisConfig]): The configs to skim
        skim_dir (str): The output directory (absolute path) to write to
        xrd_redirector (str): The input xrootd redirector
        debug (bool): Whether to use debug mode (crash on bad file rather than skip)
        parallel (bool): Whether to run each dataset in parallel
        n_to_one (int): The number of files to be combined into one
        **kwargs (dict): Any additional arguments
    """
    # Run on channel(s)
    for config in configs:
        logger.info(f"Handling config {config}")
        handle_config(
            config,
            xrd_redirector,
            skim_dir,
            parallel,
            skip_bad_files=not debug,
            n_to_one=n_to_one,
        )

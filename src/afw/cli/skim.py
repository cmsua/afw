"""Command-line utility to create skims for a given :class:`objects.AnalysisConfig`."""

import logging
import os

import awkward as ak
import dask
import dask_awkward as dak
import uproot
from coffea.dataset_tools import apply_to_fileset, preprocess
from coffea.nanoevents import NanoAODSchema

from ..objects import AnalysisConfig
from . import utils
from ..dataset import print_summary, skimmed

## SOURCE: https://github.com/scikit-hep/coffea/discussions/1100


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
) -> None:
    """Create and save skims for a given :class:`objects.AnalysisConfig`

    Parameters:
        config (objects.AnalysisConfig): The config to create skims for
        xrd_redirector (str): The host of the XRootD Redirector to use
        skim_dir (str): The output directory for skims
        run_combined (bool, default False): Whether to submit preprocessing and skimming to the Dask Client as one compute or to run each dataset in series
        skip_bad_files (bool, default False): Whether or not to skip bad files in the dataset
    """
    # Load dataset, with preskims if needed
    my_dataset = config.get_dataset(xrd_redirector)

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
        skimmed_writable = skimmed_writable.repartition(
            n_to_one=15,
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


if __name__ == "__main__":
    # Setup Args
    parser = utils.get_common_args()
    parser.add_argument(
        "-p",
        "--parallel",
        action="store_true",
        help="Compute each dataset in parallel rather than in series",
    )
    args = parser.parse_args()

    # Setup Logging
    utils.setup_logging(args.debug)

    logger = logging.getLogger("Main")
    logger.info("Loaded program")

    # Create output dir
    skim_dir = os.path.expanduser(args.skim_dir)
    logger.info(f"Writing to skim dir {skim_dir}")

    # Get Dask Client
    client = utils.create_dask_client(args.cluster_address, [args.config])

    try:
        # Run on channel(s)
        for config in utils.get_configs(args.config):
            logger.info(f"Handling config {config}")
            handle_config(
                config,
                args.xrd_redirector,
                skim_dir,
                args.parallel,
                skip_bad_files=not args.debug,
            )

    finally:
        client.close()

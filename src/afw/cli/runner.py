"""Command-line utility to create run a given :class:`objects.AnalysisConfig`."""

import logging
import os
import pickle
import time

from coffea.nanoevents import NanoAODSchema
from coffea.processor import DaskExecutor, Runner

from .. import dataset
from ..objects import AnalysisConfig
from ..processor import MyProcessor
from . import plotter, utils


def handle_channel(
    config: AnalysisConfig,
    xrd_redirector: str,
    output_dir: str,
    skim_dir_root: str,
    runner: Runner,
) -> None:
    """Create and save plots for a given :class:`objects.AnalysisConfig`

    Parameters:
        config (objects.AnalysisConfig): The config to process
        xrd_redirector (str): The host of the XRootD Redirector to use
        output_dir (str): The output directory for plots
        skim_dir_root (str): The input directory for skims
        Runner (coffea.processor.Runner): The Coffea runner to use
    """
    logger.info(f"Handling channel {config.name}")

    # Load dataset, with preskims if needed
    my_dataset = config.get_dataset(xrd_redirector)

    # Check for skims
    skim_dir = os.path.join(skim_dir_root, config.name)
    if os.path.isdir(skim_dir):
        my_dataset = dataset.skimmed.convert_to_skimmed(my_dataset, skim_dir)
    else:
        logger.warning(
            f"Skim directory {skim_dir} does not exist, running from raw files..."
        )
        skim_dir = None

    # Print
    dataset.print_summary(my_dataset, logger, use_short_name=False)
    processor = MyProcessor(skimmed=skim_dir is not None, config=config)

    # Preprocess
    logger.info("Preprocessing fileset")
    preprocessed_fileset = runner.preprocess(my_dataset)

    # Run
    logger.info("Running Analysis")

    # Rub with time elapsed
    tstart = time.time()
    results, report = runner(preprocessed_fileset, processor_instance=processor)
    elapsed = time.time() - tstart

    # Print metrics
    logger.info(f"Processed {report['bytesread'] / 1e9:.3f} GB")
    logger.info(f"Processed {report['bytesread'] / 1e9 / elapsed:.3f} GB/sec")
    logger.info(f"Processed {report['entries']:>15,.0f} events")
    logger.info(f"Processed {report['entries'] / (elapsed):>15,.0f} events/s")

    # Save results
    output_dir = os.path.join(output_dir, config.name)
    os.makedirs(output_dir, exist_ok=True)

    logger.info("Saving hists")
    with open(os.path.join(output_dir, "results.pkl"), "wb") as file:
        pickle.dump(results, file)

    plotter.save_results(output_dir, "png", config.get_things_to_plot(), results)


if __name__ == "__main__":
    # Setup Args
    parser = utils.get_common_args()

    # Intermediates
    parser.add_argument(
        "-o", "--output_dir", default="plots", help="Directory in which to save plots"
    )

    args = parser.parse_args()

    # Setup Logging
    utils.setup_logging(args.debug)

    logger = logging.getLogger("Main")
    logger.info("Loaded Program and Arguments")

    output_dir = os.path.expanduser(args.output_dir)
    logger.info(f"Writing to output dir {output_dir}")

    skim_dir = os.path.expanduser(args.skim_dir)
    logger.info(f"Reading from skim dir {skim_dir}")

    # Get Dask Client
    client = utils.create_dask_client(args.cluster_address, [args.config])
    skim_dir_root = os.path.expanduser(args.skim_dir)

    try:
        # Create runner
        runner = Runner(
            DaskExecutor(client=client, compression=None),
            chunksize=500_000,
            # maxchunks=10,
            skipbadfiles=not args.debug,
            schema=NanoAODSchema,
            savemetrics=True,
        )

        # Run on channel
        for config in utils.get_configs(args.config):
            handle_channel(
                config,
                args.xrd_redirector,
                output_dir,
                skim_dir_root,
                runner,
            )
    finally:
        client.close()

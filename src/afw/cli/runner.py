"""Command-line utility to create run a given :class:`objects.AnalysisConfig`."""

import logging
import os
import pickle
import time

import dask
import uproot
from coffea.dataset_tools import apply_to_fileset, preprocess
from coffea.nanoevents import NanoAODSchema
from coffea.processor import DaskExecutor, Runner, accumulate
from dask.distributed import Client

from ..dataset import apply_xcache_host, print_summary, skimmed
from ..objects import AnalysisConfig, AnalysisStep
from ..processor import ModifyingProcessor, NonModifyingProcessor
from .utils import solve_dependency_chain, uproot_writeable

logger = logging.getLogger("Runtime")


def cache_accumulators(new_accumulators: dict[str, dict], output_dir: str):
    """
    Save the accumulators in a given chain

    Args:
        new_accumulators (dict[str, dict]): A mapping from accumulator names to accumulator to save
    """
    for step_name, accumulator in new_accumulators.items():
        output_file = os.path.join(output_dir, step_name + ".pkl")
        with open(output_file, "wb") as file:
            pickle.dump(accumulator, file)


def run_chain(
    chain: list[str],
    steps_dict: dict[str, AnalysisStep],
    dataset: dict,
    prev_accumulator: dict,
    save_files: bool,
    skim_dir: str,
    output_dir: str,
    runner: DaskExecutor,
):
    """
    Run a given chain of steps, returning the resulting accumulator and (optionally) the location of saved files

    Args:
        chain (list[str]): The list of steps to run
        steps_dict (dict[str, AnalysisStep]): The mapping from step names to step
        dataset (dict): The dataset to run over
        prev_accumulator (dict): The combined accumulator from any previous steps
        save_files (bool): Whether to save files to disk for reuse
        skim_dir (str): The directory from which to save/load skims
        output_dir (str): The output directory for accumulators
        runner (DaskExecutor): The executor to use
    """
    # This is a skimming run!
    if save_files:
        logger.debug("Running and saving files")
        logger.info("Preprocessing")
        dataset, _ = preprocess(
            dataset,
            align_clusters=False,
            step_size=100_000,
            files_per_batch=1,
            skip_bad_files=False,
            save_form=False,
        )

        logger.info("Computing Task Graph")
        processor = ModifyingProcessor(
            chain=chain, steps_dict=steps_dict, prev_accumulator=prev_accumulator
        )

        skimmed_dict = apply_to_fileset(
            processor.process, dataset, schemaclass=NanoAODSchema
        )

        skim_write_objs = []
        acc_dict = []
        for fileset_name, (skimmed_fileset, acc) in skimmed_dict.items():
            logger.debug(f"Computing task graph for {fileset_name}")
            skimmed_writable = uproot_writeable(skimmed_fileset)
            # Output directory
            destination = os.path.join(
                skim_dir, chain[-1], skimmed.escape_name(fileset_name)
            )

            # Return so that compute can be called
            process = uproot.dask_write(
                skimmed_writable,
                compute=False,
                tree_name="Events",
                destination=destination,
            )
            skim_write_objs += [process]
            acc_dict += [acc]

        # Compute
        logger.info("Processing")
        _, result = dask.compute(skim_write_objs, acc_dict)

        # Postprocess
        result = accumulate(result)
        result = processor.postprocess(result)
        logger.debug(f"Got result from compute: {result}")

        # Save
        cache_accumulators(result["breakdown"], output_dir)
        new_accum = accumulate([result["out"]], prev_accumulator)
        return new_accum, os.path.join(skim_dir, chain[-1])

    # This is not a skimming run!
    else:
        logger.debug("Running without modification of fileset")
        logger.info("Preprocessing")
        preprocessed_dataset = runner.preprocess(dataset)

        logger.info("Processing")
        processor = NonModifyingProcessor(
            chain=chain, steps_dict=steps_dict, prev_accumulator=prev_accumulator
        )

        tstart = time.time()
        result, report = runner(preprocessed_dataset, processor_instance=processor)
        elapsed = time.time() - tstart

        logger.debug(f"Got result from compute: {result}")

        # Save
        cache_accumulators(result["breakdown"], output_dir)

        # Print metrics
        logger.info(f"Processed {report['bytesread'] / 1e9:.3f} GB")
        logger.info(f"Processed {report['bytesread'] / 1e9 / elapsed:.3f} GB/sec")
        logger.info(f"Processed {report['entries']:>15,.0f} events")
        logger.info(f"Processed {report['entries'] / (elapsed):>15,.0f} events/s")
        return accumulate([result["out"]], prev_accumulator), None


def call(
    client: Client,
    config: AnalysisConfig,
    xrd_redirector: str,
    debug: bool,
    n_files: int,
    chunksize: int,
    skim_dir: str,
    output_dir: str,
    # target_step: str,
    **kwargs: dict,
):
    """
    Call this subcommand from the CLI

    Args:
        client (dask.distributed.Client): The dask client to use
        config (afw.objects.AnalysisConfig): The config to run over
        xrd_redirector (str): The input xrootd redirector
        debug (bool): Whether to use debug mode (crash on bad file rather than skip)
        n_files (int): If not None, the amount of files to limit to
        chunksize (int): The max chunk size allowed per worker
        skim_dir (str): The location to save/load skims
        output_dir (str): The directory to write plots to
        # target_step (str): The step to run
        **kwargs (dict): Any additional arguments
    """
    # Make output
    output_dir = os.path.abspath(os.path.join(output_dir, config.name))
    os.makedirs(output_dir, exist_ok=True)

    # Create runner
    runner = Runner(
        DaskExecutor(client=client, compression=None),
        chunksize=chunksize,
        # maxchunks=10,
        skipbadfiles=False,
        schema=NanoAODSchema,
        savemetrics=True,
    )

    # Load raw dataset
    logger.info("Getting dataset")
    raw_files = config.get_dataset(n_files)
    if n_files is not None:
        logger.critical(f"Limited to {n_files} files per fileset!")
    raw_files = apply_xcache_host(raw_files, xrd_redirector)
    print_summary(raw_files, logger, use_short_name=False)

    # Get target
    logger.info("Building chains")
    steps_dict = config.get_steps()
    all_chains = solve_dependency_chain(config.get_default_step(), steps_dict)
    logger.debug(f"Using list of chains {all_chains}")

    prev_accumulator = {}
    prev_files_output_dir = None

    for i, chain in enumerate(all_chains):
        is_final_chain = i + 1 == len(all_chains)
        logger.debug(f"Running chain {chain}, is final chain: {is_final_chain}")

        # Initial run, get the dataset from the config
        if chain[0] == "Raw Files":
            dataset = raw_files
        # Otherwise, use prev files output
        else:
            dataset = skimmed.convert_to_skimmed(raw_files, prev_files_output_dir)

        # Run
        prev_accumulator, prev_files_output_dir = run_chain(
            chain=chain,
            steps_dict=steps_dict,
            dataset=dataset,
            prev_accumulator=prev_accumulator,
            save_files=not is_final_chain,
            skim_dir=skim_dir,
            output_dir=output_dir,
            runner=runner,
        )
        logger.debug(f"Received new accumulator {prev_accumulator}")

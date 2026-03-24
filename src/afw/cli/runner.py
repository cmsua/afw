"""Command-line utility to create run a given :class:`objects.AnalysisConfig`."""

import logging
import os
import pickle
import time

from typing import Callable
import dask
import uproot
from coffea.dataset_tools import apply_to_fileset, preprocess
from coffea.nanoevents import NanoAODSchema
from coffea.processor import DaskExecutor, Runner, accumulate, IterativeExecutor
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
        # Skip empty accumulators
        if len(accumulator.keys()) == 0:
            continue

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
    cleaner: Callable,
    runner,
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
        cleaner (Callable): If saving events, the function to clean them
        runner (Executor): The executor to use
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
            skimmed_clean = cleaner(skimmed_fileset)
            skimmed_writable = uproot_writeable(skimmed_clean)
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
        return new_accum

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
        return accumulate([result["out"]], prev_accumulator)


def call(
    client: Client,
    config: AnalysisConfig,
    xrd_redirector: str,
    debug: bool,
    n_files: int,
    chunksize: int,
    skim_dir: str,
    output_dir: str,
    target_step: str = None,
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
    executor = (
        IterativeExecutor()
        if client is None
        else DaskExecutor(client=client, compression=None)
    )
    runner = Runner(
        executor=executor,
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
    if target_step is None:
        target_step = config.get_default_step()
        logger.info("Using target step {target_step}")

    # List cached
    cached_available = []
    for step_name, step in steps_dict.items():
        if step.modifies_events and os.path.exists(os.path.join(skim_dir, step_name)):
            cached_available += [step_name]
        elif os.path.exists(os.path.join(output_dir, f"{step_name}.pkl")):
            cached_available += [step_name]

    if target_step in cached_available:
        logger.critical(f"Requested step {target_step} is cached - not running!")
        return

    # List the chain
    all_chains, cached_to_load, initial_data = solve_dependency_chain(
        target_step, steps_dict, cached_steps=cached_available
    )

    logger.debug(f"Using list of chains {all_chains}")

    # Load values from the cache
    logger.info(f"Loading the following cached steps: {cached_available}")
    prev_accumulator = {}
    for step_name in cached_to_load:
        cache_file = os.path.join(output_dir, f"{step_name}.pkl")
        if os.path.exists(cache_file):
            with open(cache_file, "rb") as file:
                prev_accumulator = accumulate([pickle.load(file)], prev_accumulator)
    logger.debug(f"Loaded from cache: {prev_accumulator}")

    # Loop over chains
    prev_chain_last_step = initial_data
    prev_modifying_steps = ["Raw Files"]
    for i, chain in enumerate(all_chains):
        is_final_chain = i + 1 == len(all_chains)
        logger.debug(f"Running chain {chain}, is final chain: {is_final_chain}")

        # Initial run, get the dataset from the config
        if prev_chain_last_step == "Raw Files":
            dataset = raw_files
        # Otherwise, use prev files output
        else:
            dataset = skimmed.convert_to_skimmed(
                raw_files, os.path.join(skim_dir, prev_chain_last_step)
            )

        # Add to list of previous modifying steps
        for step_name in chain:
            if steps_dict[step_name].modifies_events:
                prev_modifying_steps += [step_name]

        if skim_dir == "none":
            chain = prev_modifying_steps + chain

        # Run
        prev_accumulator = run_chain(
            chain=chain,
            steps_dict=steps_dict,
            dataset=dataset,
            prev_accumulator=prev_accumulator,
            save_files=(not is_final_chain) and (skim_dir != "none"),
            skim_dir=skim_dir,
            output_dir=output_dir,
            cleaner=config.clean_events,
            runner=runner,
        )
        logger.debug(f"Received new accumulator {prev_accumulator}")
        prev_chain_last_step = chain[-1]

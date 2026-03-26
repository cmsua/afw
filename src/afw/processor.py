import abc
import logging
import os
import pickle
import time
from typing import Callable

import awkward as ak
import dask
import dask_awkward as dak
import uproot
from coffea.dataset_tools import apply_to_fileset, preprocess
from coffea.nanoevents import NanoAODSchema
from coffea.processor import (
    DaskExecutor,
    IterativeExecutor,
    ProcessorABC,
    Runner,
    accumulate,
)
from dask.distributed import Client

from .objects import MicroProcessorABC, Stage
from .utils import slugify

logger = logging.getLogger("Stages")


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


class ProcessEvents(Stage):
    def __init__(
        self,
        name: str,
        processors: list[MicroProcessorABC],
        dataset: dict,
        accumulator: dict,
    ):
        super().__init__(name=name)
        self.accumulator = accumulator
        self.processors = processors
        self.dataset = dataset

    def run(self, output_dir: str, **kwargs: dict):
        dataset = self.dataset
        if callable(dataset):
            dataset = dataset(**{"output_dir": output_dir, **kwargs})

        accumulator = self.accumulator
        if callable(accumulator):
            accumulator = accumulator(**{"output_dir": output_dir, **kwargs})

        result = self.process(dataset, accumulator, **kwargs)

        # Make output and save
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{self.name}.pkl")
        with open(output_file, "wb") as file:
            pickle.dump(result, file)

    @abc.abstractmethod
    def process(self, dataset: dict, accumulator: dict, **kwargs: dict):
        pass


class ProcessEventsNonSkimming(ProcessEvents):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def process(
        self,
        dataset: dict,
        accumulator: dict,
        client: Client,
        chunk_size: int,
        skip_bad_files: bool,
        **kwargs: dict,
    ):

        # Create runner
        executor = (
            IterativeExecutor()
            if client is None
            else DaskExecutor(client=client, compression=None)
        )
        runner = Runner(
            executor=executor,
            chunksize=chunk_size,
            # maxchunks=10,
            skipbadfiles=skip_bad_files,
            schema=NanoAODSchema,
            savemetrics=True,
        )

        logger.debug("Running without modification of fileset")
        logger.info("Preprocessing")
        preprocessed_dataset = runner.preprocess(dataset)

        logger.info("Processing")
        processor = NonModifyingProcessor(
            processors=self.processors, accumulator=accumulator
        )

        tstart = time.time()
        result, report = runner(preprocessed_dataset, processor_instance=processor)
        elapsed = time.time() - tstart

        logger.debug(f"Got result from compute: {result}")

        # Save

        # Print metrics
        logger.info(f"Processed {report['bytesread'] / 1e9:.3f} GB")
        logger.info(f"Processed {report['bytesread'] / 1e9 / elapsed:.3f} GB/sec")
        logger.info(f"Processed {report['entries']:>15,.0f} events")
        logger.info(f"Processed {report['entries'] / (elapsed):>15,.0f} events/s")
        return result


class ProcessEventsSkimming(ProcessEvents):
    def __init__(self, batch: bool = False, **kwargs):
        self.batch = batch
        super().__init__(**kwargs)

    def run_batch(
        self, dataset: dict, process_function: Callable, output_dir: str
    ) -> dict:
        skimmed_dict = apply_to_fileset(
            process_function, dataset, schemaclass=NanoAODSchema
        )

        # After skimming, handle saving
        skim_write_objs = []
        acc_list = []
        for fileset_name, (skimmed_fileset, acc) in skimmed_dict.items():
            logger.debug(f"Computing task graph for {fileset_name}")
            skimmed_writable = uproot_writeable(skimmed_fileset)
            # Output directory
            destination = os.path.join(output_dir, slugify(fileset_name))

            # Return so that compute can be called
            process = uproot.dask_write(
                skimmed_writable,
                compute=False,
                tree_name="Events",
                destination=destination,
            )
            skim_write_objs += [process]
            acc_list += [acc]

        # Compute
        logger.info("Processing")
        _, result = dask.compute(skim_write_objs, acc_list)
        return result

    def run_indep(
        self, dataset: dict, process_function: Callable, output_dir: str
    ) -> dict:

        # After skimming, handle saving
        acc_list = []
        for fileset_name, fileset in dataset.items():
            skimmed_fileset, acc = apply_to_fileset(
                process_function, {fileset_name: fileset}, schemaclass=NanoAODSchema
            )
            logger.info(f"Processing {fileset_name}")
            skimmed_writable = uproot_writeable(skimmed_fileset)
            # Output directory
            destination = os.path.join(output_dir, slugify(fileset_name))

            # Return so that compute can be called
            process = uproot.dask_write(
                skimmed_writable,
                compute=False,
                tree_name="Events",
                destination=destination,
            )
            _, acc = dask.compute(process, acc)
            acc_list += [acc]
        return acc_list

    def process(
        self,
        dataset: dict,
        accumulator: dict,
        skip_bad_files: bool,
        chunk_size: int,
        skim_dir: str,
        **kwargs: dict,
    ):
        logger.debug("Running and saving files")
        logger.info("Preprocessing")
        dataset, _ = preprocess(
            dataset,
            align_clusters=False,
            step_size=chunk_size,
            files_per_batch=1,
            skip_bad_files=skip_bad_files,
            save_form=False,
        )

        logger.info("Computing Task Graph")
        processor = ModifyingProcessor(
            processors=self.processors, accumulator=accumulator
        )

        if self.batch:
            result = self.run_batch(
                dataset, processor.process, os.path.join(skim_dir, self.name)
            )
        else:
            result = self.run_indep(
                dataset, processor.process, os.path.join(skim_dir, self.name)
            )

        # Postprocess
        result = accumulate(result)
        result = processor.postprocess(result)
        logger.debug(f"Got result from compute: {result}")
        return result


class ModifyingProcessor:
    """
    A processor that can handle a chain of processors
    """

    def __init__(
        self,
        processors: list[MicroProcessorABC],
        accumulator: dict = {},
    ) -> None:
        """
        Args:
            processors (list[str]): The list of processors to run (by name) in order
            accumulator (dict): The accumulator from running any previous runs
        """
        self.accumulator = accumulator
        self.processors = processors

    def process(self, events: ak.Array) -> tuple[ak.Array, dict, dict]:
        """
        Args:
            events (ak.Array): The events from any previous chain

        Returns:
            tuple[ak.Array, dict]: Returns the events for future chains and the cumulative accumulator
        """
        out = self.accumulator
        for processor in self.processors:
            events, out = processor.process(events, out)

        return events, out

    def postprocess(self, result: dict) -> dict:
        """
        Args:
            result (dict): The result address from ``process()``

        Returns:
            dict: The post-processed result
        """
        for processor in self.processors:
            if not hasattr(processor, "postprocess") or not callable(
                processor.postprocess
            ):
                continue

            p_result = processor.postprocess(result)
            if p_result is not None:
                result = p_result
        return result


class NonModifyingProcessor(ModifyingProcessor, ProcessorABC):
    """
    An instance of ModifyingProcessor that does not return events and thus can be called via Coffea
    """

    def __init__(self, **kwargs: dict):
        super().__init__(**kwargs)

    def process(self, events):
        events, result = super().process(events)
        return result

    def postprocess(self, result: dict) -> dict:
        return super().postprocess(result)


__all__ = [ProcessEventsNonSkimming, ProcessEventsSkimming, MicroProcessorABC]

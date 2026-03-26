"""
This file contains the bare-bones essentials of any analysis.
"""

import abc
import os
import pickle

import awkward as ak
import hist

from .dataset import apply_xrd_redirector, build_datasets, to_skimmed


class Stage(abc.ABC):
    """
    Represents one stage of an analysis (eg. the first or second run)
    """

    def __init__(self, name: str):
        """
        Args:
            name (str): The name of this stage.
        """
        self.name = name

    @abc.abstractmethod
    def run(self, **kwargs: dict):
        """
        Runs the given stage.

        Args:
            **kwargs (dict): Any command-line arguments passed to the analysis
        """
        pass


class AnalysisConfig(abc.ABC):
    """An analysis config"""

    def __init__(self, name: str):
        self.name = name

    @abc.abstractmethod
    def get_options(self) -> dict[str, list[Stage]]:
        """
        Returns the set of runnable options in this analysis.

        Returns:
            dict[str, list[Stage]]: A mapping between runnable options and the stages they contain
        """
        pass

    def build_datasets(self, defs_file: str):
        """
        Utility method. Given a definitions file, returns a runnable that, when called, will use xrd_redirector and n_files to properly build the dataset.

        Args:
            defs_file (str): The definitions yaml file

        Returns:
            callable: A callable that requires ``xrd_redirector`` and ``n_files`` at runtime
        """

        def build_datasets_at_runtime(xrd_redirector: str, n_files: int, **kwargs: dict):
            return apply_xrd_redirector(build_datasets(defs_file, n_files), xrd_redirector)

        return build_datasets_at_runtime

    def load_from_skims(self, task_name: str, defs_file: str, **kwargs: dict):
        """
        Utility method. Given a definitions file, returns a runnable that, when called, will generate a dataset definitions from a previous run's skims using ``skim_dir`` and ``task_name``

        Args:
            task_name (str): The task to load frim
            defs_file (str): The definitions yaml file

        Returns:
            callable: A callable that requires ``skim_dir`` at runtime
        """
        return lambda skim_dir, **kwargs2: to_skimmed(
            build_datasets(defs_file), os.path.join(skim_dir, task_name)
        )

    def load_from_pickle(self, task_name: str, **kwargs: dict):
        """
        Utility method. Given a definitions file, returns a runnable that, when called, will load a pickle from the given task's ``output_dir``

        Args:
            task_name (str): The task to load from

        Returns:
            callable: A callable that requires ``output_dir`` at runtime
        """
        def load_from_pickle_runtime(output_dir: str, **kwargs2: dict):
            file_name = os.path.join(output_dir, f"{task_name}.pkl")
            with open(file_name, "rb") as file:
                return pickle.load(file)

        return load_from_pickle_runtime


class MicroProcessorABC(abc.ABC):
    """
    A micro-processor, for use later
    """

    @abc.abstractmethod
    def process(self, events: ak.Array, accumulator: dict) -> tuple[ak.Array, dict]:
        """
        Processes the given step, akin to a Coffea Processor. Returns a tuple consisting of ``events, accumulator`` from dependencies.

        Args:
            events (ak.array): The initial events object. This should only be modified if ``modifies_events`` is ``True``, or the dependency solver may error.
            accumulator (dict): A coffea accumulator, summed and returned from previous dependencies

        Returns:
            ak.Array, dict: A tuple consisting of ``events`` and a new accumulator, which will be post-processed after ran on all files and then merged with ``prev_accumulator``.
        """
        pass

    def postprocess(self, accumulator: dict) -> dict:
        """
        Post-processes the given step, akin to a Coffea Processor.

        Args:
            accumulator (dict): A coffea accumulator, summed and returned from ``process``

        Returns:
            dict: A coffea accumulator
        """
        pass


class Plottable(abc.ABC):
    """
    A generic class representing any given object to plot
    """

    def __init__(self, label: str):
        """
        Parameters:
            label (str): The label of the overall plot
        """
        self.label = label

    @abc.abstractmethod
    def create_histogram(self) -> hist.Hist:
        """
        Create a histogram for the given thing to plot.

        Returns:
            hist.Hist: A histogram.
        """
        pass

    @abc.abstractmethod
    def fill_histogram(
        self,
        histogram: hist.Hist,
        events: ak.Array,
        dataset: str,
        weights: ak.Array,
        **kwargs: dict,
    ) -> hist.Hist:
        """
        Fill a histogram with a given set of events, post-selection

        Args:
            events (ak.Array): Events post-selection
            dataset (str): The dataset for which the events belong
            weights (ak.Array): A one-dimensional array with the same length as events

        Returns:
            hist.Hist: A histogram with filled data
        """
        pass

    @abc.abstractmethod
    def plot_histogram(
        self,
        histogram: hist.Hist,
        metadata: dict,
        title: str,
        output_file: str,
    ) -> None:
        """
        Plot a filled histogram to a given file

        Args:
            histogram (hist.Hist): The filled histogram to plot.
            metadata (dict): The metadata for use with generating plots.
            title (str): The title (eg. channel) to use
            output_file (str): The output file, given as an absolute path.
            **kwargs (dict | None): A set of keyword arguments created in by the analysis config.

        Returns:
            None
        """
        pass


__all__ = [MicroProcessorABC, Plottable, Stage]

"""
This file contains the bare-bones essentials of any analysis.
"""

import abc

import awkward as ak
import hist


class AnalysisStep(abc.ABC):
    """
    A step in any given analysis
    """

    def __init__(
        self,
        dependencies: list[str] = [],
        modifies_events: bool = False,
        split_required: bool = False
    ):
        """
        Args:
            dependencies (str | list[str]): Any dependencies given for this particular step of the analysis, referred to by their name in the analysis config. This may be given as a list, or in the case of a singular dependency, a string. If specifying multiple dependencies, only one may modify the ``events`` object.
            modifies_events (bool, default False): Whether the given step modifies the ``events`` object.
            split_required (bool, default False): Whether the given step requires post-processing in order for it to be utilized as a dependency
        """
        self.dependencies = (
            dependencies if isinstance(dependencies, list) else [dependencies]
        )
        self.modifies_events = modifies_events
        self.split_required = split_required

    @abc.abstractmethod
    def process(
        self, events: ak.Array, prev_accumulator: dict
    ) -> tuple[ak.Array, dict]:
        """
        Processes the given step, akin to a Coffea Processor. Returns a tuple consisting of ``events, accumulator`` from dependencies.

        Args:
            events (ak.array): The initial events object. This should only be modified if ``modifies_events`` is ``True``, or the dependency solver may error.
            prev_accumulator (dict): A coffea accumulator, summed and returned from previous dependencies

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


class AnalysisConfig(abc.ABC):
    """
    An object representing a full analysis
    """

    def __init__(self, name: str):
        """
        Args:
            name (str): The name of the analysis. This should be a valid folder name
        """
        self.name = name

    @abc.abstractmethod
    def get_dataset(self, max_files: int = None) -> dict:
        """
        Gets the fully-formatted dataset for the current analysis

        Args:
            max_files (int, default None): The maximum amount of files per fileset to allow
        Returns:
            dict: A fully rendered dataset with a list of files and metadata
        """
        pass

    @abc.abstractmethod
    def get_steps(self) -> dict[str, AnalysisStep]:
        """
        Gets a mapping of steps associated with this analysis

        Returns:
            dict[str, AnalysisStep]: A mapping from the name of the analysis step to its implementation
        """
        pass

    @abc.abstractmethod
    def get_default_step(self) -> str:
        """
        Returns:
            str: The name of the default step of the analysis to run
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
        self.escaped_name = label.replace("$", "").replace("\\", "").replace("/", "")

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


class BlankAnalysisStep(AnalysisStep):
    """A blank step, used either to force the use of raw files or to combine several existing steps in logic"""

    def __init__(
        self,
        dependencies: list[str] = [],
        modifies_events: bool = False,
        split_required: bool = False
    ):
        super().__init__(
            dependencies=dependencies,
            modifies_events=modifies_events,
            split_required=split_required
        )
        
    def process(
        self, events: ak.Array, prev_accumulator: dict
    ) -> tuple[ak.Array, dict]:
        return events, {}

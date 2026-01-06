"""
This file contains the bare-bones essentials of any analysis.
"""

import abc

import awkward as ak
import hist


class ThingToPlot(abc.ABC):
    """
    A generic class representing any given object to plot
    """

    def __init__(self, title: str):
        """
        Parameters:
            title (str): The title of the overall plot
        """
        self.title = title
        self.escaped_name = title.replace("$", "")

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
    def plot_histogram(self, histogram: hist.Hist, output_file: str) -> None:
        """
        Plot a filled histogram to a given file

        Args:
            histogram (hist.Hist): The filled histogram to plot.
            output_file (str): The output file, given as an absolute path.
            **kwargs (dict | None): A set of keyword arguments created in by the analysis config.

        Returns:
            None
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
    def get_dataset(self, xcache_host: str) -> dict:
        """
        Gets the fully-formatted dataset for the current analysis

        Args:
            xcache_host (str): The address of the local xcache redirector, if present
        Returns:
            dict: A fully rendered dataset with a list of files and metadata
        """
        pass

    @abc.abstractmethod
    def define_objects(self, events: ak.Array) -> ak.Array:
        """
        Define objects, such as leptons. This method operates in-place

        Args:
            events (ak.Array): An awkward array of events, as read from root files

        Returns:
            ak.Array: The new events
        """
        pass

    @abc.abstractmethod
    def preselect_events(self, events: ak.Array) -> ak.Array:
        """
        Run preselection of events

        Args:
            events (ak.Array): An awkward array of events post object definition

        Returns:
            events (ak.Array): An awkward array of events which pass preselection
        """
        pass

    @abc.abstractmethod
    def minify(self, events: ak.Array) -> ak.Array:
        """
        Minify a set of events to be saved locally, for use in skimming

        Args:
            events (ak.Array): An awkward array of events, as read from root files

        Returns:
            ak.Array: The new events
        """
        pass

    @abc.abstractmethod
    def select_events(self, events: ak.Array) -> list[bool]:
        """
        Run selection of events

        Args:
            events (ak.Array): An awkward array of events post object definition and preselection

        Returns:
            mask: A mask on events representing whether the event passes preselection
        """
        pass

    @abc.abstractmethod
    def augment_events(self, events: ak.Array) -> dict[ak.Array]:
        """
        Augment a set of events for use in plotting. This should be used to eliminate dual-computation of properties for separate plots

        Args:
            events (ak.Array): An awkward array of events post event selection

        Returns:
            dict[ak.Array]: A set of keyword arguments that will get passed to the ThingToPlot.fill_histogram method
        """
        pass

    @abc.abstractmethod
    def get_things_to_plot(self) -> list[ThingToPlot]:
        """
        Create a list of things to plot

        Returns:
            list[ThingToPlot]: Any set of things to plot
        """
        pass

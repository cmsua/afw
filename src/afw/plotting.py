"""
A utility module containing commonly-plotted things
"""

import awkward as ak
import hist
import mplhep as hep
import numpy as np
import os

from typing import Callable
from .objects import Plottable, MicroProcessorABC, Stage
from .utils import slugify


class FillHistograms(MicroProcessorABC):
    """
    The final step in the analysis, which computes weights and fills histograms
    """

    def __init__(
        self,
        plottables: list[Plottable],
        compute_weights: Callable,
    ):
        """
        Args:
            plottables (list[Plottable]): A list of all plottables
            compute_weights (Callable): Accepts events and prev_accumulator and returns an ``ak.Array`` of weights
        """
        super().__init__()

        self.plottables = plottables
        self.compute_weights = compute_weights

    def process(
        self, events: ak.Array, prev_accumulator: dict
    ) -> tuple[ak.Array, dict]:
        result = {}
        for plottable in self.plottables:
            # Use mask if present
            if plottable.where is not None:
                mask = plottable.where(events)
                result[plottable.label] = plottable.fill_histogram(
                    plottable.create_histogram(),
                    events[mask],
                    events.metadata["shortName"],
                    self.compute_weights(events[mask], prev_accumulator),
                )
            else:
                result[plottable.label] = plottable.fill_histogram(
                    plottable.create_histogram(),
                    events,
                    events.metadata["shortName"],
                    self.compute_weights(events, prev_accumulator),
                )

        return events, {"plots": result}


class SaveHistograms(Stage):
    def __init__(self, name: str, plottables: list[Plottable], dataset: dict | Callable, accumulator: dict|Callable):
        super().__init__(name=name)
        self.plottables = plottables

        self.dataset = dataset
        self.accumulator = accumulator

    def run(self, output_dir: str, extension: str, **kwargs: dict):
        dataset = self.dataset
        if callable(dataset):
            dataset = dataset(**{"output_dir": output_dir, **kwargs})

        accumulator = self.accumulator
        if callable(accumulator):
            accumulator = accumulator(**{"output_dir": output_dir, **kwargs})

        # Get metadata
        metadata = {}
        for key, val in dataset.items():
            name = val["metadata"]["shortName"]
            metadata[name] = val["metadata"]

        

        plots = accumulator["plots"]
        plot_dir = os.path.join(output_dir, "plots")
        os.makedirs(plot_dir, exist_ok=True)

        for plottable in self.plottables:
            output_file = os.path.join(
                plot_dir, f"{slugify(plottable.label)}.{extension}"
            )
            plottable.plot_histogram(
                histogram=plots[plottable.label],
                metadata=metadata,
                title=plottable.label,
                output_file=output_file,
            )


def stacked_colors(num: int) -> list[str]:
    """
    Returns a list of colors consistent with the CMS Analysis Guidelines (https://cms-analysis.docs.cern.ch/guidelines/plotting/colors/)

    Args:
        num (int): The number of colors to request (max 10)
    Returns:
        list[str]: A list of colors, formatted in hexadecimal
    """
    colors = """
    #3f90da
    #ffa90e
    #bd1f01
    #94a4a2
    #832db6
    #a96b59
    #e76300
    #b9ac70
    #717581
    #92dadd 	""".split()
    return colors[0:num]


def create_single_axis_histogram(axis: hist.axis.AxesMixin) -> hist.Hist:
    """
    Creates a hist.Hist with a dataset axis and weights, alongside a given axis

    Parameters:
        axis (hist.Axis) - The given axis to create a histogram with

    Returns:
        hist.Hist: A histogram with the given axis as well as a dataset axis and weight storage
    """
    dataset_axis = hist.axis.StrCategory(
        [], name="dataset", label="Process", growth=True
    )

    return hist.Hist(
        dataset_axis,
        axis,
        storage=hist.storage.Weight(),
    )


class Arbitrary(Plottable):
    """
    Plots some arbitrary thing.
    """

    def __init__(
        self,
        # Essential
        label: str,
        units: str,
        fetch_data: Callable,
        where: Callable | None = None,
        # Type: "continuous", "discrete"
        hist_type: str = "continuous",
        # For hist_type == "continuous"
        n_bins: int = None,
        low_bin: float = None,
        high_bin: float = None,
        # For hist_type == "discrete"
        bin_values: list = None,
    ):
        """
        Args:
            label (str): The label on the x-axis of the chart
            units (str): The units of bin width
            fetch_data (Callable): Given ``events``, returns the values to be plotted. This should not include the ``where`` filter if present.
            where (Callable): Given ``events``, returns a mask of events to process.
            hist_type (str): One of ``"continuous"`` or ``"discrete"``
            n_bins (int, default None): If ``hist_type == "continuous"``, the number of bins present
            low_bin (int, default None): If ``hist_type == "continuous"``, the lower bound for bins
            high_bin (int, default None): If ``hist_type == "continuous"``, the upper bound for bins
            bin_values (list, default None): If ``hist_type == "discrete"``, the possible values present for each bin

        """
        super().__init__(label=label)

        self.units = units
        self.fetch_data = fetch_data
        self.where = where

        self.hist_type = hist_type
        assert hist_type == "continuous" or hist_type == "discrete"

        if hist_type == "continuous":
            self.n_bins = n_bins
            self.low_bin = low_bin
            self.high_bin = high_bin
            assert n_bins is not None
            assert low_bin is not None
            assert high_bin is not None
        elif hist_type == "discrete":
            self.bin_values = bin_values

    def create_histogram(self) -> hist.Hist:
        # Create the Boost histogram from called parameters
        if self.hist_type == "continuous":
            axis = hist.axis.Regular(
                self.n_bins,
                self.low_bin,
                self.high_bin,
                name="values",
                label=self.label,
            )
        elif self.hist_type == "discrete":
            axis = hist.axis.Variable(
                self.bin_values, name="values", label=self.label
            )
        else:
            raise ValueError(
                "self.hist_type was changed between __init__ and create_histogram!"
            )
        return create_single_axis_histogram(axis)

    def fill_histogram(
        self,
        histogram: hist.Hist,
        events: ak.Array,
        dataset: str,
        weights: ak.Array,
        **kwargs: dict[ak.Array],
    ) -> hist.Hist:
        histogram.fill(
            dataset=dataset, values=self.fetch_data(events, **kwargs), weight=weights
        )
        return histogram

    def plot_histogram(
        self,
        histogram: hist.Hist,
        metadata: dict,
        title: str,
        output_file: str,
    ) -> None:
        data_fields = [key for key, val in metadata.items() if val.get("isData", True)]
        mc_keys = [field for field in histogram.axes[0] if field not in data_fields]

        # Filter - some categories get ALL events filtered and thus aren't present in the histograms
        data_fields = [field for field in data_fields if field in histogram.axes[0]]
        mc_keys = [field for field in mc_keys if field in histogram.axes[0]]

        data = histogram[data_fields, :][sum, :]

        signal_keys = [key for key, val in metadata.items() if val.get("signal", False)]
        stacked_keys = [key for key in mc_keys if key not in signal_keys]
        stacked_histos = [histogram[key, :] for key in stacked_keys]

        # DISABLED: This can vary between plots!
        # pairs = zip(stacked_keys, stacked_histos)
        # pairs_sorted = sorted(pairs, key=lambda pair: pair[1].sum().value, reverse=True)
        # stacked_keys, stacked_histos = list(zip(*pairs_sorted))

        fig, (ax_main, ax_comparison, ax_comparison_2) = hep.subplots(nrows=3)
        hep.comp.data_model(
            data,
            stacked_components=stacked_histos,
            stacked_labels=stacked_keys,
            stacked_colors=stacked_colors(len(stacked_histos)),
            fig=fig,
            ax_comparison=ax_comparison,
            ax_main=ax_main,
            h1_label="Data",
            h2_label="MC",
            comparison="split_ratio",
            ylabel=f"Counts / {min(stacked_histos[0].axes[0].widths):.0f} {self.units}",
        )

        # Add Signal
        for signal_key in signal_keys:
            hep.histplot(
                histogram[signal_key, :], ax=ax_main, label=signal_key, color="#000000"
            )

        # Add Second Comp
        hep.comp.comparison(
            data,
            sum(stacked_histos),
            ax_comparison_2,
            comparison="relative_difference",
            xlabel=self.label,
        )

        # Log Scale
        ax_main.set_yscale("log")
        ax_main.set_title(title)

        # Label
        hep.cms.label(
            "Preliminary", data=True, ax=ax_main, year="2022EE", lumi="26.7", com=13.6
        )

        fig.savefig(output_file)


class NJets(Arbitrary):
    """
    Plot a the number of jets in each event
    """

    def __init__(self):
        super().__init__(
            label="NJets",
            units="Jet",
            fetch_data=lambda events: ak.num(events.Jet),
            hist_type="discrete",
            bin_values=list(range(0, 16)),
        )


class Discriminant(Arbitrary):
    """
    Plot a discriminant (a float ranging from 0 to 1) such as DeepJet Score
    """

    def __init__(self, label: str, fetch_data: Callable):
        """
        Parameters:
            label (str): The label of the overall plot.
            fetch_data (typing.Callable): Given ``events``, returns the discriminant
        """
        super().__init__(
            label=label,
            units="Units",
            fetch_data=fetch_data,
            hist_type="continuous",
            n_bins=500,
            low_bin=0,
            high_bin=1,
        )


class Pt(Arbitrary):
    """
    Plot the pT of a given object.

    No check is done to ensure the given lepton is present. Non-present leptons will result in a crash.
    """

    def __init__(self, label: str, fetch_data: Callable):
        """
        Parameters:
            label (str): The label of the overall plot.
            fetch_data (typing.Callable): Given ``events``, returns the pt to plot
        """
        super().__init__(
            label=label,
            units="GeV",
            fetch_data=fetch_data,
            hist_type="continuous",
            n_bins=500,
            low_bin=0,
            high_bin=500,
        )


class Eta(Arbitrary):
    """
    Plot the eta of a given object.
    """

    def __init__(self, label: str, fetch_data: Callable):
        """
        Parameters:
            label (str): The label of the overall plot.
            fetch_data (typing.Callable): Given ``events``, returns the pt to plot
        """
        super().__init__(
            label=label,
            units="Radians",
            fetch_data=fetch_data,
            hist_type="continuous",
            n_bins=500,
            low_bin=-5,
            high_bin=5,
        )


class DileptonMass(Arbitrary):
    """
    Plot the dilepton mass of two leptons. The order doesn't matter due to the cos and cosh functions being even.

    No check is done to ensure the given lepton is present. Non-present leptons will result in a crash.
    """

    def __init__(self, label: str, fetch_data: Callable):
        """
        Parameters:
            label (str): The label of the overall plot.
            fetch_data (Callable): Returns a tuple consisting of two lepton arrays
        """

        def calculate_mass(events):
            obj_1, obj_2 = fetch_data(events)
            return np.sqrt(
                2
                * obj_1.pt
                * obj_2.pt
                * (np.cosh(obj_1.eta - obj_2.eta) - np.cos(obj_1.phi - obj_2.phi))
            )

        super().__init__(
            label=label,
            units="GeV",
            fetch_data=calculate_mass,
            hist_type="continuous",
            n_bins=500,
            low_bin=0,
            high_bin=1000,
        )

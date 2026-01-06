"""
A utility module containing commonly-plotted things
"""

import abc
import awkward as ak
import hist
import mplhep as hep
import numpy as np

from .objects import ThingToPlot


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


def plot_thing(histogram: hist.Hist, title: str, units: str):
    """
    A convenience function used to generate a bar graph and Data/MC agreement plot, as well as plot signal

    Args:
        histogram (hist.Hist): The histogram object to plot. This should have two axis: a dataset axis and an axis to plot
        title (str): The title of the appropriate histogram
        units (str): The units of bin width, for use when labeling axis

    Returns:
        plt.Figure: A figure which may be saved locally
    """
    data_fields = ["EGamma", "Muon", "MuonEG"]
    mc_keys = [field for field in histogram.axes[0] if field not in data_fields]

    data = histogram[data_fields, :][sum, :]

    stacked_keys = [key for key in mc_keys if key != "TTTT"]
    stacked_histos = [histogram[key, :] for key in stacked_keys]

    pairs = zip(stacked_keys, stacked_histos)
    pairs_sorted = sorted(pairs, key=lambda pair: pair[1].sum().value, reverse=True)
    stacked_keys, stacked_histos = list(zip(*pairs_sorted))

    fig, (ax_main, ax_comparison) = hep.subplots(nrows=2)
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
        xlabel=title,
        ylabel=f"Counts / {min(stacked_histos[0].axes[0].widths):.0f} {units}",
    )

    hep.histplot(histogram["TTTT", :], ax=ax_main, label="TTTT", color="#000000")

    ax_main.set_yscale("log")

    hep.cms.label(
        "Preliminary", data=True, ax=ax_main, year="2022EE", lumi="26.7", com=13.6
    )

    return fig


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


class NJetToPlot(ThingToPlot):
    """
    Plot a the number of jets in each event
    """

    def __init__(self):
        super().__init__(title="NJets")

    def create_histogram(self) -> hist.Hist:
        axis = hist.axis.Variable(
            list(range(4, 16)), name="njet", label="Jet multiplicity"
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
        histogram.fill(dataset=dataset, njet=ak.num(events.Jet), weight=weights)

        return histogram

    def plot_histogram(self, histogram: hist.Hist, output_file: str) -> None:
        fig = plot_thing(histogram[:, ::10j], self.title, "GeV")
        fig.savefig(output_file)


class DiscriminantToPlot(ThingToPlot, abc.ABC):
    """
    Plot a discriminant (a float ranging from 0 to 1) such as DeepJet Score
    """

    def __init__(self, title: str):
        """
        Parameters:
            title (str): The title of the overall plot.
        """
        super().__init__(title=title)

    def create_histogram(self) -> hist.Hist:
        axis = hist.axis.Regular(500, 0, 1, name="score", label=self.title)
        return create_single_axis_histogram(axis)

    def plot_histogram(self, histogram: hist.Hist, output_file: str) -> None:
        fig = plot_thing(histogram[:, ::10j], self.title, "Units")
        fig.savefig(output_file)


class PtToPlot(ThingToPlot):
    """
    Plot the pT of a given lepton. Only the lepton with the given index will be plotted.

    No check is done to ensure the given lepton is present. Non-present leptons will result in a crash.
    """

    def __init__(self, title: str, lepton_name: str, index: int):
        """
        Parameters:
            title (str): The title of the overall plot.
            lepton_name (str): The name of the lepton. This should match a key in events.
            index (int): The index of the targeted lepton.
        """
        super().__init__(title=title)
        self.lepton_name = lepton_name
        self.index = index

    def create_histogram(self) -> hist.Hist:
        axis = hist.axis.Regular(
            500,
            0,
            500,
            name="pt",
            label=self.title,
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
            dataset=dataset,
            pt=events[self.lepton_name].pt[:, self.index],
            weight=weights,
        )

        return histogram

    def plot_histogram(self, histogram: hist.Hist, output_file: str) -> None:
        fig = plot_thing(histogram[:, ::10j], self.title, "GeV")
        fig.savefig(output_file)


class EtaToPlot(ThingToPlot):
    """
    Plot the eta of a given lepton. Only the lepton with the given index will be plotted.

    No check is done to ensure the given lepton is present. Non-present leptons will result in a crash.
    """

    def __init__(self, title: str, lepton_name: str, index: int):
        """
        Parameters:
            title (str): The title of the overall plot.
            lepton_name (str): The name of the lepton. This should match a key in events.
            index (int): The index of the targeted lepton.
        """
        super().__init__(title=title)
        self.lepton_name = lepton_name
        self.index = index

    def create_histogram(self) -> hist.Hist:
        axis = hist.axis.Regular(
            500,
            -5,
            5,
            name="eta",
            label=self.title,
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
            dataset=dataset,
            eta=events[self.lepton_name].eta[:, self.index],
            weight=weights,
        )

        return histogram

    def plot_histogram(self, histogram: hist.Hist, output_file: str) -> None:
        fig = plot_thing(histogram[:, ::10j], self.title, "Radians")
        fig.savefig(output_file)


class DileptonMassToPlot(ThingToPlot):
    """
    Plot the dilepton mass of two leptons. The order doesn't matter due to the cos and cosh functions being even.

    No check is done to ensure the given lepton is present. Non-present leptons will result in a crash.
    """

    def __init__(
        self,
        title: str,
        first_lepton_name: str,
        first_lepton_index: int,
        second_lepton_name: str,
        second_lepton_index: int,
    ):
        """
        Parameters:
            title (str): The title of the overall plot.
            first_lepton_name (str): The name of the first lepton. This should match a key in events.
            first_lepton_index (int): The index of the first lepton.
            second_lepton_name (str): The name of the second lepton. This should match a key in events.
            second_lepton_index (int): The index of the second lepton.
        """
        super().__init__(title=title)
        self.first_lepton_name = first_lepton_name
        self.first_lepton_index = first_lepton_index
        self.second_lepton_name = second_lepton_name
        self.second_lepton_index = second_lepton_index

    def create_histogram(self) -> hist.Hist:
        axis = hist.axis.Regular(
            500,
            0,
            1000,
            name="mass",
            label=self.title,
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
        obj_1 = events[self.first_lepton_name][:, self.first_lepton_index]
        obj_2 = events[self.second_lepton_name][:, self.second_lepton_index]
        mass = np.sqrt(
            2
            * obj_1.pt
            * obj_2.pt
            * (np.cosh(obj_1.eta - obj_2.eta) - np.cos(obj_1.phi - obj_2.phi))
        )

        histogram.fill(
            dataset=dataset,
            mass=mass,
            weight=weights,
        )

        return histogram

    def plot_histogram(self, histogram: hist.Hist, output_file: str) -> None:
        fig = plot_thing(histogram[:, ::10j], self.title, "GeV")
        fig.savefig(output_file)

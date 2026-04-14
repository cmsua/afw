"""
A utility module containing commonly-plotted things
"""

import os
from enum import Enum

import awkward as ak
import hist
import mplhep as hep

from .objects import MicroProcessorABC, Stage
from .utils_internal import slugify


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


class AxisType(Enum):
    """
    Type of axis
    """

    DISCRETE = "discrete"
    CONTINUOUS = "continuous"
    BOOLEAN = "boolean"
    STRING = "string"


class AxisParameters:
    """
    Parameters for any given axis of a histogram
    """

    def __init__(
        self,
        name: str,
        label: str,
        axis_type: AxisType,
        fetch_data: callable,
        bin_values: list = None,
        n_bins: int = None,
        low_bin: int = 0,
        high_bin: int = None,
    ):
        """
        Args:
            name (str): The internal name of the axis in the histogram
            label (str): The label on the x-axis of the chart
            axis_type (str): A given ``AxisType``
            fetch_data (Callable): Given ``events`` and ``accumulator``, returns the values to be plotted. This should not include the ``where`` filter if present.
            n_bins (int, default None): If ``axis_type == AxisType.CONTINUOUS``, the number of bins present
            low_bin (int, default None): If ``axis_type == AxisType.CONTINUOUS``, the lower bound for bins
            high_bin (int, default None): If ``axis_type == AxisType.CONTINUOUS``, the upper bound for bins
            bin_values (list, default None): If ``axis_type == AxisType.DISCRETE``, the possible values present for each bin
        """
        self.name = name
        self.label = label
        self.axis_type = axis_type
        self.fetch_data = fetch_data
        self.bin_values = bin_values
        self.n_bins = n_bins
        self.low_bin = low_bin
        self.high_bin = high_bin
        pass

    def build_axis(self):
        """Build the given axis at runtime"""
        if self.axis_type == AxisType.CONTINUOUS:
            if self.bin_values is not None:
                return hist.axis.Variable(
                    self.bin_values, name=self.name, label=self.label
                )
            else:
                return hist.axis.Regular(
                    self.n_bins,
                    self.low_bin,
                    self.high_bin,
                    name=self.name,
                    label=self.label,
                )
        elif self.axis_type == AxisType.DISCRETE:
            return hist.axis.IntCategory(
                self.bin_values, name=self.name, label=self.label
            )
        elif self.axis_type == AxisType.BOOLEAN:
            return hist.axis.Boolean(name=self.name, label=self.label)
        elif self.axis_type == AxisType.STRING:
            return hist.axis.StrCategory(
                [], name=self.name, label=self.label, growth=True
            )


DATASET_AXIS = AxisParameters(
    name="dataset",
    label="Process",
    axis_type=AxisType.STRING,
    fetch_data=lambda events, **kwargs: events.metadata["shortName"],
)


class HistogramParameters:
    """Represents a histogram with any given number of axis"""

    def __init__(
        self, name: str, axis: list[AxisParameters], predicate: callable = None
    ):
        """
        Args:
            name (str): The internal name of the histogram
            axis (list[AxisParameters]): Parameters for each axis of the histogram
            predicate (callable, default None): If present, the condition required for this histogram to be filled
        """
        self.name = name
        self.axis = axis
        self.predicate = predicate

    def fill_histogram(
        self,
        events: ak.Array,
        weight: ak.Array = None,
        **kwargs: dict,
    ) -> hist.Hist:
        histogram = hist.Hist(
            *[axis.build_axis() for axis in self.axis],
            storage=hist.storage.Weight(),
        )

        parameters = {}
        for axis in self.axis:
            parameters[axis.name] = axis.fetch_data(events, **kwargs)

        histogram.fill(**parameters, weight=weight)
        return histogram

    def plot_histogram(
        self, histogram: hist.Hist, output_file: str, metadata: dict
    ) -> None:
        raise NotImplementedError()


class SingleAxisHistogramParameters(HistogramParameters):
    def __init__(self, **kwargs: dict):
        super().__init__(
            name=kwargs["name"], axis=[DATASET_AXIS, AxisParameters(**kwargs)]
        )

    def plot_histogram(
        self, histogram: hist.Hist, output_file: str, metadata: dict
    ) -> None:
        data_fields = [key for key, val in metadata.items() if val.get("isData", True)]
        mc_keys = [field for field in histogram.axes[0] if field not in data_fields]

        # Filter - some categories get ALL events filtered and thus aren't present in the histograms
        data_fields = [field for field in data_fields if field in histogram.axes[0]]
        mc_keys = [field for field in mc_keys if field in histogram.axes[0]]

        data = histogram[data_fields, :][sum, self.rebin_slice]

        signal_keys = [key for key, val in metadata.items() if val.get("signal", False)]
        stacked_keys = [key for key in mc_keys if key not in signal_keys]
        stacked_histos = [histogram[key, self.rebin_slice] for key in stacked_keys]

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
                histogram[signal_key, self.rebin_slice],
                ax=ax_main,
                label=signal_key,
                color="#000000",
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
        ax_main.set_title(self.name)

        # Label
        hep.cms.label(
            "Preliminary", data=True, ax=ax_main, year="2022EE", lumi="26.7", com=13.6
        )

        fig.savefig(output_file)


class FillHistograms(MicroProcessorABC):
    """
    Fills a given number of histograms
    """

    def __init__(
        self,
        histograms: list[HistogramParameters],
        fill_extra_parameters: callable = None,
    ):
        """
        Args:
            histograms (list[HistogramParameters]): A list of all histograms to fill
            fill_extra_parameters (callable, default None): Accepts ``events``, ``accumulator``, and returns values to pass to histograms during filling
        """
        self.histograms = histograms
        self.fill_extra_parameters = fill_extra_parameters

    def process(self, events: ak.Array, accumulator: dict) -> tuple[ak.Array, dict]:
        if self.fill_extra_parameters is not None:
            extra_parameters = self.fill_extra_parameters(events, accumulator)
        else:
            extra_parameters = {}

        extra_parameters["accumulator"] = accumulator

        result = {}
        for histogram in self.histograms:
            # If predicate is present and returns false, skip
            if histogram.predicate is not None and not histogram.predicate(events):
                continue

            try:
                result[histogram.name] = histogram.fill_histogram(
                    events, **extra_parameters
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed filling histogram {histogram.name} with exception", e
                )

        accumulator["plots"] = result
        return events, accumulator


class SaveHistograms(Stage):
    """
    Saves a given number of histograms
    """

    def __init__(
        self,
        name: str,
        histograms: list[HistogramParameters],
        dataset: dict,
        accumulator: callable,
    ):
        super().__init__(name=name)
        self.histograms = histograms
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

        for histogram in self.histograms:
            output_file = os.path.join(
                plot_dir, f"{slugify(histogram.name)}.{extension}"
            )
            histogram.plot_histogram(plots[histogram.name], output_file, metadata)

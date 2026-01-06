from coffea.processor import ProcessorABC
from coffea.analysis_tools import Weights

from .objects import AnalysisConfig

import awkward as ak


lumi22EE = 26.6717 * 1e3


class MyProcessor(ProcessorABC):
    def __init__(self, config: AnalysisConfig, skimmed: bool = False) -> None:
        self.skimmed = skimmed
        self.config = config

    def process(self, events):
        # Do definition and preselection if needed
        if not self.skimmed:
            events = self.config.define_objects(events)
            events = self.config.preselect_events(events)

        # Do selection
        events = self.config.select_events(events)

        ### Generate results object
        extra_args = self.config.augment_events(events)
        if extra_args is None:
            extra_args = {}

        ## Weights
        weights = Weights(len(events))
        if "isData" in events.metadata and events.metadata.get("isData", False):
            weights.add("nominal", ak.ones_like(events.event))
        else:
            # weights.add("genWeight", events.genWeight)
            weights.add(
                "xsec",
                ak.ones_like(events.genWeight)
                * lumi22EE
                * events.metadata["xsec"]
                / events.metadata["nevents"],
            )

        weights = weights.weight()

        ## Fill histograms
        dataset = events.metadata["shortName"]

        result = {}
        for thing in self.config.get_things_to_plot():
            histogram = thing.create_histogram()
            result[thing.title] = thing.fill_histogram(
                histogram, events, dataset, weights, **extra_args
            )

        return result

    def postprocess(self, accumulator):
        pass

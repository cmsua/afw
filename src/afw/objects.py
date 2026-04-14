"""
This file contains the bare-bones essentials of any analysis.
"""

import abc
import awkward as ak


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


__all__ = [MicroProcessorABC, Stage]

import operator
from collections.abc import MutableMapping, MutableSet
from typing import Protocol, TypeVar, Union, runtime_checkable

import awkward as ak
from coffea.processor import ProcessorABC

from .objects import AnalysisStep

# The below iadd function and utilities are directly copied from coffea and modified to avoid the copy package
T = TypeVar("T")


@runtime_checkable
class Addable(Protocol):
    def __add__(self: T, other: T) -> T: ...


Accumulatable = Union[Addable, MutableSet, MutableMapping]


def iadd(a: Accumulatable, b: Accumulatable) -> Accumulatable:
    """Add two accumulatables together, assuming the first is mutable"""
    if isinstance(a, Addable) and isinstance(b, Addable):
        return operator.iadd(a, b)
    elif isinstance(a, MutableSet) and isinstance(b, MutableSet):
        return operator.ior(a, b)
    elif isinstance(a, MutableMapping) and isinstance(b, MutableMapping):
        if not isinstance(b, type(a)):
            raise ValueError(
                f"Cannot add two mappings of incompatible type ({type(a)} vs. {type(b)})"
            )
        lhs, rhs = set(a), set(b)
        # Keep the order of elements as far as possible
        for key in a:
            if key in rhs:
                a[key] = iadd(a[key], b[key])
        for key in b:
            if key not in lhs:
                # This is modified to not use copy!
                a[key] = b[key]
        return a
    raise ValueError(
        f"Cannot add accumulators of incompatible type ({type(a)} vs. {type(b)})"
    )


class ModifyingProcessor:
    """
    A processor that can handle a chain of steps
    """

    def __init__(
        self,
        chain: list[str],
        steps_dict: dict[str, AnalysisStep],
        prev_accumulator: dict = {},
    ) -> None:
        """
        Args:
            chain (list[str]): The list of steps to run (by name) in order
            steps_dict (dict[str, AnalysisStep]): A mapping from step name to AnalysisStep objects
            prev_accumulator (dict): The accumulator from running any previous chains
        """
        self.prev_accumulator = prev_accumulator
        self.steps_dict = steps_dict
        self.chain = chain

    def process(self, events: ak.Array) -> tuple[ak.Array, dict, dict]:
        """
        Args:
            events (ak.Array): The events from any previous chain

        Returns:
            tuple[ak.Array, dict, dict]: Returns the events for future chains, the cumulative accumulator, and a per-step accumulator
        """
        breakdown = {}
        out = {}
        for step_name in self.chain:
            events, accumulator = self.steps_dict[step_name].process(
                events, self.prev_accumulator
            )

            breakdown[step_name] = accumulator
            out = iadd(out, accumulator)
        return events, {"out": out, "breakdown": breakdown}

    def postprocess(self, result: dict) -> dict:
        """
        Args:
            result (dict): The result address from ``process()``

        Returns:
            dict: The post-processed result
        """
        for step_name in self.chain:
            step = self.steps_dict[step_name]
            if not hasattr(step, "postprocess") or not callable(step.postprocess):
                continue
            
            step_result = step.postprocess(result["out"])
            if step_result is not None:
                result["out"] = step_result
                result["breakdown"][step_name] = step.postprocess(
                    result["breakdown"][step_name]
                )
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

import os
import pickle

from .dataset import to_skimmed


## Used to load things at runtime
def load_from_skims(task_name: str, dataset: dict, **kwargs: dict):
    """
    Utility method. Given a definitions file, returns a runnable that, when called, will generate a dataset definitions from a previous run's skims using ``skim_dir`` and ``task_name``

    Args:
        task_name (str): The task to load frim
        defs_file (str): The definitions yaml file

    Returns:
        callable: A callable that requires ``skim_dir`` at runtime
    """
    return lambda skim_dir, **kwargs2: to_skimmed(
        dataset, os.path.join(skim_dir, task_name)
    )


def load_from_pickle(task_name: str, **kwargs: dict):
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

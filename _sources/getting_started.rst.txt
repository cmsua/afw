Getting Started
=======================================

===============================
First Steps: Setup
===============================

The traditional CMS workflow can be broken into the following stages:

- Data Discovery, in which lists of root files are associated with given metadata values
- Object Definition, in which electrons, muons, jets, or other objects are filtered based on a given criteria
- Event Preselection, in which events are filtered based on a fairly loose set of criteria. These criteria are expected to remain fixed throughout the analysis
- Event Selection, in which events are further filtered with a far-stricter set of criterion
- Plotting, in which a series of histograms are created, filled with various data, and saved

In this framework, the entire analysis can be represented in a single file, consisting of an instance of a class extending :class:`configs.base.AnalysisConfig`. This is an abstract class, meant to represent any given analysis. It consists of the following methods:

- ``__init__(name)`` accepts one parameter, which is the name of the directory in which any results will be saved.
- ``get_dataset(xcache_host)`` returns a dataset definition populated with a list of remote root files, using the given xcache host. By convention, the metadata provided should contain ``shortName``, ``isData``, and optionally ``nevents`` and ``xsec``. More info may be found on the datasets page. #TODO
- ``define_objects(events)`` is the analysis's object definition method. It is expected to return a version of the ``events`` parameter after redefining objects as-needed. This modification is presumed to be destructive.
- ``preselect_objects(events)`` is the first of two object selection functions. Given events with objects defined as per the above method, it is expected to return a version of the ``events`` parameter after event preselection. While intended to be a destructive operation, many preselection functions are non-destructive.
- ``select_objects(events)`` is the second object selection functions. Given events passing preselection and with objects defined as per the above methods, it is expected to return a version of the ``events`` parameter after event selection. While intended to be a destructive operation, many preselection functions are non-destructive.

To promote code reuse as much as possible, the histogramming and plotting code has been abstracted. The :class:`configs.base.ThingToPlot` class is the fundamental object in plotting, and consists of the following methods:

- ``__init__(name)`` creates the object. Though many subclasses require fewer or more parameters, the ``name`` parameter is both the key in which the histogram is saved in the output file as well as the title of the generated plot
- ``create_histogram`` is intended to create a :class:`hist.Histogram` object with any given amount of axis, though it may return any object that can be pickled and reduced.
- ``fill_histogram(histogram, events, dataset, weights, **kwargs)`` is designed to apply the data provided to the object created in ``create_histogram``. This is presumed to be a destructive method on the histogram object, but non-destructive on events, dataset, weights, and other ``kwargs``.
- ``plot_histogram(histogram, output_file)`` is designed to save a copy of the filled and reduced histogram generated above. Note that this is done in addition to a pickled version of the histogram object, making this function ideal for saving large (and recreatable) objects such as graphics.

To interface with the plots being generated, the AnalysisConfig class contains two methods:

- ``augment_events(events)`` is designed to create additional information that may be shared across several plots, such as the total number of events, or masks to be shared across multiple plots. The result of this function will be passed to ``fill_histogram`` as keyword arguments.
- ``get_things_to_plot`` is expected to return a list of ThingToPlot objects.


===============================
Running A Workflow
===============================

To run a configuration, it must first be added to the :class:`utils` class's ``get_config`` method, which will enable its use in command-line utilities.

Then, the ``main.py`` file can be used to run your analysis. The following CLI parameters may be of interest:

- ``-c, --channel`` should be set to your analysis config.
- ``-w, --max-files-per-category`` may be used to limit the analysis to a set number of files. This **will** change ``nevents`` for MC files if the dataset utilities are used, but not reweight data files. As such, this should only be used with a :class:`dask.distributed.LocalCluster` for validating Python code.
- ``-s, --no-skim`` should be set accordingly. While skims are not yet present, and as such, the processor will default to raw files, it is still best-practice to include this flag to avoid running on incomplete skims later-on.

Once the workflow is completed, it may be necessary to regenerate plots, such as to change the rebinning options. The ``plotter.py`` file may be used in this case, which will rerun only the ``plot_histogram`` function for each ``ThingToPlot``.

===============================
Skimming
===============================

Adapting an AnalysisConfig for skimming is fairly straight-forward. The following modifications will need to be done:

- ``define_objects(events)`` and ``preselect_objects(events)`` must be adapted in a way that allows them to work with :class:`dask_awkward.Array` objects instead of :class:`awkward.Array` objects. This should be fairly straight-forward to implement.
- A new ``minify(events)`` function should be introduced to the configuration. This method is expected to trim unneeded branches from the events object.

Then, the ``skim.py`` file may be used to create skims. Note that the skims are saved on the worker nodes - if the workers do not have access to the local filesystem, the skims will need to be saved to a shared filesystem such as EOS. **This is currently non-functional.**

Additionally, the skimming code supports making skims from a baseline skim. **This is currently non-functional.**

# type: ignore

### Standard imports ###

import os
import timeit
import pickle
import logging

from datetime import timedelta
from concurrent.futures import ProcessPoolExecutor as Pool

### Local imports ###

from .utilities import unpickler, grouper, filter_by_ext
from .filterbank_no_rfifind import Filterbank

# from .filterbank import Filterbank


class PipelineWorker(object):

    """Function-like object that takes a single date as an argument and processes all
    filterbank files in it. This is to circumvent a limitation of the map() method of the
    "ProcessPoolExecutor" class of "concurrent.futures" module which requires the mapped
    function to:

    - take just one argument.
    - be pickle-able, and therefore be defined at the top level of a module.

    It borrows this limitation from the "multiprocessing.Pool" class, since it serves as
    wrapper for that class. However, it is used here, instead of the "multiprocessing"
    module, since it allows launching parallel processes from within each of its parallel
    processes, a feature required for this pipeline to function.
    """

    def __init__(self, config):

        """Create a PipelineWorker object.

        Parameters:
        -----------
        config: CfgManager
            A CfgManager object storing the current configuration of the pipeline.
        """

        self.config = config

    def raw_or_fil(self, path):

        """Function to determine whether the pipeline should start with creating filterbank
        files from raw data or are filterbank files already present. Returns the corresponding
        extension ("*.raw" or "*.fil") depending on whether the former or latter is true,
        otherwise returns None, in which case we have a problem since there is no data to analyse.

        Parameters:
        -----------
        path: str or Path-like
            The absolute path to the directory where the data files are supposed to be.
        """

        RAW_FILES = filter_by_ext(path, extension=".raw")

        try:

            RAW_FILES.__next__()

        except StopIteration:

            try:
                FIL_FILES = filter_by_ext(path, extension=".fil")
                FIL_FILES.__next__()

            except StopIteration:
                return None

            else:
                EXT = ".fil"
                return EXT

        else:

            EXT = ".raw"
            return EXT

    def make_dirs(self, beam_rfi_path, beam_state_path):

        """Make the appropriate directories.

        Parameters:
        -----------
        beam_rfi_path: str or Path-like
            The absolute path to the parent directory
            where all RFI masks are stored.
        beam_state_path: str or Path-like
            The absolute path to the parent directory
            where all outputs are stored.
        """

        os.makedirs(beam_rfi_path, exist_ok=True)
        os.makedirs(beam_state_path, exist_ok=True)

    def configure_logger(self, log_path, level=logging.INFO):

        """ Configure the logger for this filterbank file. """

        _logger_name_ = self.date
        self.logger = logging.getLogger(_logger_name_)
        self.logger.setLevel(level)

        formatter = logging.Formatter(
            fmt="%(asctime)s || %(name)s || %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        if not self.logger.handlers:

            handler = logging.FileHandler(log_path)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def cumulative_walltime(self):

        """Returns the total amount of time spent in processing the whole
        date directory, returned in the appropriate format.
        """

        total_time = timedelta(seconds=self._cumulative_walltime)
        return total_time

    def __call__(self, date):

        """Processes all the filterbank files in a particular date directory.

        Parameters:
        -----------
        date: str
            The date for which data is to be processed.
        """

        #####################################################################################

        self.date = date
        self.date_path = os.path.join(self.config.store_path, self.date)
        self.beam_rfi_path = os.path.join(self.config.rfi_path, self.date)
        self.beam_state_path = os.path.join(self.config.state_path, self.date)

        #####################################################################################

        # Make the appropriate directories.

        self.make_dirs(self.beam_rfi_path, self.beam_state_path)

        # Path to the log file.

        self.log_file = os.path.join(self.beam_state_path, "{}.log".format(self.date))

        # Configure the logger.

        self.configure_logger(self.log_file)

        # Determine where to start from, raw data or filterbank data.

        EXT = self.raw_or_fil(self.date_path)

        # Start processing all files in the given date directory.

        if EXT:

            self.logger.info("Start processing data for date {:s}.".format(self.date))
            start_time = timeit.default_timer()

            _files_ = filter_by_ext(self.date_path, extension=EXT)

            #####################################################################################

            # Unpickle list of Meta objects and get the filenames from them.

            metalist = "./{}.history.log".format(self.date)
            _metalist_ = unpickler(metalist)

            # Decide which mode to open the hidden log file in based on whether it already exists
            # or not and filter the list of files accordingly and iterate through them to process
            # the remaining files. If the hidden log doesn't exist, just return a list of all files.

            try:

                next(_metalist_)

                # The pipeline has run before. Restore previous state.

                _filelist_ = [meta["fname"] for meta in _metalist_]
                FILES = [f for f in _files_ if f.name not in _filelist_]

            except StopIteration:

                # Running from scratch. Initialise the list of files.

                FILES = [_file_ for _file_ in _files_]

                # Before processing starts, pickle the current pipeline configuration into
                # the hidden log file, before it is too late.

                with open(metalist, "wb+") as _mem_:
                    pickle.dump(self.config, _mem_)

            #####################################################################################

            # Path to the human readable version of the log file.

            filelist = os.path.join(self.beam_state_path, "filelist")

            #####################################################################################

            for FILE in FILES:

                with open(metalist, "ab") as _mem_, open(filelist, "a") as _list_:

                    # Initialise the filterbank file and process it.

                    FIL_NAME = FILE.name
                    FIL_PATH = FILE.path

                    filterbank = Filterbank(
                        FIL_NAME,
                        FIL_PATH,
                        self.date,
                        self.beam_rfi_path,
                        self.beam_state_path,
                        self.config,
                    )
                    filterbank.process()

                    # Pickle Metadata object corresponding to the filterbank file
                    # just processed into a hidden log file and store the name of
                    # the file in a human readable filelist.

                    pickle.dump(filterbank.metadata, _mem_)
                    _list_.write("{}\n".format(FIL_NAME))

                    # Delete the filterbank file if we started with a "*.raw" file.
                    # Otherwise leave it be.

                    if (EXT == ".raw") and (filterbank.path.endswith(".fil")):
                        os.remove(filterbank.path)

            #####################################################################################

        end_time = timeit.default_timer()
        self._cumulative_walltime = end_time - start_time
        self.logger.info("Done processing date {}.".format(self.date))
        self.logger.info("Total processing time: {}".format(self.cumulative_walltime()))


class PipelineManager(object):

    """ Class that handles the parallelisation of the pipeline across multiple dates. """

    def __init__(self, config):

        """Create a PipelineManager object.

        Parameters:
        -----------
        config: CfgManager
            A CfgManager object storing the current configuration of the pipeline.
        """

        self.config = config
        self.make_dirs(self.config.rfi_path, self.config.state_path)

        self.Worker = PipelineWorker(config)

    def make_dirs(self, rfi_path, state_path):

        os.makedirs(rfi_path, exist_ok=True)
        os.makedirs(state_path, exist_ok=True)

    def process(self):

        """Processes several dates in parallel using the "concurrent.futures"
        module's "ProcessPoolExecutor".
        """

        with Pool() as pool:
            [p for p in pool.map(self.Worker, self.config.analysis_dates)]

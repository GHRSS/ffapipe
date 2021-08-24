# type: ignore

### Standard imports ###

import os
import re
import shlex
import timeit
import logging
import subprocess

from datetime import datetime
from datetime import timedelta
from concurrent.futures import wait
from concurrent.futures import ProcessPoolExecutor as Pool

### Non-standard imports ###

import fileinput
import numpy as np
import matplotlib

from pytz import timezone
from datetime import datetime
from astropy.time import Time
from astropy.coordinates import SkyCoord

from riptide.pipelines import Candidate
from riptide.pipelines import CandidatePlot

### Local imports ###

from .utilities import grouper, filter_by_ext, count_files, step_iter, make_pdf
from .metas import Meta


class Filterbank(object):

    """ Container for a filterbank file. """

    def __init__(self, name, path, date, beam_rfi_path, beam_state_path, config):

        """Create a new Filterbank object, a container for filterbank data. All
        functions to create, manipulate and process filterbank data reside here.
        Methods of this class form the main processing part of the pipeline.

        Parameters:
        -----------
        name: str
            Name of the filterbank file.
        path: str or Path-like
            Absolute path to the filterbank file.
        beam_rfi_path: str or Path-like
            Absolute path to the parent directory where
            all RFI masks are to be stored.
        beam_state_path: str or Path-like
            Absolute path to the parent directory where
            all output files are to be stored.
        config: CfgManager
            CfgManager object.
            Used to access the current pipeline configuration.
        """

        # Store the current pipeline configuration.

        self.config = config

        # Date on which the filterbank file was generated.

        self.date = date

        # Information about the input: how is it named and where is it coming from.

        self.name = name
        self.path = path

        # Information about the output: how is it named and where is it going.

        self.output_name = os.path.splitext(self.name)[0]

        self.output_rfi_path = os.path.join(beam_rfi_path, self.output_name)
        self.timeseries_path = os.path.join(beam_state_path, self.output_name)

        self.cands_path = os.path.join(self.timeseries_path, "candidates")
        self.fold_prf_path = os.path.join(self.timeseries_path, "folded_profiles")
        self.arv_prf_path = os.path.join(self.timeseries_path, "archived_profiles")

        # Configure the logger.

        self.configure_logger()

        # Store some attributes for later.

        self._attrs_ = {"fname": self.name}

    @property
    def proc_dm_trials(self):
        """ Number of "processed" DM trials. """
        return count_files(self.timeseries_path, extension=".inf")

    @property
    def num_candidates(self):
        """ Number of output candidates generated. """
        return count_files(self.cands_path, extension=".h5")

    @property
    def num_fold_prfs(self):
        """ Number of folded profiles generated. """
        return count_files(self.fold_prf_path, extension=".pfd")

    @property
    def num_arv_prfs(self):
        """ Number of folded profiles archived """
        return count_files(self.arv_prf_path, extension=".archive")

    def cumulative_walltime(self):

        """Returns the total amount of time spent in processing each filterbank
        file, in the appropriate format.
        """

        total_time = timedelta(seconds=self._cumulative_walltime)
        return total_time

    def configure_logger(self):

        """ Configure the loggers for this filterbank file. """

        # Inherit logger from the date to which this observation belongs.

        _log_name_ = ".".join([self.date, self.output_name])

        self.logger = logging.getLogger(_log_name_)

        # Add another handler to this logger to log process information to
        # a hidden log file. This will later be used for updating the user
        # about the progress of this file.

        _formatter_ = logging.Formatter(fmt="%(message)s")

        _handler_ = logging.FileHandler("./{}.PIDS.log".format(self.date), mode="w+")

        _handler_.setFormatter(_formatter_)

        self.logger.addHandler(_handler_)

    def calc_time_params(self):

        """Get the date and time for the observation from the associated
        "*.timestamp" file and calculate the equivalent UTC date/time and
        MJD for it.

        Used in the creation of the header file ("*.hdr").
        """

        self.logger.info("Calculating date/time parameters.")

        TMSTMP_PATH = self.path.replace(".raw", ".raw.timestamp")

        with open(TMSTMP_PATH, "r") as instream:
            line = instream.readline()
            param = line.split()[7:][:-1]

        year = param[0]
        month = param[1]
        day = param[2]
        hour = param[3]
        minute = param[4]
        sec = param[5]
        tstmp = param[6]

        date = year + "-" + month + "-" + day
        time = hour + ":" + minute + ":" + sec

        IST = timezone("Asia/Kolkata")
        fmt = "%Y-%m-%d %H:%M:%S"

        ist = IST.localize(datetime.strptime(date + " " + time, fmt))
        utc = (
            ist.astimezone(timezone("UTC")).strftime(fmt)
            + "."
            + tstmp.replace("0.", "")
        )
        utc_date = utc.split(" ")[0]
        utc_time = utc.split(" ")[1]
        utc_obj = Time(utc, precision=9)

        self.mjd = int(utc_obj.mjd)
        self.utc_date = (
            datetime.strptime(utc_date, "%Y-%m-%d")
            .strftime("%d/%m/%Y")
            .replace("-", "/")
        )
        self.utc_time = utc_time

        self.logger.info("Date/time parameters calculated.")

    def get_coords(self):

        """Get the coordinates by searching for the source ID of the given
        observation in the source lists of the GHRSS survey and returning them.
        """

        self.logger.info("Searching for source in source lists. Getting coordinates.")

        source_lists = [
            files
            for files in filter_by_ext(self.config.sources_path, extension=".list")
        ]
        sources = fileinput.input(source_lists)

        for source in sources:

            ID = source.split()[0]

            if self.output_name.find(ID) != -1:

                self.source_id = ID
                src_raj = source.split()[1]
                src_decj = source.split()[2]

        self.logger.info("Got coordinates.")

        return src_raj, src_decj

    def store_coords(self):

        """Store the coordinates of the given observation in the appropriate
        format for the creation of the header file. Uses the "SkyCoord" class
        from the "astropy.coordinates" module to store and manipulate the
        coordinates.
        """

        self.logger.info("Storing coordinates in proper format.")

        ra_str, dec_str = self.get_coords()

        coords = SkyCoord(ra_str, dec_str)

        ra_hour = str(int(coords.ra.hms.h))
        ra_minute = str(int(coords.ra.hms.m))
        ra_second = str(round(coords.ra.hms.s, 2))

        dec_deg = str(int(coords.dec.dms.d))
        dec_minute = str(int(coords.dec.dms.m))
        dec_second = str(round(coords.dec.dms.s, 2))

        self.raj = ra_hour + ":" + ra_minute + ":" + ra_second
        self.decj = dec_deg + ":" + dec_minute + ":" + dec_second

        self.coords_str = ", ".join([str(self.raj), str(self.decj)])

        self.logger.info("Coordinates stored.")

    def fill_blank(self, search_str, fill_str, filler):

        """Fill a blank in the header template file. Takes a search string
        for the field to be filled, a line in the header file, and the filler
        variable. Fills the blank if the line matches the search string,
        otherwise returns it without any change.

        Parameters:
        -----------
        search_str: str
            The field to be searched.
        fill_str: str
            The line in the file to be filled.
        filler: int, float or str
            The filler variable.
        """

        regex = re.compile(search_str)

        if re.match(regex, fill_str):
            filled_str = " ".join([fill_str, str(filler)])
        else:
            filled_str = fill_str

        return filled_str

    def fill_template(self, template_path):

        """Fill in the template file for the header and write a header file
        for the filterbank file. Also creates a symbolic link between the
        "*.raw" file and a "*.raw.gmrt_dat" file, for use by GPTool or the
        "filterbank" script.
        """

        self.logger.info("Start filling in header template.")

        # Get appropriate header template, according to backend.

        header_template = os.path.join(
            template_path, ".".join(["template", self.config.backend, "gmrt_hdr"])
        )

        template = []

        with open(header_template, "r") as instream:
            for line in instream:
                template.append(line.strip())

        # The blank fields in the template and the variables they are to be filled with.

        blanks = ["Date", "MJD", "UTC", "Source", "Coordinates"]

        fillers = [
            self.utc_date,
            self.mjd,
            self.utc_time,
            self.source_id,
            self.coords_str,
        ]

        # Fill the template and write it to file in one go.

        with open(self.path.replace(".raw", ".raw.hdr"), "w+") as outstream:
            for string in template:
                for filler, blank in zip(fillers, blanks):
                    string = self.fill_blank(blank, string, filler)

                outstream.write(string + "\n")

        self.logger.info("Header constructed.")
        self.logger.info(
            "Making symbolic link between the '*.raw' file and a "
            "'*.raw.gmrt_dat' file."
        )

        # Create a symbolic link between the "*.raw" file and a "*.raw.gmrt_dat" file.

        cmd_symb = "ln -s {}".format(
            " ".join([self.path, self.path.replace(".raw", ".raw.gmrt_dat")])
        )
        arg_symb = shlex.split(cmd_symb)
        proc_symb = subprocess.Popen(arg_symb)
        proc_symb.wait()

    def gmrt_psr_tool(self, template_path):

        """Run GPTool. The GMRT Pulsar Tool is RFI mitigation module built in-house at NCRA
        for removing RFI directly from raw data files.

        Parameters:
        -----------
        template_path: str or Path-like
            The absolute path to all template files.
            Filtered by backend.
        """

        self.logger.info("Starting RFI mitigation using GPTool.")

        # Copy appropriate GPTool template, according to backend.

        cmd_cp = "cp {} {}".format(
            os.path.join(
                template_path, ".".join(["gptool", self.config.backend, "in"])
            ),
            "./gptool.in",
        )
        arg_cp = shlex.split(cmd_cp)
        proc_cp = subprocess.Popen(arg_cp)
        proc_cp.wait()

        # Run GPTool.
        if self.config.backend == "GWB":
            cmd_gptool = (
                "/Data/jroy/softwares/gptool_ver4.4.5.FRB/gptool -f {} -o {} -m {} -nodedisp -zsub "
                "-t 2"
            ).format(self.path, os.path.dirname(self.path), "32")
        else:
            cmd_gptool = (
                "/Data/jroy/softwares/gptool_ver4.4.5.PSR/gptool -f {} -o {} -m {} -nodedisp "
                "-t 2"
            ).format(self.path, os.path.dirname(self.path), "32")

        #        cmd_gptool = ('/data/jroy/softwares/gptool_ver4.2.1/gptool -f {} -o {} -m {} -nodedisp '
        #                      '-t {}').format(self.path,
        #                                      os.path.dirname(self.path),
        #                                      '32',
        #                                      self.config.cores)
        arg_gptool = shlex.split(cmd_gptool)
        proc_gptool = subprocess.Popen(
            arg_gptool, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        proc_gptool.wait()

        self.logger.info("Done with RFI mitigation.")
        self.logger.info(
            "Deleting previous symbolic link and making new one between "
            "'*.raw.gpt' and '*.raw.gmrt_dat' file."
        )

        # Delete previous symbolic link between "*.raw" and "*.raw.gmrt_dat" file.

        os.remove(self.path.replace(".raw", ".raw.gmrt_dat"))

        # Form new symbolic link between "*.raw.gpt" and "*.raw.gmrt_dat" file.

        cmd_symb = "ln -s {}".format(
            " ".join(
                [
                    self.path.replace(".raw", ".raw.gpt"),
                    self.name.replace(".raw", ".raw.gmrt_dat"),
                ]
            )
        )
        arg_symb = shlex.split(cmd_symb)
        proc_symb = subprocess.Popen(arg_symb)
        proc_symb.wait()

    def create_filterbank(self):

        """Create the filterbank file.

        Uses the in-built "filterbank" script.
        """

        self.logger.info("Start creating the filterbank file.")

        # Copy header file to current working directory.

        cmd_cp = "cp {} {}".format(
            self.path.replace(".raw", ".raw.hdr"),
            self.name.replace(".raw", ".raw.gmrt_hdr"),
        )
        arg_cp = shlex.split(cmd_cp)
        proc_cp = subprocess.Popen(arg_cp)
        proc_cp.wait()

        # Create filterbank file.

        cmd_filbnk = "filterbank {} > {}".format(
            self.name.replace(".raw", ".raw.gmrt_dat"),
            self.path.replace(".raw", ".fil"),
        )
        proc_filbnk = subprocess.Popen(
            cmd_filbnk, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        proc_filbnk.wait()

        self.logger.info(
            "Filterbank file created. "
            "Delete all GPTool related files and symbolic links."
        )

        # Delete ALL GPTool-associated files and the symbolic link.

        proc_rm_gpt_cwd = subprocess.Popen("rm -rf *gpt*", shell=True)
        proc_rm_gpt = subprocess.Popen(
            "rm -rf {}".format(os.path.join(os.path.dirname(self.path), "*gpt*")),
            shell=True,
        )
        os.remove(self.name.replace(".raw", ".raw.gmrt_dat"))
        os.remove(self.name.replace(".raw", ".raw.gmrt_hdr"))

    def zeroDM_filtering(self):

        """Do zero DM filtering on the filterbank file.

        Done only when the backend is GSB. Uses the in-built "zerodm" script.
        """

        if self.config.backend == "GSB":

            self.logger.info("Start zeroDM filtering, since backend is GSB.")

            cmd_zdm = "zerodm {} > {}".format(
                self.path.replace(".raw", ".fil"),
                self.path.replace(".raw", ".zeroDM.fil"),
            )
            proc_zdm = subprocess.Popen(
                cmd_zdm,
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            proc_zdm.wait()

            self.logger.info("Done with zeroDM filtering.")

    def make_fil(self):

        """ Create the filterbank file. """

        if self.name.endswith(".raw"):

            self.logger.info(
                "Starting creation of the filterbank file from "
                "the raw file, {}".format(self.name)
            )

            template_path = os.path.join(
                self.config.preprocessing_path, self.config.backend
            )

            # Header creation.

            self.calc_time_params()
            self.store_coords()
            self.fill_template(template_path)

            # RFI mitigation using GPTool and filterbank creation.

            self.gmrt_psr_tool(template_path)
            self.create_filterbank()

            # ZeroDM filtering. Done only if the backend is GSB.

            self.zeroDM_filtering()

            # Change the name and path extension to ".fil".
            # If zeroDM filtering is done, change the extension
            # to ".zeroDM.fil"

            if (self.config.backend == "GWB") or (self.config.backend == "SIM"):

                self.name = self.name.replace(".raw", ".fil")
                self.path = self.path.replace(".raw", ".fil")

            else:

                self.name = self.name.replace(".raw", ".zeroDM.fil")
                self.path = self.path.replace(".raw", ".zeroDM.fil")

    def make_dirs(self):

        """ Make all output directories. """

        self.logger.info("Make all appropriate directories.")

        os.makedirs(self.timeseries_path, exist_ok=True)
        os.makedirs(self.output_rfi_path, exist_ok=True)
        os.makedirs(self.cands_path, exist_ok=True)
        os.makedirs(self.fold_prf_path, exist_ok=True)
        os.makedirs(self.arv_prf_path, exist_ok=True)

        self.logger.info("Done making directories.")

    def load_header(self):

        """Load variables from the filterbank header.

        Uses the in-built "header" script and the "grep" and "awk" utilities
        to extract the header variables.
        """

        # Reading the header of the given filterbank file using the "header"
        # command and getting the required parameters by piping the output into
        # "grep"s for each parameter.

        self.logger.info("Extract all variables from the header.")

        cmd1 = "header " + self.path
        cmd2 = []
        cmd2.append("grep 'Frequency of channel 1'")
        cmd2.append("grep 'Channel bandwidth'")
        cmd2.append("grep 'Number of channels'")
        cmd2.append("grep 'Sample time'")
        cmd2.append("grep 'Observation length'")
        cmd3 = "awk -F: '{print $2}'"

        arg1 = shlex.split(cmd1)
        arg2 = []
        arg2 = [shlex.split(i) for i in cmd2]
        arg3 = shlex.split(cmd3)

        variables = []

        for i in range(len(cmd2)):
            p1 = subprocess.Popen(arg1, stdout=subprocess.PIPE)
            p2 = subprocess.Popen(arg2[i], stdin=p1.stdout, stdout=subprocess.PIPE)
            p3 = subprocess.Popen(arg3, stdin=p2.stdout, stdout=subprocess.PIPE)
            variables.append(float(p3.communicate()[0].decode("utf-8").split()[0]))

        self.BW = round(-1 * variables[1] * variables[2])  # Bandwidth.
        self.CFREQ = variables[0] - (self.BW / 2)  # Central frequency.
        self.NUMCHAN = variables[2]  # Number of channels.
        self.TSAMP = variables[3]  # Sampling time.
        self.OBSVT = variables[4]  # Length of the observation.

        self.logger.info("Done extracting variables.")

    def segmented_dedisp(self, dm_segment):

        """Dedisperse a segment of DM space using the "prepsubband" module.

        Parameters:
        -----------
        dm_segment: list
            A pair of start and end DM values across which dedispersion is
            to be carried out.
        """

        TSAMP = float(self.TSAMP) / 1e6
        NUMOUT = int(
            round((self.OBSVT * 60) / TSAMP / self.config.ddplan["DS"] / 1000) * 1000
        )

        cmd_dedisp = (
            "prepsubband -nobary -numout {} -lodm {} -dmstep {} -numdms {} -nsub {} "
            "-downsamp {} {} -o {}"
        ).format(
            NUMOUT,
            dm_segment,
            self.config.ddplan["dDM"],
            self.dm_trials,
            self.config.ddplan["nsub"],
            self.config.ddplan["DS"],
            self.path,
            os.path.join(self.timeseries_path, self.output_name),
        )
        arg_dedisp = shlex.split(cmd_dedisp)
        proc_dedisp = subprocess.Popen(
            arg_dedisp, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        proc_dedisp.wait()

    def para_dedisp(self):

        """Dedisperse the filterbank file. Parallelised by dividing the DM space into
        "segments": each segment is dedispersed separately. The number of segments is
        equal to the number of cores per node.
        """

        DM_lowest = self.config.ddplan["DM_lowest"]
        DM_highest = self.config.ddplan["DM_highest"]
        dDM = self.config.ddplan["dDM"]
        numDMs = self.config.ddplan["numDMs"]

        dm_sequence = np.arange(DM_lowest, DM_highest, dDM)
        segment_length = float(DM_highest - DM_lowest) / self.config.cores

        self.dm_trials = int(float(numDMs) / self.config.cores)
        self.dm_segments = [
            segments
            for segments in step_iter(
                dm_sequence, DM_lowest, DM_highest, segment_length
            )
        ]

        self.logger.info(
            "Starting dedispersion. Parallely running {:d} worker processes "
            "across {:d} DM trials.".format(len(self.dm_segments), numDMs)
        )
        with Pool() as pool:
            [p for p in pool.map(self.segmented_dedisp, self.dm_segments)]

        self.logger.info(
            "Done with dedispersion. "
            "Number of DM trials processed: {:d}".format(self.proc_dm_trials)
        )

        self._attrs_["proc_dm_trials"] = self.proc_dm_trials

    def fast_folding(self):

        """Search all dedispersed timeseries using the FFA. The FFA is implemented in
        Python through the RIPTIDE module. The main algorithm is written in C, to ensure
        high computational speeds.
        """

        self.logger.info(
            "Starting the FFA search on {:d} timeseries.".format(self.proc_dm_trials)
        )

        ffa_module_path = os.path.join(self.config.scripts_path, "pipeline.py")

        ffa_config_path = os.path.join(
            self.config.configurations_path, "ffa_config", "manager_config.yaml"
        )

        glob_pattern = os.path.join(self.timeseries_path, '"*.inf"')

        # Run the "pipeline.py" script.

        proc_ffa = subprocess.Popen(
            "python {} {} {} {}".format(
                ffa_module_path, ffa_config_path, glob_pattern, self.cands_path
            ),
            shell=True,
        )  # ,
        # stdout = subprocess.DEVNULL,
        # stderr = subprocess.DEVNULL)
        proc_ffa.communicate()

        self.logger.info(
            "Done with the FFA search. "
            "{:d} candidates generated.".format(self.num_candidates)
        )

        self._attrs_["num_candidates"] = self.num_candidates

    def plot_candidates(self):

        """Plots are created for all output candidates and compiled into one PDF file.
        Candidates are plotted in batches. Each batch has as many candidates as the
        number of cores available per node.
        """

        self.logger.info(
            "Plotting all candidates in batches of {:d}.".format(self.config.cores)
        )

        #

        matplotlib.use("Agg")

        # Disable the "too many figures open" warning raised by "matplotlib".

        matplotlib.pyplot.rcParams.update({"figure.max_open_warning": 0})

        candidates = []

        candidates = filter_by_ext(self.cands_path, extension=".h5")
        plot_batches = grouper(candidates, self.config.cores)

        plot_path = os.path.join(self.cands_path, "candidate_plts.pdf")
        images_path = os.path.join(self.cands_path, "png")

        os.makedirs(images_path, exist_ok=True)

        for plot_batch in plot_batches:

            for candidate in plot_batch:

                cand_plot_obj = CandidatePlot(Candidate.load_hdf5(candidate.path))
                cand_plot_obj.saveimg(
                    os.path.join(images_path, candidate.name.replace(".h5", ".png"))
                )

        self.logger.info(
            "All candidates plotted. "
            "Compile all plots into single PDF file. "
            "Delete all PNG files."
        )

        # Turn all PNG files into a single PDF and delete the "png" directory.

        make_pdf(images_path, plot_path)
        proc_rm = subprocess.Popen(
            "rm -rf {}".format(images_path),
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Delete all timeseries because we are done with them now.

        [
            os.remove(timeseries)
            for timeseries in filter_by_ext(self.timeseries_path, extension=".dat")
        ]

        # And their headers too.

        [
            os.remove(headers)
            for headers in filter_by_ext(self.timeseries_path, extension=".inf")
        ]

        self.logger.info("Done with plotting. Delete all timeseries.")

    def fold_candidate(self, cand_path):

        """ Fold an output candidate using the "prepfold" module. """

        cand = Candidate.load_hdf5(cand_path)
        cand_DM = cand.metadata.get("best_dm")
        cand_P = cand.metadata.get("best_period")
        if cand_DM > 400:
            num_sub = 256
        else:
            num_sub = self.config.folding_params["NSUBCHAN"]

        if cand_P >= 1000.0:
            cmd_fold = (
                "prepfold -dm {} -p {} -pd {} -pdd {} -nosearch -topo -slow -n {} -nsub {} -npart {} -noxwin {} -o {}"
            ).format(
                cand_DM,
                cand_P,
                self.config.folding_params["PD"],
                self.config.folding_params["PDD"],
                self.config.folding_params["NUMBINS"],
                num_sub,
                self.config.folding_params["NPART"],
                self.path,
                self.output_name + "_dm_" + str(cand_DM),
            )
        else:
            cmd_fold = (
                "prepfold -dm {} -p {} -pd {} -pdd {} -nosearch -topo -n {} -nsub {} -npart {} -noxwin {} -o {}"
            ).format(
                cand_DM,
                cand_P,
                self.config.folding_params["PD"],
                self.config.folding_params["PDD"],
                self.config.folding_params["NUMBINS"],
                num_sub,
                self.config.folding_params["NPART"],
                self.path,
                self.output_name + "_dm_" + str(cand_DM),
            )

        #        cmd_fold = ('prepfold -dm {} -p {} -pd {} -pdd {} -nosearch -n {} -nsub {} -npart {} -noxwin {} '
        #                    '-o {}').format(cand_DM,
        #                                    cand_P,
        #                                    self.config.folding_params['PD'],
        #                                    self.config.folding_params['PDD'],
        #                                    self.config.folding_params['NUMBINS'],
        #                                    self.config.folding_params['NSUBCHAN'],
        #                                    self.config.folding_params['NPART'],
        #                                    self.path,
        #                                    self.output_name)

        arg_fold = shlex.split(cmd_fold)
        proc_fold = subprocess.Popen(
            arg_fold, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        proc_fold.wait()

    def fold_profiles(self):

        """Fold all output candidates. Candidates are divided into batches. Each batch
        is then folded parallely across the available cores in each node. This is done
        until all candidates have been folded.
        """

        self.logger.info(
            "Start folding {:d} candidates. "
            "Fold {:d} candidates parallely.".format(
                self.num_candidates, self.config.cores
            )
        )

        # Change to directory where folded profiles are to be stored.

        cwd = os.getcwd()
        os.chdir(self.fold_prf_path)

        candidate_paths = filter_by_ext(self.cands_path, extension=".h5")
        candidate_batches = grouper(candidate_paths, self.config.cores)

        for candidate_batch in candidate_batches:

            processes = []

            with Pool(max_workers=self.config.cores) as pool:
                [
                    processes.append(pool.submit(self.fold_candidate, cand.path))
                    for cand in candidate_batch
                ]
                wait(processes)

        self.logger.info(
            "All candidates folded. "
            "Compile all plots into single PDF file. "
            "Delete all PS files."
        )

        # Use "ghostscript" to combine all .ps files into a single PDF.

        proc_ps_combine = subprocess.Popen(
            "gs -sDEVICE=pdfwrite -dNOPAUSE -dBATCH -dSAFER "
            "-sOutputFile=fold_candidates.pdf *.ps",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Change back to current working directory.

        os.chdir(cwd)

        self.logger.info("Folding done for {:d} candidates.".format(self.num_fold_prfs))

        self._attrs_["num_fold_prfs"] = self.num_fold_prfs

    def archive_profiles(self):

        """ Archive all folded profiles using the "PSRCHIVE" module. """

        self.logger.info("Archive {:d} folded profiles.".format(self.num_fold_prfs))

        fold_prfs = filter_by_ext(self.fold_prf_path, extension=".pfd")
        fold_prf_paths = [fold_prf.path for fold_prf in fold_prfs]

        cmd_arvs = []
        arg_arvs = []
        proc_arvs = []

        [
            cmd_arvs.append(
                "pam -q -a PRESTO {} -e archive -u {}".format(
                    fold_prf_path, self.arv_prf_path
                )
            )
            for fold_prf_path in fold_prf_paths
        ]

        [arg_arvs.append(shlex.split(cmd_arv)) for cmd_arv in cmd_arvs]

        [
            proc_arvs.append(
                subprocess.Popen(
                    arg_arv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
            )
            for arg_arv in arg_arvs
        ]
        [proc_arv.wait() for proc_arv in proc_arvs]

        self.logger.info(
            "Archiving done for {:d} candidates.".format(self.num_arv_prfs)
        )

        self._attrs_["num_arv_prfs"] = self.num_arv_prfs

    def clean_profiles(self):
        ### ADD CODE HERE ###
        self.logger.info("No cleaning yet. Maybe later?")

    def process(self):

        """ Start processing the filterbank file. """

        self.logger.info("Start processing {}.".format(self.output_name))
        start_time = timeit.default_timer()

        self.make_fil()
        self.make_dirs()
        self.load_header()
        self.para_dedisp()
        self.fast_folding()
        self.plot_candidates()
        self.fold_profiles()
        self.archive_profiles()
        self.clean_profiles()

        self.metadata = Meta(self._attrs_)

        end_time = timeit.default_timer()
        self._cumulative_walltime = end_time - start_time
        self.logger.info("Done processing {}.".format(self.output_name))
        self.logger.info("Total processing time: {}".format(self.cumulative_walltime()))

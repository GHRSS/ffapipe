# type: ignore

### Standard imports ###

import os
import pprint

### Non-standard imports ###

import yaml


class CfgManager(object):

    """ Class that extracts and stores the current configuration of the pipeline. """

    def __init__(self, config_file, backend):

        """Create a new CfgManager object that extracts and stores the current
        configuration of the pipeline. All pipeline and path variables are stored
        and derived here.

        Parameters:
        -----------
        config_file: str or Path-like
            The absolute path to the YAML configuration file.
        backend: str
            The specific backend which produced the data.

                Could be one of:

                1. GMRT Software Backend (GSB),
                2. GMRT Wideband Backend (GWB), or
                3. SIMulated GWB data (SIM).

            The SIM backend is used only for testing purposes.
        """

        self.backend = backend
        self._config = self.parse_yaml_config(config_file)

    ### Pipeline variables. ###

    @property
    def cores(self):
        """ Number of cores per node. """
        return self._config["pipeline_variables"]["cores"]

    @property
    def nodes(self):
        """ List of node ID(s). """
        nodes = self._config["pipeline_variables"]["nodes"]
        nodes = [d.strip() for d in nodes.split(",")]
        return nodes

    @property
    def mach_config(self):
        """The machine configuration ("single" or "multiple").
        Decided by looking at the list of node ID(s) entered by
        user in the configuration file.
        """
        if len(self.nodes) == 1:
            mach_config = "single"
        else:
            mach_config = "multiple"

        return mach_config

    @property
    def analysis_dates(self):
        """ The dates to be analysed. Filtered by backend. """
        dates = str(self._config["pipeline_variables"]["dates"][self.backend])
        dates = [d.strip() for d in dates.split(",")]
        dates = dates[:5]
        return dates

    @property
    def variables(self):
        """ All pipeline variables compiled into a dictionary. """
        VarDict = {
            "nodes": self.nodes,
            "mach_config": self.mach_config,
            "backend": self.backend,
            "cores": self.cores,
            "analysis_dates": self.analysis_dates,
        }

        return VarDict

    ### Path variables. ###

    @property
    def store_path(self):
        """ The absolute path where all inputs are stored. """
        return self._config["path_variables"]["store_path"].rstrip(os.path.sep)

    @property
    def pipeline_path(self):
        """ The absolute path where the pipeline itself resides. """
        return self._config["path_variables"]["pipeline_path"].rstrip(os.path.sep)

    @property
    def configurations_path(self):
        """ The absolute path where all configuration files reside. """
        return os.path.join(self.pipeline_path, "configurations")

    @property
    def scripts_path(self):
        """ The absolute path where all pipeline scripts are stored. """
        return os.path.join(self.pipeline_path, "src_scripts")

    @property
    def preprocessing_path(self):
        """ The absolute path where all preprocessing templates are stored. """
        return os.path.join(self.pipeline_path, "preprocessing")

    @property
    def sources_path(self):
        """ The absolute path where all source list for the GHRSS survey reside. """
        return os.path.join(self.pipeline_path, "sources")

    @property
    def rfi_path(self):
        """ The absolute path where all RFI masks are stored. """
        return os.path.join(os.path.dirname(self.store_path), "RFI")

    @property
    def state_path(self):
        """ The absolute path where all output files are stored. """
        return os.path.join(os.path.dirname(self.store_path), "state")

    @property
    def paths(self):
        """ All path variables compiled into a dictionary. """
        PathDict = {
            "store_path": self.store_path,
            "pipeline_path": self.pipeline_path,
            "configurations_path": self.configurations_path,
            "scripts_path": self.scripts_path,
            "preprocessing_path": self.preprocessing_path,
            "sources_path": self.sources_path,
            "rfi_path": self.rfi_path,
            "state_path": self.state_path,
        }

        return PathDict

    ### Processing variables. ###

    @property
    def bad_channels(self):
        """ The bad frequency channels, filtered by backend. """
        return self._config["RFI Masking"]["Bad channels"][self.backend]

    @property
    def freq_sig(self):
        """Sigma-cutoff for frequency flagging, carried out by
        the "rfifind" module. Cut-off used only for the GWB/SIM
        backend.
        """
        return self._config["RFI Masking"]["FREQ SIGMA"]

    @property
    def ddplan(self):
        """ The de-dispersion plan. """
        return self._config["DDPlan"][self.backend]

    @property
    def folding_params(self):
        """Parameters used by the "prepfold" module for folding
        all output pulsar candidates thrown out by the FFA module.
        """
        return self._config["Folding"]

    def parse_yaml_config(self, fname):

        """Parse the YAML configuration file.

        Uses the "safe_load" method since the "load" method is known
        to be a unsafe (since it can load "any" Python object).

        Parameters:
        -----------
        fname: str or Path-like
            The absolute path to the configuration file.
        """

        with open(fname, "r") as fobj:
            config = yaml.safe_load(fobj)
        return config

    def print_config(self):

        """Print the configuration in easy-to-read style,
        just in case it needs to be verified by the user.
        """

        print("\nVariables\n")
        for key, value in self.variables.items():
            print("{}: {}".format(key, value))

        print("\nPaths\n")
        for key, value in self.paths.items():
            print("{}: {}".format(key, value))

        print("\nBad Channels\n")
        print(self.bad_channels)

        print("\nDD Plan\n")
        for key, value in self.ddplan.items():
            print("{}: {}".format(key, value))

        print("\nFolding Parameters\n")
        for key, value in self.folding_params.items():
            print("{}: {}".format(key, value))

# type: ignore

import os
import textwrap
import argparse

from src_scripts.cfg_manager import CfgManager
from src_scripts.wkr_manager import PipelineWorker

# Parse arguments.

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    usage=argparse.SUPPRESS,
    description=textwrap.dedent(
        """
    ################################################
    GHRSS Survey FFA Pipeline: Multiple Nodes Script
    ################################################
    
    Launches the GHRSS Survey FFA Pipeline on mutliple
    machines. All dates are processed in parallel.
    
    usage: python %(prog)s [node] [date] -config [config_file] [backend]"""
    ),
)

parser.add_argument(
    "node",
    type=str,
    help=textwrap.dedent("""The node on which the data is going to be processed."""),
)

parser.add_argument(
    "date",
    type=str,
    help=textwrap.dedent("""The date to be processed on a particular node."""),
)

parser.add_argument(
    "-config",
    type=str,
    default="./configurations/ghrss_config.yaml",
    help=textwrap.dedent("""The absolute path to the "ghrss_config.yaml" file."""),
)

parser.add_argument(
    "backend",
    type=str,
    help=textwrap.dedent(
        """The specific backend which produced the data. 
                    Could be one of:
                    
                        1. GMRT Software Backend (GSB), 
                        2. GMRT Wideband Backend (GWB), or
                        3. SIMulated GWB data (SIM).
                    The SIM backend is used only for testing purposes."""
    ),
)

try:

    args = parser.parse_args()

except:

    parser.print_help()
    parser.exit(1)

# Convert any relative paths to absolute paths, just in case.
# Store all arguments.

node = args.node
date = args.date
config_file = os.path.realpath(args.config)
backend = args.backend

# Get the pipeline configuration.

cfg = CfgManager(config_file, backend)

# Initialise the pipeline.

worker = PipelineWorker(cfg)
worker(date)

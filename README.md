<div align="center">

# `ffapipe`: The GHRSS Survey FFA Pipeline

![License][license]
![Stars][stars]

## Contents

[**Citation**](#citation)<br/>
[**Sky Coverage**](#sky-coverage)<br/>
[**DM and Period Coverage**](#dm-and-period-coverage)<br/>
[**Directories**](#directories)<br/>
[**Scripts**](#scripts)<br/>
[**Dependencies**](#dependencies)<br/>
[**Notes**](#notes)<br/>

</div>
<br/>

<div align="justify">

This is the code being used to analyse data obtained from the [**G**iant
**M**eterwave **R**adio **T**elescope (**GMRT**)][gmrt] as part of the [**G**MRT
**H**igh **R**esolution **S**outhern **S**ky (**GHRSS**)][ghrss] survey, written
from scratch by [**Ujjwal Panda**][upanda] in 2019. It is written in pure
Python, but it depends on the [**PRESTO**][presto] package for some of its
processing capabilities (such as dedispersion and folding). This pipeline uses
[**riptide**][riptide], an FFA implementation developed by [**Vincent
Morello**][morello] at
the University of Manchester.

## Citation

If you use this code, or the output it produces, in a scientific publication, do
cite the following paper: <u>[**The GMRT High Resolution Southern Sky Survey for
Pulsars and Transients. III. Searching for Long-period Pulsars**][GHRSS3]</u>.
This citation is available in the [`CITATION.bib`](CITATION.bib) file as well,
for direct use with `BibTeX` or `BibLaTeX`.

## DM and Period Coverage

The pipeline searches dispersion measures up to **500** pc per cubic centimeter,
in steps of **0.1** (*GWB*) or **0.2** (*GSB*) pc per cubic centimeter. This
amounts to **5000** or **2500** DM trials, respectively.

The pipeline searches **4** different period ranges, and the parameters of the
FFA search are set accordingly. These are:

* **0.1** to **0.5** seconds
* **0.5** to **2.0** seconds
* **2.0** to **10.0** seconds
* **10.0** to **100.0** seconds

## Directories

There are 4 main directories:

1. `configurations`: This directory stores the configurations for the pipeline.
   The [`ghrss_config.yaml`](./configurations/ghrss_config.yaml) contains the
   configuration for the main pipeline, whereas the
   [`ffa_config`](./configurations/ffa_config) directory contains the
   configurations for the different parameter spaces searched for by `riptide`.
   There is a
   [`manager_config.yaml`](./configurations/ffa_config/manager_config.yaml) that
   stores the overall configuration for `riptide`, and then there are 4
   different files for each different period space searched by our FFA pipeline:

    * [`config_short.yaml`](./configurations/ffa_config/config_short.yaml)
    * [`config_mediium.yaml`](./configurations/ffa_config/config_medium.yaml)
    * [`config_long.yaml`](./configurations/ffa_config/config_long.yaml)
    * [`config_longer.yaml`](./configurations/ffa_config/config_longer.yaml)

2. `sources`: This directory stores the coordinates for all pointings that are
   observed as part of the GHRSS survey. These, along with the associated
   timestamp files, are used by the pipeline to construct the metadata for each
   raw file. There are two source lists:
   [`ghrss.1.list`](./sources/ghrss.1.list) and
   [`ghrss.2.list`](./sources/ghrss.2.list).

3. `preprocessing`: This directory stores configuration parameters for certain
   preprocessing scripts that are used by this pipeline, such as
   [**`GPTool`**][gptool]. **`GPTool`** is primarily used for RFI mitigation (in
   *both* frequency and time domains). It reads the configuration variables from
   the corresponding `gptool.in` files for each backend (see the note on
   [**backends**](#notes)), stored in their corresponding sub-directories here.

4. `src_scripts`: This is where the main processing code resides. The primary
   purpose of this code is *automation*. It runs the necesssary Python
   functions/scripts and shell commands for each step, and ensures that each
   process waits for the previous one to finish. It also uses a simple mechanism
   that allows the pipeline to restart from where it left off, in case of a
   crash.

## Scripts

Depending on how you want to run the pipeline, you can either of two scripts:

* The [`single_config.py`](./single_config.py) runs the pipeline on a single
  machine. If your machine has multiple cores, you can get a speedup by
  specifying the number of cores you want to use in the
  [`ghrss_config.yaml`](./configurations/ghrss_config.yaml) file.

* The [`multi_config.py`](./multi_config.py) file. Originally, this script was
  intended for automating the run of the pipeline on multiple machines. However,
  I could not get a framework like **paramiko** to work at the time (for
  automating the login into each of the nodes and setting up the conda
  environment). This file is no diffferent from the
  [`single_config.py`](./single_config.py) in any way, except for the extra
  `node` argument.

The pipeline's run can be monitored using the
[`the_monitor.py`](./the_monitor.py) script. This uses the `curses` library to
construct a simple terminal user interface, where you can see both the state of
the current run of the pipeline, as well as the files it has already processed.
It uses the logs produced by the pipeline, as well as file system information,
to do this.

## Dependencies

The pipeline relies on the following Python packages:

* [**`pytz`**][pytz]
* [**`yaml`**][yaml]
* [**`numpy`**][numpy]
* [**`astropy`**][astropy]
* [**`riptide`**][riptide]
* [**`matplotlib`**][matplotlib]

The best way to ensure that all these dependencies are present on your machine
is to use a [**conda environment**][conda]. The
[`the_monitor.py`](./the_monitor.py) script relies on the `curses` package in
the Python standard library, which in turn depends on the `ncurses` backend.
This implies that this particular script may not run on a Windows system.

Additionally, this pipeline has the following non-Python dependencies:

* [**`PRESTO`**][presto]
* [**`GPTool`**][gptool]

There are also certain in-house scripts that this pipeline depends on for
processes such as zero DM filtering, filterbank file creation, and so on. I will
try to add these scripts to this repository soon :sweat_smile:. If you find a
bug in the pipeline or have any issues in running it on your system, let me know
in the [**issues**][issues] :grin: :+1: !

## Notes

1. There are two backends in use at **GMRT**: the **G**MRT **S**oftware
   **B**ackend (**GSB**) and the **G**MRT **W**ideband **B**ackend (**GWB**). As
   their names indicate, the former is *narrowband*, while the latter is
   *wideband* (installed as a part of the **upgraded** GMRT, a.k.a. uGMRT). The
   scripts in this repository work with data from both backends. The following
   table lists out some of the relevant parameters for each backend:

    <div align="center">

    |     |       Bandwidth      |    Sampling Time   | Center Frequency |
    | --- | -------------------- | ------------------ | ---------------- |
    | GSB |   8, 16 or 32 MHz    | 61.44 microseconds |     336 MHz      |
    | GWB | 100, 200, or 400 MHz | 81.92 microseconds |     400 MHz      |

    </div>

2. This pipeline uses an old version of [**`riptide`**][riptide] (v0.0.1, to be
   precise :sweat_smile:). The codes here *may* work with `v0.0.2` and `v0.0.3`,
   but they definitely would not work with any of the newer versions (`v0.1.0`
   and beyond) because of a massive refactor of `riptide`'s entire codebase. A
   version of the pipeline that works with newer versions of `riptide` is in the
   works :hammer:.

</div>

[morello]: https://github.com/v-morello
[upanda]: https://github.com/astrogewgaw

[GHRSS3]: https://doi.org/10.3847/1538-4357/ac7b91

[gmrt]: http://gmrt.ncra.tifr.res.in
[conda]: https://docs.conda.io/en/latest
[issues]: https://github.com/GHRSS/ffapipe/issues
[ghrss]: http://www.ncra.tifr.res.in/~bhaswati/GHRSS.html

[stars]: https://img.shields.io/github/stars/GHRSS/ffapipe?style=for-the-badge

[arXiv3]: https://img.shields.io/badge/arXiv-2206.00427-B31B1B.svg?style=for-the-badge
[license]: https://img.shields.io/github/license/astrogewgaw/ghrss-ffa?style=for-the-badge

[yaml]: https://pyyaml.org/
[numpy]: https://numpy.org/
[astropy]: https://www.astropy.org/
[matplotlib]: https://matplotlib.org/
[pytz]: https://pythonhosted.org/pytz/
[riptide]: https://github.com/v-morello/riptide
[presto]: https://github.com/scottransom/presto
[gptool]: https://github.com/chowdhuryaditya/gptool

"""A script to convert MIF NMR folder structure to useful data."""

import argparse
import logging
import re
import time
import tomllib
from datetime import datetime
from pathlib import Path
from queue import Queue
from shutil import copytree
from typing import Optional

__author__ = "Filip T. Szczypiński"
__version__ = "0.3.0"

logging.captureWarnings(True)
logger = logging.getLogger(__name__)


def strip_illegal_characters(string: str) -> str:
    """
    Remove characters that cannot form a path.

    Parameters
    ----------
    string
        String to be corrected.

    Returns
    -------
        Corrected string with no illegal characters.

    """
    translation_table = str.maketrans("/$:?|'\\", "_______")
    return string.translate(translation_table)


class NMRFolder:
    """
    A class containing basic NMR folder information.

    Attributes
    ----------
    nmr_sample
        A name of the NMR sample (a.k.a. sample ID).
    experiment
        Name of the experimental parameter set.
    inpath
        Folder from which the data are taken.
    outpath
        Folder where the data are copied to.
    timestamp
        A timestamp of the NMR data.

    """

    def __init__(
        self,
        nmr_sample: str,
        experiment: str,
        inpath: Path,
        outpath: Path,
        timestamp: float,
    ) -> None:
        """
        Initialise NMR data.

        Parameters
        ----------
        nmr_sample
            A name of the NMR sample (a.k.a. sample ID).
        experiment
            Name of the experimental parameter set.
        inpath
            Folder from which the data are taken.
        outpath
            Folder where the data are copied to.
        timestamp
            A timestamp of the NMR data.

        """
        self.nmr_sample = nmr_sample
        self.experiment = experiment
        self.inpath = inpath
        self.outpath = outpath
        self.timestamp = timestamp

    @staticmethod
    def get_sample_name(
        nmr_path: Path,
    ) -> str:
        """
        Get NMR sample name.

        Sample names are stored in the TopSpin's `orig` file, as:
        Name :-RESEARCHER_NAME   :  Sample ID :-SAMPLE_NAME. If it cannot find
        the SAMPLE_NAME, then it tries to use RESEARCHER_NAME. If that is also
        empty - or any other error has occurred here - the sample name will
        be set to UNKNOWN.

        Parameters
        ----------
        nmr_path
            Path to the TopSpin NMR data folder.

        """
        sample_id = "UNKNOWN"
        orig_path = nmr_path / "orig"

        logging.debug("Trying to open %s to find the sample ID.", orig_path)

        try:
            with orig_path.open() as file:
                for line in file:
                    m = re.search(r"Sample ID\s*[:-]{0,2}(.*)", line)
                    if m is not None:
                        sample_id = m.group(1).strip()
                        if sample_id:
                            break

                        else:
                            logger.error(
                                "Unknown sample ID: checking NAME field."
                            )
                            m = re.search(
                                r"Name\s*[:-]{0,2}(.*):\s*Sample ID",
                                line,
                            )
                            if m is not None:
                                sample_id = m.group(1).strip()
                                if sample_id:
                                    logger.info(
                                        "Using the NAME field (%s).", sample_id
                                    )
                                else:
                                    logger.error(
                                        "Unknown sample ID: saving as UNKNOWN."
                                    )
                                    sample_id = "UNKNOWN"
                                break
        except Exception:
            logger.error("Error reading sample name: saving as UNKNOWN.")
            return "UNKNOWN"

        return strip_illegal_characters(sample_id)

    @staticmethod
    def get_experiment_name(
        nmr_path: Path,
    ) -> str:
        """
        Get NMR experiment name.

        Experiment names are stored inside the `acqus` file, as:
        ##$EXP= <PROTON16.CMDnp>

        Parameters
        ----------
        nmr_path
            Path to the TopSpin NMR data folder.

        """

        acqus_path = nmr_path / "acqus"

        try:
            with acqus_path.open() as file:
                for line in file:
                    m = re.search(r"##\$EXP= <(.*)>", line)
                    if m is not None:
                        exp_name = m.group(1)
                        logger.info("Experiment type is: %s.", exp_name)
                        return strip_illegal_characters(exp_name)

            logger.error("Experiment name not found - aborted!")
            return "UNKNOWN"

        except Exception:
            logger.error("Error reading experiment name: saving as UNKNOWN.")
            return "UNKNOWN"

    @classmethod
    def from_mif(
        cls,
        inpath: Path,
        outdir: Path,
    ) -> "NMRFolder":
        """
        Initialise NMRFolder from the MIF NMR location.

        This function will perform the renaming of the directory tree.

        Parameters
        ----------
        inpath
            Path to the NMR data folder to be processed.
        outdir
            Output directory for the resulting new directory tree.

        """
        nmr_sample = NMRFolder.get_sample_name(inpath)
        experiment = NMRFolder.get_experiment_name(inpath)

        expno = 10
        for nmr_exp in (outdir / nmr_sample).glob("*"):
            if nmr_exp.is_dir():
                expno = int(nmr_exp.name) + 10

        outpath = outdir / nmr_sample / f"{expno}"

        if not outpath.exists():
            copytree(inpath, outpath)

        nmr_folder = cls(
            nmr_sample=nmr_sample,
            experiment=experiment,
            inpath=inpath,
            outpath=outpath,
            timestamp=inpath.stat().st_mtime,
        )

        nmr_folder.append_expno_info(
            expno=expno,
        )

        return nmr_folder

    def append_expno_info(
        self,
        expno: int,
    ) -> None:
        """
        Append exposition number into sample summary TOML.

        The sample summary TOML lives in the main sample directory.

        Parameters
        ----------
        expno
            TopSpin exposition number.

        """
        toml_path = self.outpath.parent / f"{self.nmr_sample}.toml"
        inpath_short = f"{self.inpath.parent.name}/{self.inpath.name}"

        new_exp = "\n".join(
            [
                f"[{expno}]",
                f"EXPERIMENT = '{self.experiment}'",
                f"inpath = '{inpath_short}'",
                "",
                "",
            ]
        )
        with open(toml_path, "a", encoding="utf-8") as f:
            f.write(new_exp)

    def to_toml_string(self) -> str:
        """Get NMRFolder as a serialisable TOML string."""
        inpath = str(self.inpath).replace("\\", "/")
        outpath = str(self.outpath).replace("\\", "/")
        toml_string = "\n".join(
            [
                "[[processed]]",
                f'nmr_sample = "{self.nmr_sample}"',
                f'experiment = "{self.experiment}"',
                f'inpath = "{inpath}"',
                f'outpath = "{outpath}"',
                f"timestamp = {self.timestamp}",
            ]
        )

        return toml_string


class GlynWatcher:
    """
    A class containing the watcher status.

    Attributes
    ----------
    inpath
        Source from which the NMR data are taken.
    outpath
        Destination of processed NMR folders.
    toml_path
        A path to dump file with the watcher status.
    last_timestamp
        A timestamp of the last processed NMR folder.

    """

    def __init__(
        self,
        inpath: Path,
        outpath: Path,
        toml_path: Path,
        last_timestamp: float = 0,
        clean: Optional[bool] = False,
    ) -> None:
        """
        Initialise the watcher.

        Parameters
        ----------
        inpath
            Source from which the NMR data are taken.
        outpath
            Destination of processed NMR folders.
        toml_path
            A path to dump file with the watcher status.
        last_timestamp
            A timestamp of the last processed NMR folder.
        processed_folders
            A list of already processed folders.
        clean
            If True, will overwrite the watcher TOML log.

        """
        self.inpath = inpath
        self.outpath = outpath
        self.toml_path = toml_path
        self.last_timestamp = last_timestamp

        inpath_str = str(self.inpath).replace("\\", "/")
        outpath_str = str(self.outpath).replace("\\", "/")
        if not toml_path.exists():
            with open(toml_path, mode="a", encoding="utf-8") as f:
                toml_str = "\n".join(
                    [
                        "[watcher]",
                        f'inpath = "{inpath_str}"',
                        f'outpath = "{outpath_str}"',
                    ]
                )
                f.write(toml_str + "\n\n")

        if clean:
            with open(toml_path, mode="w", encoding="utf-8") as f:
                toml_str = "\n".join(
                    [
                        "[watcher]",
                        f'inpath = "{inpath_str}"',
                        f'outpath = "{outpath_str}"',
                    ]
                )
                f.write(toml_str + "\n\n")

    @classmethod
    def from_toml(
        cls,
        toml_path: Path,
        default_inpath: str = "MIF_NMR",
        default_outpath: str = "AIC_NMR",
    ) -> "GlynWatcher":
        """
        Initialise the watcher from a TOML dump.

        Parameters
        ----------
        toml_path
            A path to the TOML dump.
        default_inpath, optional
            Only used for error handling, by default Path("MIF_NMR")
        default_outpath, optional
            Only used for error handling, by default Path("AIC_NMR")

        """
        try:
            with open(toml_path, "rb") as f:
                toml_data = tomllib.load(f)

            watcher = cls(
                inpath=Path(toml_data["watcher"]["inpath"]),
                outpath=Path(toml_data["watcher"]["outpath"]),
                toml_path=toml_path,
            )

            watcher.last_timestamp = max(
                folder["timestamp"] for folder in toml_data["processed"]
            )

            del toml_data

        except (KeyError, tomllib.TOMLDecodeError, OSError) as e:
            logger.error("TOML decoding failed: starting from scratch! (%s)", e)
            watcher = cls(
                inpath=Path(default_inpath),
                outpath=Path(default_outpath),
                toml_path=toml_path,
            )

        return watcher

    def process_folder(
        self,
        nmr_folder_path: Path,
    ) -> None:
        """
        Process an NMR folder.

        Parameters
        ----------
        NMRFolder
            The NMR folder to be processed.

        """
        nmr_folder = NMRFolder.from_mif(
            inpath=nmr_folder_path,
            outdir=self.outpath,
        )
        self.last_timestamp = nmr_folder_path.stat().st_mtime
        logger.debug("Last timestamp is: %s.", self.last_timestamp)
        with open(self.toml_path, mode="a", encoding="utf-8") as f:
            f.write(nmr_folder.to_toml_string() + "\n\n")


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(
        prog="Deglynifier",
        description="Converts MIF NMR folder tree into sample-based tree.",
        epilog="For better integration with electronic lab notebooks.",
    )

    parser.add_argument(
        "inpath",
        type=Path,
        help="A path to the useless NMR data structure.",
    )

    parser.add_argument(
        "outpath",
        type=Path,
        help="A path to the output data directory.",
    )

    parser.add_argument(
        "--clean",
        action="store_true",
        help="A clean run (not using any information about the former state).",
    )

    parser.add_argument(
        "-t",
        "--toml",
        type=Path,
        default=Path("deglynifier_dump.toml"),
        help="A path to the TOML processed data information dump.",
    )

    parser.add_argument(
        "-l",
        "--log",
        type=Path,
        default=Path("deglynifier.log"),
        help="A path to the log file.",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Make the log files more verbose.",
    )

    parser.add_argument(
        "-w",
        "--wait",
        type=int,
        default=120,
        help="Time in seconds to wait between re-checking the folder.",
    )

    parser.add_argument(
        "--start",
        type=datetime.fromisoformat,
        default=datetime.fromisoformat("2010-01-01"),
        help="Start date for folder conversion (YYYY-MM-DD).",
    )

    parser.add_argument(
        "--end",
        type=lambda x: datetime.fromisoformat(x).replace(
            hour=23, minute=59, second=59
        ),
        default=datetime.today().replace(hour=23, minute=59, second=59),
        help="End date for folder conversion (YYYY-MM-DD).",
    )

    return parser.parse_args()


def main(
    inpath: Path,
    outpath: Path,
    toml_path: Path,
    wait_time: int,
    start_date: datetime,
    end_date: datetime,
    clean_run: bool,
) -> None:
    """
    Execute the NMR folder processing.

    Parameters
    ----------
    inpath
        A Path to the MIF NMR folders.
    outpath
        A Path to the destination NMR folder.
    toml_path
        A Path to the TOML watcher dump file.
    wait_time
        Wait time while watching.
    start_date
        Start date for NMR processing.
    end_date
        End date for NMR processing,
    clean_run
        If True, will ignore any log/dump and start from scratch.

    """

    logger.info(
        "Will use the watcher dumped in %s (if exists).", toml_path.name
    )

    if clean_run or not toml_path.exists():
        watcher = GlynWatcher(
            inpath=inpath,
            outpath=outpath,
            toml_path=toml_path,
            clean=True,
        )
        logger.info("Empty watcher created.")

    else:
        watcher = GlynWatcher.from_toml(
            toml_path,
            default_inpath=str(inpath),
            default_outpath=str(outpath),
        )
        logger.info("Watcher loaded.")
        logger.info(
            "Latest processed folder is from %s.",
            datetime.fromtimestamp(watcher.last_timestamp).strftime('%c')
        )

    try:
        # Identify changes since running the script last time.
        logger.info("Identifying data folders in %s.", inpath.name)
        logger.debug("Start date is set at %s.", start_date.timestamp())
        logger.debug("End date is set at %s.", end_date.timestamp())

        infiles = []
        for folder in watcher.inpath.glob("*/*"):
            logger.debug("Found %s/%s.", folder.parent.name, folder.name)
            timestamp = folder.stat().st_mtime
            ctime = datetime.fromtimestamp(watcher.last_timestamp)
            logger.debug("Folder creation date is %s.", ctime.strftime('%c'))

            if timestamp <= watcher.last_timestamp:
                logger.debug("Folder predates latest processed folder.")

            elif start_date.timestamp() > timestamp:
                logger.debug("Folder is older than the desired start date.")

            elif end_date.timestamp() < timestamp:
                logger.debug("Folder is newer than the desired end date.")

            else:
                infiles.append(folder)
                logger.info(
                    "Added %s/%s to the stack.", folder.parent.name, folder.name
                )
        infiles = sorted(infiles, key=lambda x: x.stat().st_mtime)

        # Process old folders.
        logger.info("Processing folders since last run.")

        to_process: Queue[Path] = Queue()
        for folder in infiles:
            to_process.put(folder)

        del infiles

        logger.info("Will process %d folders.", to_process.qsize())

        while not to_process.empty():
            process_path = to_process.get()
            logger.info(
                "Processing %s/%s.", process_path.parent.name, process_path.name
            )
            watcher.process_folder(nmr_folder_path=process_path)
            to_process.task_done()
            logger.info("Processing done. Left: %d tasks.", to_process.qsize())

        # Watch and process new folders as they come.
        logger.info(
            "Will check for changes every %.2f minutes.", wait_time / 60
        )
        while True:
            time.sleep(wait_time)
            logger.debug("Looking for changes.")
            for folder in watcher.inpath.glob("*/*"):
                if folder.stat().st_mtime > watcher.last_timestamp:
                    logger.info(
                        "New folder identified: %s/%s",
                        folder.parent.name, folder.name
                    )
                    to_process.put(folder)

            while not to_process.empty():
                proc_path = to_process.get()
                logger.info(
                    "Processing %s/%s.", proc_path.parent.name, proc_path.name
                )
                watcher.process_folder(nmr_folder_path=proc_path)
                to_process.task_done()
                logger.info("Folder processing finished.")

    except KeyboardInterrupt:
        pass

    finally:
        logger.info("Process stopped.")


if __name__ == "__main__":
    args = parse_arguments()

    logging.basicConfig(
        level="DEBUG" if args.verbose else "INFO",
        filename=args.log,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
    )

    main(
        inpath=args.inpath,
        outpath=args.outpath,
        toml_path=args.toml,
        wait_time=args.wait,
        start_date=args.start,
        end_date=args.end,
        clean_run=args.clean,
    )

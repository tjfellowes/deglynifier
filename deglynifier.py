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
__version__ = "0.1.0"

logging.captureWarnings(True)
logger = logging.getLogger(__name__)


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
    ):
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
        Name :-RESEARCHER_NAME   :  Sample ID :-SAMPLE_NAME

        Parameters
        ----------
        nmr_path
            Path to the TopSpin NMR data folder.

        """

        orig_path = nmr_path / "orig"

        m = re.search(r"Sample ID\s*[:-]{0,2}(.*)", orig_path.read_text())

        if m is not None:
            return m.group(1).strip()

        else:
            logger.critical("Sample name not found - aborted!")
            raise (AttributeError("Sample name not found."))

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

        m = re.search(r"##\$EXP= <(.*)>", acqus_path.read_text())

        if m is not None:
            return m.group(1)

        else:
            logger.critical("Experiment name not found - aborted!")
            raise (AttributeError("Experiment name not found."))

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
            timestamp=inpath.stat().st_ctime,
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
        with open(toml_path, "a") as f:
            f.write(new_exp)

    def to_toml_string(self):
        """Get NMRFolder as a serialisable TOML string."""
        toml_string = "\n".join(
            [
                "[[processed]]",
                f'nmr_sample = "{self.nmr_sample}"',
                f'experiment = "{self.experiment}"',
                f'inpath = "{str(self.inpath).replace("\\", "/")}"',
                f'outpath = "{str(self.outpath).replace("\\", "/")}"',
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
    processed_folders
        A list of already processed folders.

    """

    def __init__(
        self,
        inpath: Path,
        outpath: Path,
        toml_path: Path,
        last_timestamp: float = 0,
        processed_folders: Optional[list[NMRFolder]] = None,
    ):
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

        """
        self.inpath = inpath
        self.outpath = outpath
        self.toml_path = toml_path
        self.last_timestamp = last_timestamp
        if processed_folders is not None:
            self.processed_folders = processed_folders
        else:
            self.processed_folders = []

        if not toml_path.exists():
            with open(toml_path, mode="a") as f:
                toml_str = "\n".join(
                    [
                        "[watcher]",
                        f'inpath = "{str(inpath).replace("\\", "/")}"',
                        f'outpath = "{str(outpath).replace("\\", "/")}"',
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

            for folder in toml_data["processed"]:
                nmr_folder = NMRFolder(
                    nmr_sample=folder["nmr_sample"],
                    experiment=folder["experiment"],
                    inpath=Path(folder["inpath"]),
                    outpath=Path(folder["outpath"]),
                    timestamp=folder["timestamp"],
                )
                watcher.processed_folders.append(nmr_folder)

            watcher.last_timestamp = max(
                folder["timestamp"] for folder in toml_data["processed"]
            )

        except Exception:
            logger.error("TOML decoding failed: starting from scratch!")
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
        self.last_timestamp = self.inpath.stat().st_ctime
        logger.debug(f"Last timestamp is: {self.last_timestamp}.")
        self.processed_folders.append(nmr_folder)
        with open(self.toml_path, mode="a") as f:
            f.write(nmr_folder.to_toml_string() + "\n\n")

    def to_toml(
        self,
        path: Path,
    ) -> None:
        """Get the watcher status as a TOML string."""
        toml_string = "\n\n".join(
            [folder.to_toml_string() for folder in self.processed_folders]
        )
        path.write_text(toml_string)


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
        f"Will use the watcher dumped in {toml_path.name} (if exists)."
    )

    if clean_run or not toml_path.exists():
        watcher = GlynWatcher(
            inpath=inpath,
            outpath=outpath,
            toml_path=toml_path,
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
            "Latest processed folder is from "
            f"{datetime.fromtimestamp(watcher.last_timestamp).strftime('%c')}."
        )

    try:
        # Identify changes since running the script last time.
        logger.info(f"Identifying data folders in {inpath.name}.")
        logger.debug(f"Start date is set at {start_date.timestamp()}.")
        logger.debug(f"End date is set at {end_date.timestamp()}.")

        infiles = []
        for folder in inpath.glob("*/*"):
            logger.debug(f"Found {folder.parent.name}/{folder.name}.")
            timestamp = folder.stat().st_ctime
            ctime = datetime.fromtimestamp(watcher.last_timestamp)
            logger.debug(f"Folder creation date is {ctime.strftime('%c')}.")

            if timestamp <= watcher.last_timestamp:
                logger.debug("Folder predates latest processed folder.")

            elif start_date.timestamp() > timestamp:
                logger.debug("Folder is older than the desired start date.")

            elif end_date.timestamp() < timestamp:
                logger.debug("Folder is newer than the desired end date.")

            else:
                infiles.append(folder)
                logger.debug(f"Added {folder.parent.name}/{folder.name}.")
        infiles = sorted(infiles, key=lambda x: x.stat().st_ctime)

        # Process old folders.
        logger.info("Processing folders since last run.")

        to_process: Queue[Path] = Queue()
        for folder in infiles:
            to_process.put(folder)

        logger.info(f"Will process {to_process.qsize()} folders.")

        while not to_process.empty():
            inpath = to_process.get()
            logger.info(f"Processing {inpath.parent.name}/{inpath.name}.")
            watcher.process_folder(nmr_folder_path=inpath)
            to_process.task_done()
            logger.info(f"Processing done. Left: {to_process.qsize()} tasks.")

        # Watch and process new folders as they come.
        logger.info(
            f"Will check for changes every {wait_time/60:.2f} minutes."
        )
        while True:
            time.sleep(wait_time)
            logger.debug("Looking for changes.")
            for folder in inpath.glob("*/*"):
                if folder.stat().st_ctime > watcher.last_timestamp:
                    logger.info(
                        "New folder identified: "
                        f"{folder.parent.name}/{folder.name}"
                    )
                    to_process.put(folder)

            while not to_process.empty():
                inpath = to_process.get()
                logger.info(f"Processing {inpath.parent.name}/{inpath.name}.")
                watcher.process_folder(nmr_folder_path=inpath)
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

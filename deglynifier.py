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
__version__ = "0.0.1"

logging.captureWarnings(True)
logger = logging.getLogger(__name__)


class NMRFolder:
    """A class containing basic NMR folder information."""

    def __init__(
        self,
        nmr_sample: str,
        experiment: str,
        mif_path: Path,
        aic_path: Path,
        timestamp: float,
    ):
        """Initialise NMRFolder."""
        self.nmr_sample = nmr_sample
        self.experiment = experiment
        self.mif_path = mif_path
        self.aic_path = aic_path
        self.timestamp = timestamp

    @staticmethod
    def get_sample_name(
        nmr_path: Path,
    ) -> str:
        """
        Get NMR sample name.

        Sample names are stored in the TopSpin's `orig` file, as:
        Name :-RESEARCHER_NAME   :  Sample ID :-SAMPLE_NAME

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
        mif_path: Path,
        aic_dir: Path,
    ):
        """Initialise NMRFolder from MIF folder path."""
        nmr_sample = NMRFolder.get_sample_name(mif_path)
        experiment = NMRFolder.get_experiment_name(mif_path)

        expno = 10
        for nmr_exp in (aic_dir / nmr_sample).glob("*"):
            if nmr_exp.is_dir():
                expno = int(nmr_exp.name) + 10

        aic_path = aic_dir / nmr_sample / f"{expno}"

        if not aic_path.exists():
            copytree(mif_path, aic_path)

        nmr_folder = cls(
            nmr_sample=nmr_sample,
            experiment=experiment,
            mif_path=mif_path,
            aic_path=aic_path,
            timestamp=mif_path.stat().st_ctime,
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

        Parameters
        ----------
        toml_path
            Path to the sample summary TOML.
        expno
            TopSpin exposition number.
        experiment_name
            Experiment name.
        mif_path
            Final path to the MIF data folder ("date/expno").

        """
        toml_path = self.aic_path.parent / f"{self.nmr_sample}.toml"
        mif_path_short = f"{self.mif_path.parent.name}/{self.mif_path.name}"

        new_exp = "\n".join(
            [
                f"[{expno}]",
                f"EXPERIMENT = '{self.experiment}'",
                f"MIF_PATH = '{mif_path_short}'",
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
                f'mif_path = "{str(self.mif_path).replace("\\", "/")}"',
                f'aic_path = "{str(self.aic_path).replace("\\", "/")}"',
                f"timestamp = {self.timestamp}",
            ]
        )

        return toml_string


class GlynWatcher:
    """A class containing information about processed data."""

    def __init__(
        self,
        inpath: Path,
        outpath: Path,
        toml_path: Path,
        last_timestamp: float = 0,
        processed_folders: Optional[list[NMRFolder]] = None,
    ):
        """Initialise the watcher."""
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
    ) -> "GlynWatcher":
        """Initialise the watcher from a TOML dump."""
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
                    mif_path=Path(folder["mif_path"]),
                    aic_path=Path(folder["mif_path"]),
                    timestamp=folder["timestamp"],
                )
                watcher.processed_folders.append(nmr_folder)

            watcher.last_timestamp = max(
                folder["timestamp"] for folder in toml_data["processed"]
            )

        except Exception:
            logger.error("TOML decoding failed: starting from scratch!")
            watcher = cls(
                inpath=Path("MIF_NMR"),
                outpath=Path("AIC_NMR"),
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
            mif_path=nmr_folder_path,
            aic_dir=self.outpath,
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
    inpath,
    outpath,
    toml_path,
    wait_time,
    start_date,
    end_date,
    clean_run,
) -> None:
    """Execute script."""

    if clean_run or not toml_path.exists():
        watcher = GlynWatcher(
            inpath=inpath,
            outpath=outpath,
            toml_path=toml_path,
        )
        logger.info("Empty watcher created.")

    else:
        watcher = GlynWatcher.from_toml(toml_path)
        logger.info("Watcher loaded.")
        logger.info(
            "Latest processed folder is from "
            f"{datetime.fromtimestamp(watcher.last_timestamp).strftime('%c')}."
        )

    try:
        # Identify changes since running the script last time.
        logger.info("Identifying data folders in MIF_FILES.")
        logger.debug(f"Start date is set at {start_date.timestamp()}.")
        logger.debug(f"End date is set at {end_date.timestamp()}.")

        mif_files = []
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
                mif_files.append(folder)
                logger.debug(f"Added {folder.parent.name}/{folder.name}.")
        mif_files = sorted(mif_files, key=lambda x: x.stat().st_ctime)

        # Process old folders.
        logger.info("Processing folders since last run.")

        to_process: Queue[Path] = Queue()
        for folder in mif_files:
            to_process.put(folder)

        logger.info(f"Will process {to_process.qsize()} folders.")

        while not to_process.empty():
            mif_path = to_process.get()
            logger.info(f"Processing {mif_path.parent.name}/{mif_path.name}.")
            watcher.process_folder(nmr_folder_path=mif_path)
            to_process.task_done()
            logger.info(f"Processing done. Left: {to_process.qsize()} tasks.")

        # Watch and process new folders as they come.
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
                mif_path = to_process.get()
                logger.info(
                    f"Processing {mif_path.parent.name}/{mif_path.name}."
                )
                watcher.process_folder(nmr_folder_path=mif_path)
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

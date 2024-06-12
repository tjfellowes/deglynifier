"""A script to convert MIF NMR folder structure to useful data."""

import json
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

CLEAN_RUN = False
MIF_FILES = Path.cwd() / "MIF_NMR"
AIC_FILES = Path.cwd() / "AIC_NMR"
DUMP_PATH = Path.cwd() / "deglynifier_dump.toml"
LOG_PATH = Path.cwd() / "deglynifier.log"
WAIT_TIME = 60
LOG_LEVEL = "INFO"
START_TIME = datetime.fromisoformat("2024-01-01")
END_TIME = datetime.fromisoformat("2024-06-12").replace(
    hour=23, minute=59, second=59
)

logging.basicConfig(
    level=LOG_LEVEL,
    filename="deglynifier.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)


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
    def from_mif(cls, mif_path: Path):
        """Initialise NMRFolder from MIF folder path."""
        nmr_sample = NMRFolder.get_sample_name(mif_path)
        experiment = NMRFolder.get_experiment_name(mif_path)

        expno = 10
        for nmr_exp in (AIC_FILES / nmr_sample).glob("*"):
            if nmr_exp.is_dir():
                expno = int(nmr_exp.name) + 10

        aic_path = AIC_FILES / nmr_sample / f"{expno}"

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

    def to_dict(self):
        """Get NMRFolder as a serialisable dictionary."""
        folder_dict = {
            "nmr_sample": self.nmr_sample,
            "experiment": self.experiment,
            "mif_path": f"{self.mif_path}",
            "aic_path": f"{self.aic_path}",
        }

        return folder_dict

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
        last_timestamp: float = 0,
        processed_folders: Optional[list[NMRFolder]] = None,
    ):
        """Initialise the watcher."""
        self.last_timestamp = last_timestamp
        if processed_folders is not None:
            self.processed_folders = processed_folders
        else:
            self.processed_folders = []

    @classmethod
    def from_json(cls, json_path):
        """Initialise the watcher from a JSON dump."""
        try:
            with open(json_path, "r") as f:
                json_data = json.load(f)
            watcher = cls(last_timestamp=json_data["last_timestamp"])

            for folder in json_data["processed_folders"]:
                nmr_folder = NMRFolder(
                    nmr_sample=folder["nmr_sample"],
                    experiment=folder["experiment"],
                    mif_path=Path(folder["mif_path"]),
                    aic_path=Path(folder["mif_path"]),
                )
                watcher.processed_folders.append(nmr_folder)

        except Exception:
            logger.error("JSON decoding failed: starting from scratch!")
            watcher = cls()

        return watcher

    @classmethod
    def from_toml(cls, toml_path):
        """Initialise the watcher from a TOML dump."""
        try:
            with open(toml_path, "rb") as f:
                toml_data = tomllib.load(f)

            watcher = cls()

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
            watcher = cls()

        return watcher

    def to_dict(self):
        """Get the watcher status as a dictionary."""
        processed_folders = [
            folder.to_dict() for folder in self.processed_folders
        ]
        watcher_dict = {
            "last_timestamp": self.last_timestamp,
            "processed_folders": processed_folders,
        }
        return watcher_dict

    def to_toml(
        self,
        path: Path,
    ) -> None:
        """Get the watcher status as a TOML string."""
        toml_string = "\n\n".join(
            [folder.to_toml_string() for folder in self.processed_folders]
        )
        path.write_text(toml_string)


def main():
    """Execute script."""

    if DUMP_PATH.exists() or not CLEAN_RUN:
        watcher = GlynWatcher.from_toml(DUMP_PATH)
        logger.info("Watcher loaded.")
        logger.info(
            "Latest processed folder is from "
            f"{datetime.fromtimestamp(watcher.last_timestamp).strftime("%c")}."
        )
    else:
        watcher = GlynWatcher()
        logger.info("Empty watcher created.")

    try:
        # Identify changes since running the script last time.
        logger.info("Identifying data folders in MIF_FILES.")
        logger.debug(f"START_TIME is set at {START_TIME.timestamp()}.")
        logger.debug(f"END_TIME is set at {END_TIME.timestamp()}.")

        mif_files = []
        for folder in MIF_FILES.glob("*/*"):
            logger.debug(f"Found {folder.parent.name}/{folder.name}.")
            timestamp = folder.stat().st_ctime
            ctime = datetime.fromtimestamp(watcher.last_timestamp)
            logger.debug(f"Folder creation date is {ctime.strftime("%c")}.")

            if timestamp <= watcher.last_timestamp:
                logger.debug("Folder predates latest processed folder.")

            elif START_TIME.timestamp() > timestamp:
                logger.debug("Folder is older than the desired start date.")

            elif END_TIME.timestamp() < timestamp:
                logger.debug("Folder is newer than the desired end date.")

            else:
                mif_files.append(folder)
                logger.debug(f"Added {folder.parent.name}/{folder.name}.")
        mif_files = sorted(mif_files, key=lambda x: x.stat().st_ctime)

        # Process old folders.
        logger.info("Processing folders since last run.")

        to_process = Queue()
        for folder in mif_files:
            to_process.put(folder)

        logger.info(f"Will process {to_process.qsize()} folders.")

        while not to_process.empty():
            mif_path = to_process.get()
            logger.info(f"Processing {mif_path.parent.name}/{mif_path.name}.")
            nmr_folder = NMRFolder.from_mif(mif_path=mif_path)
            watcher.last_timestamp = mif_path.stat().st_ctime
            logger.debug(f"Last timestamp is: {watcher.last_timestamp}.")
            watcher.processed_folders.append(nmr_folder)
            to_process.task_done()
            with open(DUMP_PATH, mode="a") as f:
                f.write(nmr_folder.to_toml_string() + "\n\n")
            logger.info(f"Processing done. Left: {to_process.qsize()} tasks.")

        # Watch and process new folders as they come.
        while True:
            time.sleep(WAIT_TIME)
            logger.debug("Looking for changes.")
            for folder in MIF_FILES.glob("*/*"):
                if folder.stat().st_ctime > watcher.last_timestamp:
                    logger.info(
                        "New folder identified: "
                        f"{folder.parent.name}/{folder.name}"
                    )
                    to_process.put(folder)

            while not to_process.empty():
                mif_path = to_process.get()
                logger.info(
                    f"Processing {mif_path.parent.name}/{mif_path.name}"
                )
                nmr_folder = NMRFolder.from_mif(mif_path=mif_path)
                watcher.last_timestamp = mif_path.stat().st_ctime
                logger.debug(f"Last timestamp is: {watcher.last_timestamp}.")
                watcher.processed_folders.append(nmr_folder)
                to_process.task_done()
                with open(DUMP_PATH, mode="a") as f:
                    f.write(nmr_folder.to_toml_string() + "\n\n")
                logger.info("Folder processing finished.")

    finally:
        logger.info("Process stopped.")


if __name__ == "__main__":
    main()

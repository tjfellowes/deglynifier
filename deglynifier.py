"""A script to convert MIF NMR folder structure to useful data."""

import json
import logging
from queue import Queue
import re
from pathlib import Path
from shutil import copytree
from dataclasses import dataclass
import time

__author__ = "Filip T. Szczypiński"
__version__ = "0.0.1"

logging.captureWarnings(True)
logger = logging.getLogger(__name__)

MIF_FILES = Path.cwd() / "MIF_NMR"
AIC_FILES = Path.cwd() / "AIC_NMR"
DUMP_PATH = Path.cwd() / "deglynifier_dump.json"
LOG_PATH = Path.cwd() / "deglynifier.log"
WAIT_TIME = 60

logging.basicConfig(
    level=logging.INFO,
    filename="deglynifier.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)


@dataclass
class NMRFolder:
    """A class containing basic NMR folder information."""

    def __init__(
        self,
        nmr_sample: str,
        experiment: str,
        mif_path: Path,
        aic_path: Path,
    ):
        self.nmr_sample = nmr_sample
        self.experiment = experiment
        self.mif_path = mif_path
        self.aic_path = aic_path

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
        )

        nmr_folder.append_expno_info(
            expno=expno,
        )

        return nmr_folder

    def append_expno_info(
        self,
        expno: int,
    ) -> None:
        """Append exposition number into sample summary TOML.

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
        folder_dict = {
            "nmr_sample": self.nmr_sample,
            "experiment": self.experiment,
            "mif_path": f"{self.mif_path}",
            "aic_path": f"{self.aic_path}",
        }

        return folder_dict


class GlynWatcher:
    """A class containing information about processed data."""

    def __init__(
        self,
        last_timestamp: float = 0,
        processed_folders: list[NMRFolder] = [],
    ):
        self.last_timestamp = last_timestamp
        self.processed_folders = processed_folders

    @classmethod
    def from_json(cls, json_path):
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

    def to_dict(self):
        processed_folders = [
            folder.to_dict() for folder in self.processed_folders
        ]
        watcher_dict = {
            "last_timestamp": self.last_timestamp,
            "processed_folders": processed_folders,
        }
        return watcher_dict


def main():
    """Execute script."""

    if DUMP_PATH.exists():
        watcher = GlynWatcher.from_json(DUMP_PATH)
    else:
        watcher = GlynWatcher()

    to_process = Queue()

    try:
        mif_files = sorted(
            [
                folder
                for folder in MIF_FILES.glob("*/*")
                if folder.stat().st_birthtime > watcher.last_timestamp
            ],
            key=lambda x: x.stat().st_birthtime,
        )

        for folder in mif_files:
            to_process.put(folder)

        print(f"Will process {to_process.qsize()} tasks")

        while not to_process.empty():
            mif_path = to_process.get()
            logger.info(f"Processing {mif_path}")
            nmr_folder = NMRFolder.from_mif(mif_path=mif_path)
            watcher.last_timestamp = mif_path.stat().st_birthtime
            watcher.processed_folders.append(nmr_folder)
            to_process.task_done()
            print(f"Left: {to_process.qsize()} tasks")
            with open(DUMP_PATH, "w") as f:
                json.dump(
                    watcher.to_dict(),
                    f,
                    indent=4,
                )

        while True:
            time.sleep(WAIT_TIME)
            for folder in MIF_FILES.glob("*/*"):
                if folder.stat().st_birthtime > watcher.last_timestamp:
                    to_process.put(folder)

            print(f"Will process {to_process.qsize()} tasks")

            while not to_process.empty():
                mif_path = to_process.get()
                logger.info(f"Processing {mif_path}")
                nmr_folder = NMRFolder.from_mif(mif_path=mif_path)
                watcher.last_timestamp = mif_path.stat().st_birthtime
                watcher.processed_folders.append(nmr_folder)
                to_process.task_done()
                print(f"Left: {to_process.qsize()} tasks")
                with open(DUMP_PATH, "w") as f:
                    json.dump(
                        watcher.to_dict(),
                        f,
                        indent=4,
                    )

    finally:
        with open(DUMP_PATH, "w") as f:
            json.dump(
                watcher.to_dict(),
                f,
                indent=4,
            )


if __name__ == "__main__":
    main()

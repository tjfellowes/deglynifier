"""A script to convert MIF NMR folder structure to useful data."""

import argparse
import logging
import re
import time
import tomllib
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from queue import Queue
from shutil import copytree, copyfile
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


class ExptFile(ABC):
    """
    Abstract base class for experiment files.
    """

    @abstractmethod
    def to_toml_string(self) -> str:
        """Get the object as a serialisable TOML string."""

    @classmethod
    @abstractmethod
    def copy_from_instrument(cls, inpath: Path, outdir: Path) -> "ExptFile":
        """Initialise the object from the MIF NMR location."""

class SimpleFile(ExptFile):
    """
    A class containing basic folder information.

    Attributes
    ----------
    inpath
        Folder from which the data are taken.
    outpath
        Folder where the data are copied to.
    timestamp
        A timestamp of the data.

    """

    def __init__(
        self,
        inpath: Path,
        outpath: Path,
        timestamp: float,
    ) -> None:
        """
        Initialise data.

        Parameters
        ----------
        inpath
            Folder from which the data are taken.
        outpath
            Folder where the data are copied to.
        timestamp
            A timestamp of the data.

        """
        self.inpath = inpath
        self.outpath = outpath
        self.timestamp = timestamp

    @classmethod
    def copy_from_instrument(
        cls,
        inpath: Path,
        outdir: Path,
    ) -> "SimpleFile":
        """
        This function will perform the renaming of the directory tree.

        Parameters
        ----------
        inpath
            Path to the data folder to be processed.
        outdir
            Output directory for the resulting new directory tree.

        """

        outpath = outdir / inpath.name

        if not outpath.exists():
            if inpath.is_file():
                copyfile(inpath, outpath)
            elif inpath.is_dir():
                copytree(inpath, outpath)

        file = cls(
            inpath=inpath,
            outpath=outpath,
            timestamp=inpath.stat().st_mtime,
        )

        return file

    def to_toml_string(self) -> str:
        """Get SimpleFile as a serialisable TOML string."""
        inpath = str(self.inpath).replace("\\", "/")
        outpath = str(self.outpath).replace("\\", "/")
        toml_string = "\n".join(
            [
                "[[processed]]",
                f'inpath = "{inpath}"',
                f'outpath = "{outpath}"',
                f"timestamp = {self.timestamp}",
            ]
        )

        return toml_string


class MIFNMRFolder(ExptFile):
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
        except (OSError, FileNotFoundError):
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

        except (OSError, FileNotFoundError):
            logger.error("Error reading experiment name: saving as UNKNOWN.")
            return "UNKNOWN"

    @classmethod
    def copy_from_instrument(
        cls,
        inpath: Path,
        outdir: Path,
    ) -> "MIFNMRFolder":
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
        nmr_sample = MIFNMRFolder.get_sample_name(inpath)
        experiment = MIFNMRFolder.get_experiment_name(inpath)

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


class Watcher:
    """
    A class containing the watcher status.

    Attributes
    ----------
    inpath
        Source from which the data are taken.
    outpath
        Destination of processed folders.
    toml_path
        A path to dump file with the watcher status.
    last_timestamp
        A timestamp of the last processed folder.

    """

    def __init__(
        self,
        inpath: Path,
        outpath: Path,
        toml_path: Path,
        last_timestamp: float = 0,
        clean: Optional[bool] = False,
        exptfile_cls=SimpleFile,
    ) -> None:
        """
        Initialise the watcher.

        Parameters
        ----------
        inpath
            Source from which the data are taken.
        outpath
            Destination of processed folders.
        toml_path
            A path to dump file with the watcher status.
        last_timestamp
            A timestamp of the last processed folder.
        processed_folders
            A list of already processed folders.
        clean
            If True, will overwrite the watcher TOML log.

        """
        self.inpath = inpath
        self.outpath = outpath
        self.toml_path = toml_path
        self.last_timestamp = last_timestamp
        self.exptfile_cls = exptfile_cls

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
        default_inpath: str = "test_inpath",
        default_outpath: str = "test_outpath",
        exptfile_cls= SimpleFile,  # <-- add this
    ) -> "Watcher":
        """
        Initialise the watcher from a TOML dump.

        Parameters
        ----------
        toml_path
            A path to the TOML dump.
        default_inpath, optional
            Only used for error handling, by default Path("test_inpath")
        default_outpath, optional
            Only used for error handling, by default Path("test_outpath")

        """
        try:
            with open(toml_path, "rb") as f:
                toml_data = tomllib.load(f)

            watcher = cls(
                inpath=Path(toml_data["watcher"]["inpath"]),
                outpath=Path(toml_data["watcher"]["outpath"]),
                toml_path=toml_path,
                exptfile_cls=exptfile_cls,  # <-- pass here
            )

            watcher.last_timestamp = max(
                folder["timestamp"] for folder in toml_data.get("processed", [])
            )

            del toml_data

        except (KeyError, tomllib.TOMLDecodeError, OSError) as e:
            logger.error("TOML decoding failed: starting from scratch! (%s)", e)
            watcher = cls(
                inpath=Path(default_inpath),
                outpath=Path(default_outpath),
                toml_path=toml_path,
                exptfile_cls=exptfile_cls,  # <-- pass here
            )

        return watcher

    def process_data(
        self,
        data_path: Path,
    ) -> None:
        """
        Process a data folder/file.

        Parameters
        ----------
        data_path
            The folder/file to be processed.

        """
        nmr_folder = self.exptfile_cls.copy_from_instrument(  # <-- use the class
            inpath=data_path,
            outdir=self.outpath,
        )
        self.last_timestamp = data_path.stat().st_mtime
        logger.debug("Last timestamp is: %s.", self.last_timestamp)
        with open(self.toml_path, mode="a", encoding="utf-8") as f:
            f.write(nmr_folder.to_toml_string() + "\n\n")


EXPTFILE_TYPES = {
    "mifnmr": MIFNMRFolder,
    "simple": SimpleFile,
    # Add other experiment file types here as needed
}

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
        "--exptfile-type",
        type=str,
        choices=EXPTFILE_TYPES.keys(),
        help="Type of experiment file to process (default: mifnmr).",
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

    parser.add_argument(
        "--search-depth",
        type=int,
        default=2,
        help="Depth for searching for input folders (default: 2, i.e., */*).",
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
    glob_depth: int,
    exptfile_type: str,
) -> None:
    """
    Execute the folder processing.

    Parameters
    ----------
    inpath
        A Path to the input folders.
    outpath
        A Path to the destination folder.
    toml_path
        A Path to the TOML watcher dump file.
    wait_time
        Wait time while watching.
    start_date
        Start date for processing.
    end_date
        End date for processing,
    clean_run
        If True, will ignore any log/dump and start from scratch.
    glob_depth
        Depth for globbing input folders.
    exptfile_type
        Type of experiment file to process.
    """

    logger.info(
        "Will use the watcher dumped in %s (if exists).", toml_path.name
    )

    exptfile_cls = EXPTFILE_TYPES[exptfile_type]

    if clean_run or not toml_path.exists():
        watcher = Watcher(
            inpath=inpath,
            outpath=outpath,
            toml_path=toml_path,
            clean=True,
            exptfile_cls=exptfile_cls,
        )
        logger.info("Empty watcher created.")

    else:
        watcher = Watcher.from_toml(
            toml_path,
            default_inpath=str(inpath),
            default_outpath=str(outpath),
            exptfile_cls=exptfile_cls, 
        )
        logger.info("Watcher loaded.")
        logger.info(
            "Latest processed folder is from %s.",
            datetime.fromtimestamp(watcher.last_timestamp).strftime('%c')
        )

    # Build the glob pattern based on depth
    glob_pattern = "*/" * (glob_depth - 1) + "*"

    try:
        # Identify changes since running the script last time.
        logger.info("Identifying data folders in %s.", inpath.name)
        logger.debug("Start date is set at %s.", start_date.timestamp())
        logger.debug("End date is set at %s.", end_date.timestamp())

        infiles = []
        for folder in watcher.inpath.glob(glob_pattern):
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
            watcher.process_data(data_path=process_path)
            to_process.task_done()
            logger.info("Processing done. Left: %d tasks.", to_process.qsize())

        # Watch and process new folders as they come.
        logger.info(
            "Will check for changes every %.2f minutes.", wait_time / 60
        )
        while True:
            time.sleep(wait_time)
            logger.debug("Looking for changes.")
            for folder in watcher.inpath.glob(glob_pattern):
                logger.debug("Found folder: %s Modifed at: %s", folder, folder.stat().st_mtime)
                if (folder.stat().st_mtime > watcher.last_timestamp):
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
                watcher.process_data(data_path=proc_path)
                to_process.task_done()
                logger.info("Folder processing finished.")

    except KeyboardInterrupt:
        pass

    finally:
        logger.info("Process stopped.")


def cli():
    """Command line interface for the script."""
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
        glob_depth=args.search_depth,
        exptfile_type=args.exptfile_type,
    )

if __name__ == "__main__":
    cli()
import logging
import pathlib
import os
from ulod.un import UNDataTopics


def main():
    UNDATA_PATH = pathlib.Path(os.environ["HOME"], "data", "open_data", "UNData")
    UNDATA_PATH.mkdir(exist_ok=True)

    LOGGING_PATH = UNDATA_PATH.joinpath("logging")
    LOGGING_PATH.mkdir(exist_ok=True)
    LOGGING_FILE = LOGGING_PATH.joinpath("undata.log")

    logging.basicConfig(filename=LOGGING_FILE, level=logging.INFO, filemode="w")

    UNDATA_TOPICS_RAW_TREE_PATH = UNDATA_PATH.joinpath("topics-tree-raw")

    # if already exists, raise an error
    UNDATA_TOPICS_RAW_TREE_PATH.mkdir()

    UNDataTopics(download_path=UNDATA_TOPICS_RAW_TREE_PATH, logging_path=LOGGING_FILE)


if __name__ == "__main__":
    main()

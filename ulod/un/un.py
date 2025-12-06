import json
import logging
import os
import re
import zipfile
from io import BytesIO
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tqdm import tqdm


def zip(data, filename: Path, polars_options: dict):
    with zipfile.ZipFile(BytesIO(data)) as zip:
        file_list = zip.filelist
        for file in file_list:
            with zip.open(file) as file:
                pl.read_csv(file, **polars_options).write_csv(filename)


def excel(data, filename: Path, polars_options: dict):
    polars_options = {
        k: v for k, v in polars_options.items() if k not in {"ignore_errors"}
    }
    pl.read_excel(BytesIO(data), **polars_options).write_csv(filename)


def csv(data, filename: Path, polars_options: dict):
    pl.read_csv(BytesIO(data), **polars_options).write_csv(filename)


class UNDataTopics:
    def __init__(self):
        self.un_url = ("http://data.un.org/Handlers/ExplorerHandler.ashx",)

        ua = UserAgent()
        self.headers = (
            {
                "User-Agent": ua.firefox,
                "Referer": "http://data.un.org/Explorer.aspx",
                "Accept": "*/*",
                "Accept-Encoding": "gzip,deflate",
                "X-Requested-With": "XMLHttpRequest",
                "Host": "data.un.org",
            },
        )

    def download(self, download_path: Path, logging_path: Path):
        _url_topic = f"{self.un_url}?t=topics"
        url_mart = f"{self.un_url}?m={{mart_id}}"

        response = requests.get(
            self.un_url,
            headers=self.headers,
        )

        logging.basicConfig(filename=logging_path, level=logging.INFO, filemode="w")

        data = response.content.decode().replace("{Nodes:", '{"Nodes":', 1)
        data = json.loads(data)

        # Apply cleaning
        for n in tqdm(data["Nodes"][5:], desc="Topics: "):
            label_html = n["label"]

            soup = BeautifulSoup(label_html, "html.parser")

            topic = soup.find("span", class_="topic").get_text(strip=True)

            os.makedirs(download_path.joinpath(topic), exist_ok=True)

            # create data marts
            for child in n["childNodes"]:
                csoup = BeautifulSoup(child["label"], "html.parser")
                _owner = csoup.find("span", class_="martOwner").get_text(strip=True)
                name = csoup.find("span", class_="martName").get_text(strip=True)
                mart_id = child["martId"]

                mart_path = os.path.join(download_path, topic, name)
                os.makedirs(mart_path, exist_ok=True)

                # get datasets inside data mart (or inner data marts)
                response = requests.get(
                    url_mart.format(mart_id=mart_id), headers=self.headers
                )

                data = response.content.decode().replace("{Nodes:", '{"Nodes":', 1)
                data = json.loads(data)

                for node in tqdm(
                    data["Nodes"], desc="Data Marts: ", position=1, leave=False
                ):
                    self.download_data_mart(
                        node,
                        mart_id,
                        download_path.joinpath(topic, name),
                        tqdm_position=2,
                    )

    def download_data_mart(
        self, node: dict, mart_id: str, download_path: Path, tqdm_position: int
    ):
        polars_options = {
            "has_header": True,
            "infer_schema_length": 10_000,
            "ignore_errors": True,
            "truncate_ragged_lines": True,
        }

        logger = logging.getLogger(__name__)

        soup = BeautifulSoup(node["label"], "html.parser")

        if node["childNodes"]:
            folder_name = soup.find("span", class_="node").get_text(strip=True)

            folder_path = download_path.joinpath(folder_name)
            os.makedirs(folder_path, exist_ok=True)

            for child in tqdm(
                node["childNodes"],
                desc=f"Inner folder level {tqdm_position}: ",
                position=tqdm_position,
                leave=False,
            ):
                self.download_data_mart(child, mart_id, folder_path, tqdm_position + 1)
        else:
            name = soup.find("span", class_="node").get_text(strip=True)
            dataset_id = node["dataFilter"]
            if not dataset_id:
                return

            try:
                dataset_id = re.sub(r"docID:", "", dataset_id)
            except TypeError:
                print(name, dataset_id, download_path)

            candidate_urls = [
                f"http://data.un.org/Handlers/DownloadHandler.ashx?DataFilter={dataset_id}&DataMartId={mart_id}&Format=csv",
                f"http://data.un.org/Handlers/DocumentDownloadHandler.ashx?id={dataset_id}&t=bin",
            ]

            for url in candidate_urls:
                response = requests.get(
                    url.format(dataset_id, mart_id), headers=self.headers
                )
                if (
                    response.status_code != 200
                    or response.headers is None
                    or not response.headers.get("Content-Type")
                    or not response.headers.get("Content-Length")
                ):
                    continue

                content_type = response.headers.get("Content-Type").removeprefix(
                    "application/"
                )

                name = name.replace("/", "-")

                if len(name) > 255:
                    continue

                filename = download_path.joinpath(f"{name}.csv")

                try:
                    match content_type:
                        case "zip":
                            zip(response.content, filename, polars_options)
                        case "vnd.ms-excel":
                            excel(response.content, filename, polars_options)
                        case "csv":
                            csv(response.content, filename, polars_options)
                        case _:
                            raise ValueError(f"Unknown Content-Type: {content_type}")

                    logger.debug(
                        f"{response.status_code}, {dataset_id}, {mart_id}, {url.format(dataset_id, mart_id)}"
                    )

                except Exception as e:
                    logger.error(f"{dataset_id}, {mart_id}, {e}")

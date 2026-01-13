import json
import os
from pathlib import Path

import requests
from fake_useragent import UserAgent
from tqdm import tqdm


class WorldBankDataDownloader:
    def __init__(self):
        ua = UserAgent()
        self._accept = "*/*"
        self._content_type = "application/json"

        self.headers = {"User-Agent": ua.firefox, "Accept": "*/*"}

        self._url_api_search = "https://data360api.worldbank.org/data360/searchv2"
        self._url_api_indicators = (
            "https://data360api.worldbank.org/data360/indicators?datasetId={datasetId}"
        )
        self._url_indicator_metadata = "https://data360files.worldbank.org/data360-data/metadata/{database_id}/{indicator_id}.json"
        self._url_indicator_data = "https://data360files.worldbank.org/data360-data/data/{database_id}/{indicator_id}.csv"

    def get_indicator_resource(
        self,
        database_id: str,
        indicator_id: str,
        metadata_download_dst: Path,
        data_download_dst: Path,
    ):
        url_metadata = self._url_indicator_metadata.format(
            database_id=database_id, indicator_id=indicator_id
        )
        url_data = self._url_indicator_data.format(
            database_id=database_id, indicator_id=indicator_id
        )

        response: requests.Response = requests.get(url_metadata, headers=self.headers)
        metadata = json.loads(response.content.decode("utf-8-sig"))
        with open(metadata_download_dst, "w") as file:
            json.dump(metadata, file, indent=4)

        response: requests.Response = requests.get(url_data, headers=self.headers)
        with open(data_download_dst, "w") as file:
            file.write(response.content.decode())

    def get_indicators_list(self, database_id: str):
        response = requests.get(
            self._url_api_indicators.format(datasetId=database_id), headers=self.headers
        )

        assert response.status_code == 200, str(response)
        data = response.content.decode()
        indicators = json.loads(data)
        return indicators

    def get_database_ids(self):
        headers = self.headers | {"Content-Type": self._content_type}

        payload = {
            "count": True,
            "select": "series_description/database_id",
            "top": 1000,
        }

        payload = json.dumps(payload)
        response = requests.post(self._url_api_search, payload, headers=headers)

        data = response.content.decode()
        data = json.loads(data)

        database_ids = {
            obj["series_description"]["database_id"]
            for obj in data["value"]
            if obj["series_description"]["database_id"]
        }

        return database_ids

    def download_indicators(self, metadata_root_dst: Path, data_root_dst: Path):
        database_ids = self.get_database_ids()
        for database_id in tqdm(database_ids, desc="Database: "):
            try:
                indicators = self.get_indicators_list(database_id)

                for indicator_id in tqdm(
                    indicators, desc="Indicator: ", position=1, leave=False
                ):
                    metadata_dst = metadata_root_dst.joinpath(
                        database_id, f"{indicator_id}.json"
                    )
                    data_dst = data_root_dst.joinpath(
                        database_id, f"{indicator_id}.csv"
                    )

                    metadata_dst.parent.mkdir(parents=True, exist_ok=True)
                    data_dst.parent.mkdir(parents=True, exist_ok=True)
                    self.get_indicator_resource(
                        database_id, indicator_id, metadata_dst, data_dst
                    )
            except Exception as e:
                print(f"Failed with {database_id}: {e}")


def main():
    metadata_root_dst = Path(os.path.dirname(__file__), "testing", "metadata")
    data_root_dst = Path(os.path.dirname(__file__), "testing", "data")

    metadata_root_dst.mkdir(parents=True, exist_ok=True)
    data_root_dst.mkdir(parents=True, exist_ok=True)

    wbo = WorldBankDataDownloader()

    wbo.download_indicators(metadata_root_dst, data_root_dst)


if __name__ == "__main__":
    main()

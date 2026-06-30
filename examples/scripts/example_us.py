import sys
from pathlib import Path

sys.path.append(str(Path(__file__, "..", "..", "..", "src").resolve()))

from fake_useragent import UserAgent

from ulod.bulk import USDownloadConfig, datagov_download_datasets
from ulod.countries.us import US

ua = UserAgent()
headers = {"User-Agent": ua.firefox}
connection_kw = {"redirect": True, "timeout": 30}

download_destination = Path(__file__).resolve().parents[2] / "downloads" / "us"
download_destination.mkdir(parents=True, exist_ok=True)

client = US(headers=headers, connection_kw=connection_kw)
cfg = USDownloadConfig(
    download_destination=download_destination,
    mode="all",
    formats="csv",
    max_datasets=500_000,
    max_workers=10,
    max_resource_size=50 * 1024**2,
    verbose=True,
)

datagov_download_datasets(cfg, client)


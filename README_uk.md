# UK Open Data

### What is this tables collection?
This tables collection represents a partial snapshot of the UK Open Data portal https://www.data.gov.uk obtained through the [ULOD](https://github.com/nanni00/ULOD) library.

### When has it been collected?
This snapshot has been collected on 30 July 2026.

### What does it contain?
This snapshot contains 15759 of 27390 retrieved downloadable resources. 
Resources that have not been downloaded are mainly related to broken links and permission errors with remote servers protected by anti-crawling tools.

The tables are stored as parquet files, with high compression rate to reduce the storage requirement.

Note that these tables are fetched from a CKAN endpoint: resources and packages have a many-to-one relation.
Also, files are stored as \<packageID\>__\<resourceID\>.parquet

### Tree Structure
```
datasets/
└── parquet/                        # where parquet files are stored
logs/
└── download/                       # log files
metadata/
├── metadata.json                   # complete metadata JSON file of the packages/resources
├── metadata_retrieved_only.json    # metadata relative to only those datasets actually downloaded
└── rsc_url.json                    # list of <resource ID, resource URL> used for the download
```

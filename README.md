# Venom
This repository contains the source code for the framework *Venom* presented in the paper "Using Venom to Flip the Coin and Peel the Onion: Measurement Tool and Dataset for Studying the Bitcoin - Dark Web Synergy".

DOI: https://doi.org/10.1145/3714393.3726488.

## Components

### Web Crawling
The details of the web crawler implementation can be found in `crawler.py`.

### Bitcoin Data Fetching
With collected data from the crawler, one can fetch data about the discovered
Bitcoin addresses using `fetch.py`.

### Data Processing
To process data from the crawler or from a Bitcoin API, one can use `process.py`.

## Example Usage
First, we create a simple folder structure to conveniently save the output from each component.
```python
from venom import crawler, fetch, process
import os

CRAWLER_OUTPUT_DIR = "output/crawler"
FETCH_OUTPUT_DIR = "output/fetch"
PROCESS_OUTPUT_DIR = "output/process"

# (Create the directories if they do not already exist.)
os.makedirs(CRAWLER_OUTPUT_DIR, exist_ok=True)
os.makedirs(FETCH_OUTPUT_DIR, exist_ok=True)
os.makedirs(PROCESS_OUTPUT_DIR, exist_ok=True)
```

Then, we setup and run a crawler instance.
```python
# A file containing interesting keywords (one on each line) to generate seed addresses using Ahmia.
ahmia_keyword_path = "ahmia-keywords.txt"

# The Tor proxies, which will enable us to crawl onion addresses.
tor_proxies = {"http": "socks5h://localhost:9050", "https": "socks5h://localhost:9050"}

# The number of threads to use for the multithreaded crawler.
nr_of_threads = 10

kw_args = {
           "output_dir": CRAWLER_OUTPUT_DIR,
           "ahmia_keyword_path": ahmia_keyword_path, 
           "proxies": tor_proxies, 
           "nr_of_threads": nr_of_threads,
          }

# Initialize and run the crawler.
mt_dark_crawler = crawler.MultiThreadedDarkCrawler(**kw_args)
mt_dark_crawler.crawl()
```

Lastly, we process, fetch, and consolidate the different sources of data,
to create the dataset.
```python
crawler_data_path = os.path.join(CRAWLER_OUTPUT_DIR, "data.txt")

# Extract the Bitcoin addresses from the crawler data file.
btc_addrs_path = os.path.join(PROCESS_OUTPUT_DIR, "btc-addrs.txt")
process.save_btc_addrs_from_crawler(crawler_data_path, output_path=btc_addrs_path)

# Fetch data for each Bitcoin address from the Blockstream API.
blockstream_data_path = os.path.join(FETCH_OUTPUT_DIR, "blockstream.txt")
fetch.blockstream_fetch_all(btc_addrs_path, output_path=blockstream_data_path)

# Consolidate meta data for each Bitcoin address into a more parseable format.
btc_data_path = os.path.join(PROCESS_OUTPUT_DIR, "btc-data.txt")
process.consolidate_btc_data(blockstream_data_path, output_path=btc_data_path)

# Create the dataset.
dataset_path = os.path.join(PROCESS_OUTPUT_DIR, "dataset.csv")
process.create_dataset(crawler_data_path, btc_data_path, output_path=dataset_path)
```

The complete demo code can be found in `demo.py`.

## Cite
If you were to mention or use our framework in your work, we kindly ask you to reference the original paper:

*Lukas Ingemarsson, Karl Duckert Karlsson, and Niklas Carlsson, Using Venom to Flip the Coin and Peel the Onion: Measurement Tool and Dataset for Studying the Bitcoin - Dark Web Synergy, Proc. ACM Conference on Data and Application Security and Privacy (CODASPY), Pittsburgh, PA, June 2025.*

### BibTeX
```
@inproceedings{ingemarsson25using,
author = {Lukas Ingemarsson and Karl Duckert Karlsson and Niklas Carlsson},
title = {Using Venom to Flip the Coin and Peel the Onion: Measurement Tool and Dataset for Studying the Bitcoin - Dark Web Synergy},
booktitle = {Proc. ACM Conference on Data and Application Security and Privacy (CODASPY)},
address = {Pittsburgh, PA},
month = {June},
year = {2025}
}
```

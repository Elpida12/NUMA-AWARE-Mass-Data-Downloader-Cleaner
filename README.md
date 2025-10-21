# NUMA-AWARE-Mass-Data-Downloader-Cleaner
### Unofficial Version

Refactored but unfinished version of a project designed to maximize efficiency, speed, and flexibility when downloading hundreds of terabytes of raw data from public data sources like CommonCrawl.org. This project finds all cores on dual-socket servers, assign's each core to their respective memory channel (To avoid Cross-NUMA conflicts), and downloads/cleans/stores raw data.  

Primary feature:
    - NUMA-aware
    - Efficiency and speed when dealing with hundreds of terabytes of data
    - Each core acts as it's own worker, meaning they all report back to main.py, which triggers it to respond accordingly depending on the signal (finished cleaning a file, error cleaning a file, position assignment, etc.)

This has been tested on a Dell PowerEdge R730xd with 128GB of RAM (64GB per channel) and x2 Intel Xeon E5-2698 V4 CPU's

Updates to this project will be frequent.

# EdgeMiner

EdgeMiner - Distributed Process Mining at the Data Sources
The implementation uses the Alpha Miner as extension for EdgeMiner.

## Setup
Before starting EdgeMiner go to .env and change FILE_PATH to the path of an event log (.csv or .xes) where the case ID is named "case:concept:name", the activity is named "concept:name" and the timestamp is named "time:timestamp".

Next, change OUTPUTS_PATH to a path for your output-csv-files.

In the activity node's Dockerfile use

    CMD ["python", "-u", "./improved_activity_node.py"]

if you want to use Most-Frequent-Predecessor Requesting, otherwise use

    CMD ["python", "-u", "./activity_node.py"].

Also, if the unoptimized activity node is in use, uncomment the last rows in `activity_node.py`.

## Run

1. Start docker deamon.

2. To build the docker images for the central node as well as for the activity nodes, run the `build_docker_images` script.

3. Run `python3 main.py`.
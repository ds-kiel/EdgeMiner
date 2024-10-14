# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import threading
import time
import traceback

import docker
from dotenv import load_dotenv

from event_log_handler import EventLogHandler

load_dotenv()

BASE_SERVER_PORT = int(os.getenv('BASE_SERVER_PORT'))
GROUP_NAME = str(os.getenv('GROUP_NAME'))
DOCKER_LABEL = str(os.getenv('DOCKER_LABEL'))
ACTIVITY_NODE_IMAGE = str(os.getenv('ACTIVITY_NODE_IMAGE'))
CENTRAL_NODE_IMAGE = str(os.getenv('CENTRAL_NODE_IMAGE'))
OUTPUTS_PATH = str(os.getenv('OUTPUTS_PATH'))
FILE_PATH =  str(os.getenv('FILE_PATH'))
OUTPUTS_PATH = str(os.getenv('OUTPUTS_PATH'))


# --- Helper Functions ---

def attach_logs(container):
    def _print(name, stream):
        for line in stream:
            print(name + ": " + line.decode('utf8').strip())

    t = threading.Thread(target=_print, args=(container.name, container.attach(logs=True, stream=True)))
    t.daemon = True
    t.start()


def remove():
    # Remove server containers
    try:
        containers = client.containers.list(filters={"label": DOCKER_LABEL}, all=True)
        for container in containers:
            container.remove(force=True)
    except Exception as exc:
        print(exc)

    # Remove the network
    try:
        nets = client.networks.list(names=[DOCKER_LABEL + "_net"])
        for net in nets:
            net.remove()
    except Exception as exc:
        print(exc)


def get_server_name_list_str():
    server_str =""
    for i in range(NUM_SERVERS-1):
        server_str += f"{DOCKER_LABEL}_activity_node_{i},"

    server_str += f"{DOCKER_LABEL}_central_node"
    return server_str


def get_server_ip_list_str():
    server_str = ""
    for i in range(NUM_SERVERS):
        server_str += f"127.0.0.1:{BASE_SERVER_PORT+i}"
        if i != NUM_SERVERS-1:
            server_str += ","
    return server_str


# ---

client = docker.from_env()

# Set up Event Log Handler
elh = EventLogHandler()
NUM_SERVERS = elh.get_activity_count() + 1      # +1 for the central node
server_activity_mapping = elh.get_server_id_to_activity_name_mapping()

# Remove any running container instances
remove()

# Add the network
network = client.networks.create(DOCKER_LABEL + "_net", driver="bridge")

# Add the containers
for server_id in range(0, NUM_SERVERS):
    if server_id == NUM_SERVERS-1:
        # Run container for central node
        server_name = DOCKER_LABEL + "_central_node"
        server_container = client.containers.run(CENTRAL_NODE_IMAGE,
                                                detach=True,
                                                labels={DOCKER_LABEL: 'central_node'},
                                                name=server_name,
                                                ports={'80': ('127.0.0.1', BASE_SERVER_PORT + server_id)},
                                                network= DOCKER_LABEL + "_net",
                                                volumes={
                                                    OUTPUTS_PATH: {
                                                        'bind': '/application/outputs',
                                                        'mode': 'rw'}},
                                                environment={
                                                    "SERVER_NAME_LIST": get_server_name_list_str(),
                                                    "SERVER_IP_LIST": "", # not using IPs right now
                                                    "SERVER_ID": server_id,
                                                    "SERVER_ACTIVITY_MAPPING": server_activity_mapping})
    else:
        # Run container for activity node
        server_name = f"{DOCKER_LABEL}_activity_node_{server_id}"
        server_container = client.containers.run(ACTIVITY_NODE_IMAGE,
                                                detach=True,
                                                labels={DOCKER_LABEL: 'activity_node'},
                                                name=server_name,
                                                ports={'80': ('127.0.0.1', BASE_SERVER_PORT + server_id)},
                                                network=DOCKER_LABEL + "_net",
                                                volumes={
                                                    OUTPUTS_PATH: {
                                                        'bind': '/application/outputs',
                                                        'mode': 'rw'}},
                                                environment={
                                                    "SERVER_NAME_LIST": get_server_name_list_str(),
                                                    "SERVER_IP_LIST": "", # not using IPs right now
                                                    "SERVER_ID": server_id,
                                                    "ACTIVITY_NAME": server_activity_mapping[str(server_id)],
                                                    "FILE_PATH": FILE_PATH})
    attach_logs(server_container)

# ---

print("CTRL-C to shutdown...")
try:
    time.sleep(7)

    # Run until shutdown
    while True:

        time.sleep(.1)
        try:
            # Trigger Events and pass them to the according activity node by HTTP request
            res = elh.trigger_next_event()
            if res:
                break

        except Exception as e:
            print("[MAIN FCT ERROR] " + str(e))
            print(traceback.format_exc())

except KeyboardInterrupt:
    pass

print("Shutting down...")
remove()
print("Finished")
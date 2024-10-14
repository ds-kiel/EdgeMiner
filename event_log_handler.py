# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import os
import traceback

import requests
from dotenv import load_dotenv
from pm4py.objects.petri_net.importer import importer as pnml_importer

from alpha_miner_original import run_original_alpha_miner
from auxiliaries.event_log_adjuster import no_doubled_timestamps
from auxiliaries.file_reader import read_event_log
from equality_check import equality_check

load_dotenv()
BASE_SERVER_PORT = int(os.getenv('BASE_SERVER_PORT'))
DOCKER_LABEL = str(os.getenv('DOCKER_LABEL'))
IP_NO_PORT = 'http://127.0.0.1:'



class EventLogHandler:
    """
    The ``EventLogHandler`` keeps track of all event log related.
    """

    def __init__(self):
        file_path = str(os.getenv('FILE_PATH'))
        self.output_file_name = os.path.splitext(os.path.basename(file_path))[0]

        self.event_log_df = read_event_log(file_path)
        self.event_log_df = self.event_log_df.filter(items=["case:concept:name", "concept:name", "time:timestamp"]) # Caution: filter changes order
        self.event_log_df = self.event_log_df.sort_values(["case:concept:name","time:timestamp"])
        self.event_log_df = no_doubled_timestamps(self.event_log_df)

        self.num_events = self.event_log_df.shape[0] # number of rows in dataframe

        self.current_event = 0
        self.server_id_to_activity_name_mapping, self.activity_name_to_server_id_mapping = self.compute_activities_and_mapping()

        # Set up file to count queried nodes for each event
        with open(f"outputs/{self.output_file_name}_opt.csv" ,"w", encoding="utf-8") as f:
            f.write("case:concept:name;concept:name;time:timestamp;requested_nodes\n")


    def get_server_id_to_activity_name_mapping(self):
        """
        Returns the mapping from server IDs to activitiy names.
        """
        return self.server_id_to_activity_name_mapping


    def get_activity_name_to_server_id_mapping(self):
        """
        Returns the mapping from activity names to server IDs.
        """
        return self.activity_name_to_server_id_mapping


    def get_activity_count(self):
        """
        Returns how many activities are in the event log.
        """
        return len(self.server_id_to_activity_name_mapping)


    def compute_activities_and_mapping(self):
        """
        Computes a mapping from server IDs to activity names and the other way around.
        """
        activities_in_traces = set(row["concept:name"] for _,row in self.event_log_df.iterrows())
        mapping_1 = {}
        mapping_2 = {}
        for i, activity_name in enumerate(activities_in_traces):
            mapping_1[str(i)] = activity_name
            mapping_2[activity_name] = i
        return mapping_1, mapping_2


    def trigger_next_event(self):
        """
        Triggers the next event of the event log by sending a HTTP request to the corresponding activity node.
        """
        try:
            # Request process model from central node if there is no event left
            if self.current_event == self.num_events:

                print("Requesting process model")
                res = requests.get(f"{IP_NO_PORT}{BASE_SERVER_PORT + self.get_activity_count()}/process_model", data=None, timeout=5)
                response_content = res.text

                if response_content:
                    response_content = json.loads(response_content)
                    response_content['net'] = response_content['net'].encode("utf-8")

                    net_1 = pnml_importer.deserialize(response_content['net'])
                    print(f"\nEdgeAlpha \n{net_1[0]}")

                    # Compare output to original miner
                    net_2 = run_original_alpha_miner(self.event_log_df)
                    equality = ">>> Equal <<<" if equality_check(net_1,net_2) else "\n\n>>> Not equal <<<"
                    print(equality)
                return True

            # Read the next event
            row = self.event_log_df.iloc[self.current_event]
            activity = self.activity_name_to_server_id_mapping[row["concept:name"]]
            case_id = row["case:concept:name"]
            timestamp = row["time:timestamp"]

            # Trigger event by sending the event data to the corresponding activity node
            data = {'activity_id': int(activity), "case_id": str(case_id), "timestamp": timestamp}
            print(f"Triggering event   {data}      activity name  {row['concept:name']}")

            requests.post(IP_NO_PORT + f"{BASE_SERVER_PORT + int(activity)}/trigger_event", data=data, timeout=5)

            self.current_event += 1

            return False

        except Exception as e:
            print("[EVENTLOG HANDLER ERROR] " + str(e))
            print(traceback.format_exc())

# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import os
import time
import traceback
from datetime import datetime

import pytz
import util
from bottle import Bottle, request
from data_structures.activity_correlations import ActivityCorrelations
from data_structures.neighbors import NeighborhoodCollection
from data_structures.start_activities import StartActivities
from dateutil.parser import parse
from paste import httpserver

UTC = pytz.UTC
NUM_THREADS = 10



class ActivityNode(Bottle):
    """
    An instance of the ActivityNode class represents a sensor node that detects exactly
    one kind of activity of a process. An event is simulated by the receiving of a
    HTTP request to ``/trigger_event``.
    """

    def __init__(self, ID, IP, activity_name, server_ip_list, server_name_list):
        super(ActivityNode, self).__init__()

        self.id = int(ID)
        self.activity_name = activity_name
        self.ip = str(IP)

        # communication data
        self.central_node_name = server_name_list.pop() # direct connection
        self.server_ip_list = server_ip_list
        self.server_name_list = server_name_list

        # data structures
        self.neighbors = NeighborhoodCollection()
        self.start_activities = StartActivities(self)
        self.activity_correlations = ActivityCorrelations(self)

        # Initialize footprint matrix, start activities and neighbors
        size = (len(self.server_name_list),len(self.server_name_list))
        self.activity_correlations.set_variables(size)

        # for counting predecessor requests
        self.number_asked_for_predecessor = 0

        # routes
        self.post('/trigger_event', callback=self.trigger_event)
        self.get('/case_event_data', callback=self.get_case_event_data_by_request)
        self.get('/current_data', callback=self.get_current_data)
        self.post('/get_chosen', callback=self.get_chosen)


    def get_chosen(self):
        """
        Gets called to tell a node that it is the predecessor of a node in a certain case.
        keys: 'case_id', 'activity_id', 'req_timestamp', 'chosen_timestamp'
        """
        try:

            req = request.forms

            if req:
                case_id = str(req.get('case_id'))
                successor = int(req.get('activity_id'))
                successor_timestamp = req.get('req_timestamp')
                own_timestamp = req.get('chosen_timestamp')

                added = self.neighbors.add_succ_to_neighborhood(case_id, own_timestamp, successor, successor_timestamp)

                if not added:
                    return False

            # Add direct sucction between self and succ to FM
            self.activity_correlations.add_direct_succession(successor)
            return True

        except Exception as e:
            print(f"[ACTIVITY NODE {self.id} ERROR] {e}")
            print(traceback.format_exc())
            raise e


    def get_current_data(self):
        """
        FM Builder node sends request to `/current_data`.
        Returns currently stored start and end activities as well as its FM.
        """
        try:
            end_activities = []
            for neighborlist in self.neighbors.all.values():
                for neighbor in neighborlist:
                    # iterate over successors, as soon as there is a case in which the node does not have
                    # a successor this node is an end node
                    if neighbor.succ not in range(len(self.server_name_list)):
                        end_activities = [self.id]
                        break

            data =  {
                'start_activities': self.activity_correlations.get_sendable_is_start_vector(),
                'end_activities': end_activities,
                'seq_nmbr_vector': self.activity_correlations.get_sendable_seq_nmbr_vector(),
                'fm': self.activity_correlations.get_sendable_footprint_matrix()
                }
            return data

        except Exception as e:
            print(f"[ACTIVITY NODE {self.id} ERROR] {e}")
            print(traceback.format_exc())
            raise e


    def get_case_event_data_by_request(self):
        """
        Gets called by request to /case_event_data.
        The function calls get_case_event_data to return the event data
        to the given case_id if fitting event data exists.
        """
        data = request.query
        case_id = str(data.case_id)
        req_timestamp = data.timestamp

        if case_id and req_timestamp:
            return self.get_case_event_data(case_id, req_timestamp)
        else:
            print("Something went wrong with request!")


    def get_case_event_data(self, case_id, req_timestamp):
        """
        If the node has a fitting event without a successor for the given case_id,
        it returns the event data.
        """
        # print(f"Asking node {self.id}")
        try:
            if str(case_id) in self.neighbors.all:
                neighbor_list = self.neighbors.all[str(case_id)]
                predecessor = None

                for neighbor in reversed(neighbor_list): # because we want the one that is closest to the timestamp

                    if (type(neighbor.succ) != int and neighbor.event_timestamp < req_timestamp) \
                        or (neighbor.succ and neighbor.event_timestamp < req_timestamp < neighbor.succ_timestamp): # second part is not necessary for synchr comm
                        # print(f"Succ data    succ {neighbor.succ}   succ timestamp {neighbor.succ_timestamp}")
                        predecessor = {
                            'case_id': case_id,
                            'activity_id': self.id,
                            'timestamp': neighbor.event_timestamp
                            }
                        break

                    if neighbor.event_timestamp > req_timestamp:
                        break

                if predecessor:
                    return json.dumps(predecessor)
        except Exception as e:
            print(f"[ACTIVITY NODE {self.id} ERROR] {e}")
            print(traceback.format_exc())
            raise e


    def pick_predecessor(self, case_id, predecessor_list, requester_timestamp):
        """
        Asking for predecessor activity node in specific case.
        predecessor_list is a list of dicts with keys 'case_id', 'activity_id', 'timestamp'
        """
        latest_timestamp = datetime.min.replace(tzinfo=UTC)
        latest_activity = None

        # look for event with latest timestamp before own timestamp
        for pred in predecessor_list:
            pred_case_id = pred["case_id"]
            pred_activity_id = pred["activity_id"]
            pred_timestamp = pred["timestamp"]

            if pred_case_id == case_id:
                print(f"req ts: {requester_timestamp}   pred ts: {pred_timestamp}   latest: {latest_timestamp}")
                if parse(requester_timestamp) > parse(pred_timestamp) > latest_timestamp:
                    latest_timestamp = parse(pred_timestamp)
                    latest_activity = pred_activity_id

        # if no predecessor event could be found None is returned
        if latest_timestamp == datetime.min.replace(tzinfo=UTC):
            print("No predecessor")
            return

        self.inform_chosen_node(case_id, requester_timestamp, latest_activity, latest_timestamp)
        print(f"Chose {latest_activity} for case {case_id} (preceding node {self.id})")

        return latest_activity, latest_timestamp


    def inform_chosen_node(self, case_id, requester_timestamp, chosen_activity_id, chosen_timestamp):
        """
        Inform chosen node that it sensed the predecessor event of a given event.
        """
        # inform chosen node
        data = {
            'case_id': case_id,
            'activity_id': self.id,
            'req_timestamp': requester_timestamp,
            'chosen_timestamp': chosen_timestamp
            }
        server_name = self.server_name_list[chosen_activity_id]
        util.contact_another_server(server_name, '/get_chosen', 'POST', data)


    def ask_for_predecessor(self, activity, case_id, timestamp):
        """
        Node contacts all other nodes and asks for potential predecessor events for a given event.
        """
        # Ask for predecessor
        predecessor_list = []

        for server_id in range(len(self.server_name_list)):
            predecessor = self.ask_node_for_predecessor(server_id, activity, case_id, timestamp)

            if predecessor:
                predecessor_list.append(predecessor) # list of dicts with keys 'case_id', 'activity_id', 'timestamp'

        chosen_pred_data = self.pick_predecessor(case_id, predecessor_list, timestamp)
        return chosen_pred_data


    def ask_node_for_predecessor(self, pred_activity_id, activity_id, case_id, timestamp):
        """
        A node with a given ID is contacted and asked for a potential predecessor event.
        If the given ID is the ID of the node itself, it checks in its own storage without
        contacting another node.
        """

        predecessor = None
        params = {'activity':activity_id, 'case_id': case_id, 'timestamp': timestamp}

        if pred_activity_id != self.id:
            self.number_asked_for_predecessor += 1

            _, res = util.contact_another_server(self.server_name_list[pred_activity_id], '/case_event_data', 'GET', params=params)

            if res.text:
                predecessor = json.loads(res.text)

        else:
            # check own events
            res = self.get_case_event_data(case_id, timestamp)

            if res:
                predecessor = json.loads(res)

        return predecessor


    def trigger_event(self):
        """
        When an event is triggered, the node asks the other nodes for the predecessor event.
        """
        try:
            msg = request.forms

            if msg["activity_id"] and msg["case_id"] and msg["timestamp"]:

                case_id = str(msg["case_id"])
                activity = int(msg["activity_id"])
                timestamp = msg["timestamp"]

                if activity != self.id:
                    print("This event is not supposed to be triggered by me!")
                    return

                print(f"Case {case_id}: Activity {activity} was triggered at {timestamp}.")

                chosen_pred_data = self.ask_for_predecessor(activity, case_id, timestamp)

                # write number of requested nodes to file
                file_path = str(os.getenv('FILE_PATH'))
                output_file_name = os.path.splitext(os.path.basename(file_path))[0]
                filename=f"/application/outputs/{output_file_name}_opt.csv"
                with open(filename, "a") as f:
                    f.write(f"{case_id};{activity};{timestamp};{self.number_asked_for_predecessor}\n")

                self.number_asked_for_predecessor = 0

                # If there is no predecessor: Event is start event
                if not chosen_pred_data:
                    self.start_activities.add_own_start_activity(case_id)
                    self.neighbors.add_neighborhood(case_id, timestamp)

                # Otherwise: update relations, neighbors and start activities
                else:
                    pred_activity_id, pred_timestamp = chosen_pred_data
                    self.neighbors.add_neighborhood(case_id, timestamp, pred_activity_id, pred_timestamp)

            else:
                print("Something went wrong! The request did not include one of the following fields: activity, case_id, timestamp")

        except Exception as e:
            print(f"[ACTIVITY NODE {self.id} ERROR] {e}")
            print(traceback.format_exc())
            raise e


# Uncomment if using unimproved activity node
# Sleep a bit to allow logging to be attached
# time.sleep(2)

# server_list = os.getenv('SERVER_NAME_LIST').split(',')
# server_ip_list = os.getenv('SERVER_IP_LIST').split(',')
# own_id = int(os.getenv('SERVER_ID'))
# own_ip = server_list[own_id]
# own_name = os.getenv('ACTIVITY_NAME')

# server = ActivityNode(own_id, own_ip, own_name, server_ip_list, server_list)

# print(f"#### Starting Activity Node with Id {own_id}")
# httpserver.serve(server, host='0.0.0.0', port=80, threadpool_workers=NUM_THREADS, threadpool_options={"spawn_if_under": NUM_THREADS})

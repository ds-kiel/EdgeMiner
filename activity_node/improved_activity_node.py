# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import time
from activity_node import ActivityNode
from paste import httpserver

NUM_THREADS = 10


class ImprovedActivityNode(ActivityNode):


    def __init__(self,own_id, own_ip, own_name, server_ip_list, server_list):
        super(ImprovedActivityNode, self).__init__(own_id, own_ip, own_name, server_ip_list, server_list)
        self.ask_first_nmbr = len(self.server_name_list)
        self.most_frequent = [] # form of elements:   (activity_id, count)


    # Overwriting ActivityNode method ask_for_predecessor
    def ask_for_predecessor(self, activity, case_id, timestamp):
        """
        Node contacts nodes for potential predecessors.
        It contacts the nodes according to the likelihood of them being the predecessor.
        If no predecessor event was found, all others are contacted.
        """
        already_asked = []

        ## Ask most frequent predecessors first
        # Determine most frequent predecessors
        most_frequent_subset = []
        if self.ask_first_nmbr >= len(self.most_frequent):
            most_frequent_subset = [x[0] for x in self.most_frequent]

        else:
            most_frequent_subset = [x[0] for x in self.most_frequent[:self.ask_first_nmbr]]

        if self.id in most_frequent_subset: most_frequent_subset.remove(self.id)
        most_frequent_subset.insert(0, self.id) # this way not wasting requests


        # Send requests to the most frequent predecessors until fitting predecessor is found
        found = False
        for pred_activity_id in most_frequent_subset:

            predecessor = self.ask_node_for_predecessor(pred_activity_id, activity, case_id, timestamp)
            # print(predecessor)

            if predecessor:
                found = True
                # print("Found pred from most freq list!")
                break

            already_asked.append(pred_activity_id)

        if found:
            # Increase count of predecessor and return its info
            self.increase_predecessor_count(predecessor["activity_id"])

            # Inform chosen activity node
            self.inform_chosen_node(case_id, timestamp, predecessor["activity_id"], predecessor["timestamp"])

            return predecessor["activity_id"], predecessor["timestamp"]

        # Determine which activity nodes have not been asked yet
        not_asked_yet = list(range(len(self.server_name_list)))
        already_asked.sort(reverse=True)
        for i in already_asked:
            not_asked_yet.pop(i)


        ## If no predecessor was found ask all others (not the ones that were already asked)
        predecessor_list = []
        for server_id in not_asked_yet:
            predecessor = self.ask_node_for_predecessor(server_id, activity, case_id, timestamp)

            if predecessor:
                predecessor_list.append(predecessor) # list of dicts with keys 'case_id', 'activity_id', 'timestamp'

        chosen_pred_data = self.pick_predecessor(case_id, predecessor_list, timestamp)
        if chosen_pred_data:
            self.increase_predecessor_count(chosen_pred_data[0])

        return chosen_pred_data


    def increase_predecessor_count(self, chosen_activity_id):
        """
        The data structure `most_frequent` is updated whenever a predecessor activity was found.
        """
        increased = False

        for i, (pred_activity, count) in enumerate(self.most_frequent):
            if pred_activity == chosen_activity_id:
                self.most_frequent[i] = (pred_activity, count+1)
                increased = True
                break

        if not increased:
            self.most_frequent.append((chosen_activity_id, 1))

        # sort list
        self.most_frequent = sorted(self.most_frequent, key=lambda tup: tup[1], reverse=True)


# Sleep a bit to allow logging to be attached
time.sleep(2)

server_list = os.getenv('SERVER_NAME_LIST').split(',')
server_ip_list = os.getenv('SERVER_IP_LIST').split(',')
own_id = int(os.getenv('SERVER_ID'))
own_ip = server_list[own_id]
own_name = os.getenv('ACTIVITY_NAME')

server = ImprovedActivityNode(own_id, own_ip, own_name, server_ip_list, server_list)

print(f"#### Starting Activity Node with Id {own_id} and IP {own_ip}")
httpserver.serve(server, host='0.0.0.0', port=80, threadpool_workers=NUM_THREADS, threadpool_options={"spawn_if_under": NUM_THREADS})
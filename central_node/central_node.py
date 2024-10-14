# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import itertools
import json
import os
import pickle
import time
import traceback
from ast import literal_eval
from functools import reduce

import numpy as np
import util
from bottle import Bottle
from paste import httpserver
from pm4py.objects.petri_net.exporter import exporter
from pm4py.objects.petri_net.obj import Marking, PetriNet
from pm4py.objects.petri_net.utils import petri_utils

from central_node_auxiliaries import (
    find_transition_to_activity_id, get_all_subsets, is_causality_pair,
    is_independent_set, is_subset, print_with_name_instead_of_id)

NUM_THREADS = 10


class CentralNode(Bottle):
    """
    The Central Node is responsible for collecting all data from the activity nodes,
    and using it to calculate a current process model.
    """

    def __init__(self, ID, IP, server_list, server_ip_list, server_activity_mapping) -> None:
        super(CentralNode, self).__init__()

        self.id = int(ID)
        self.ip = str(IP)

        self.server_name_list = server_list
        self.server_name_list.pop() # get rid of the central node
        self.server_ip_list = server_ip_list
        self.server_ip_list.pop() # get rid of the central node

        self.server_activity_mapping = server_activity_mapping
        self.activities = [int(x) for x in list(self.server_activity_mapping.keys())]

        self.data_list = []

        self.get('/process_model', callback=self.get_process_model)


    # GET "/process_model"
    def get_process_model(self) -> tuple[PetriNet,Marking,Marking]:
        """
        All activity nodes' data is requested, then merged and the original Alpha Miner
        algorithm is continued on the merged data.
        It returns the resulting Petri Net.
        """
        try:
            print("Receiving request to form process model")

            # Request data from each node and add it to data_list
            for server_name in self.server_name_list:
                succ, res = util.contact_another_server(server_name, '/current_data', 'GET', None)
                if succ and res:

                    data = res.text
                    if data:
                        data = json.loads(data)
                        data['start_activities'] = pickle.loads(data['start_activities'].encode('latin-1'))
                        data['end_activities'] = set(data['end_activities'])
                        data['seq_nmbr_vector'] = pickle.loads(data['seq_nmbr_vector'].encode('latin-1'))
                        data['fm'] = pickle.loads(data['fm'].encode('latin-1'))
                        self.data_list.append(data)

            # Merge results at central node
            merged_data = self.merge_node_data(self.data_list)

            # Calculate the (A,B)-pair set and minimizes it
            set_pairs, self_loops = self.calculate_pairs(merged_data["fm"])
            set_pairs = self.minimize_pairs(set_pairs, self_loops)

            # Calculate resulting Petri Net
            net, start, end = self.form_petri_net(set_pairs, merged_data["start_activities"], merged_data["end_activities"])

            # Convert the PetriNet object to a pnml string and return it in response
            pnml_string = exporter.serialize(net,start,end)
            pnml_string = pnml_string.decode("utf-8")

            return json.dumps({'net': pnml_string})

        except Exception as e:
            print(f"[CENTRAL NODE ERROR]  {e}")
            print(traceback.format_exc())


    def merge_node_data(self, data:list[dict]) -> dict:
        """
        The input parameter "data" is list of items of the form
        {"start_activities": set({int}), "FM": [pred][succ], "end_activities": set({int}})}.
        Unites the start activity sets and the end activity sets.
        Merges the footprint matrices.
        Returns the merged data.
        Sequence numbers are not necessary for the synchronous communication case.
        """
        # merging start activity sets
        start_activities_w_seq_nmbrs = [np.column_stack((el["start_activities"],el["seq_nmbr_vector"])) for el in data]
        merged_start_activities = reduce(self.merge_start_activities, start_activities_w_seq_nmbrs)
        merged_start_activities = set(k for k,value in enumerate(merged_start_activities) if value[0])

        # merging end activity sets
        end_activities_list = [el["end_activities"] for el in data]
        merged_end_activities = set(reduce(lambda a,b: a.union(b), end_activities_list))

        # merging the FMs
        fm_list_w_seq_nmbrs = [np.column_stack((el["fm"], el["seq_nmbr_vector"])) for el in data] # last column is always seq numbers
        merged_fms = reduce(self.merge_two_matrices, fm_list_w_seq_nmbrs)
        # converting FM to a 0/1-matrix
        merged_fms = self.convert_footprint_matrix(merged_fms)

        merged_data = {"start_activities": merged_start_activities,"fm": merged_fms, "end_activities": merged_end_activities}
        return merged_data


    def merge_start_activities(self, start_activities_1:np.ndarray, start_activities_2:np.ndarray) -> np.ndarray:
        """
        Taking two lists of start activity tuples and returning a merged one with all entries,
        that correspond to an activity which is only part of one of the lists and additionally
        the doubled entries, but here only the ones with the higher sequence number.

        update: array have entries of arrays with first the start activity bool and second the sequence number
        """
        for k, is_start_w_seq_nmbr_2 in enumerate(start_activities_2):

            kth_seq_nmbr_2 = is_start_w_seq_nmbr_2[1]

            # kth_start_activity_1 = start_activities_1[k][0]
            kth_seq_nmbr_1 = start_activities_1[k][1]

            if kth_seq_nmbr_1 < kth_seq_nmbr_2:
                # update
                start_activities_1[k] = start_activities_2[k]
        return start_activities_1


    def convert_footprint_matrix(self, fm:np.ndarray) -> np.ndarray:
        """
        Taking a numpy array where an entry denotes how often a direct succession
        occurred and converting it into an array which denotes whether a direct succession
        occurred.
        More precisely: 0 if the amount was 0, otherwise 1.
        """
        new_matrix = np.zeros((fm.shape[0],fm.shape[1]-1),dtype="int")
        for i, row_and_seq_nmbr in enumerate(fm):
            for j, el in enumerate(row_and_seq_nmbr):
                if j != len(row_and_seq_nmbr)-1:
                    new_matrix[i][j] = 0 if el == 0 else 1

        return new_matrix


    def merge_two_matrices(self, matrix_1:np.ndarray, matrix_2:np.ndarray) -> np.ndarray:
        """
        Taking two numpy arrays and returning a merged one.
        Always choosing the entry with the higher sequence number.
        """
        for i, row_with_seq_nmbr in enumerate(matrix_1):
            seq_nmbr_1 = row_with_seq_nmbr[-1]

            row_2 = matrix_2[i]
            seq_nmbr_2 = matrix_2[i][-1]

            if seq_nmbr_2 > seq_nmbr_1:
                matrix_1[i] = row_2
        return matrix_1


    def calculate_pairs(self, fm):
        """
        Creating the set of (A,B) pairs to the given direct successions and activities  and returning it.
        Using causality and choice relations.
        """
        causalities = set()     # a > b AND NOT b > a
        choices = set()         # NOT a > b AND NOT b > a
        parallels = set()       # a > b AND b > A
        self_loops = set()      # a > a

        # iterating through the FM and filling up the relation sets
        for i,_ in enumerate(fm):
            for j in range(i,len(fm)):
                if fm[i][j] == 1 and fm[j][i] == 1:
                    parallels.add((i,j))

                    if i == j:
                        self_loops.add(i)

                elif fm[i][j] == 0 and fm[j][i] == 0:
                    choices.add((i,j))
                    choices.add((j,i))

                elif fm[i][j] == 1:
                    causalities.add((i,j))

                elif fm[j][i] == 1:
                    causalities.add((j,i))

        print_with_name_instead_of_id(self.server_activity_mapping, causalities, parallels)

        # calculating the set of (A,B)-pairs, here p_set
        activities = set(self.activities)

        set_of_pairs = set()
        activity_subsets = get_all_subsets(activities)
        independent_activity_sets = [subset for subset in activity_subsets if is_independent_set(subset, choices, self_loops)]

        for a, b in itertools.product(independent_activity_sets, independent_activity_sets):
            if is_causality_pair(a, b, causalities):
                set_of_pairs.add((frozenset(a), frozenset(b)))

        return set_of_pairs, self_loops


    def minimize_pairs(self, set_of_pairs, self_loops):
        """
        Takes a (A,B)-pair set and a list of self-loop activities.
        It removes all pairs that are subsets of other pairs and also removes self-loop
        activities from the sets and makes sure the resulting pair is still not a subset.
        """
        combs = itertools.combinations(set_of_pairs, 2)

        # remove subsets of pairs
        for (p_1, p_2) in combs:
            if is_subset(p_1,p_2):
                set_of_pairs.discard(p_1)
            elif is_subset(p_2,p_1):
                set_of_pairs.discard(p_2)

        # remove self-loops
        # e.g. if (a,b),(b,c) in a_b_pairs_set, and (b,b) in Parallel, then we need to remove (a,b),(b,c)
        # (a,b) is equal to (a,bb), also b||b, thus a and bb cannot make a pair, only "#" relations can.
        # to_be_deleted = set()

        for activity in self_loops:

            set_of_pairs_copy = set_of_pairs.copy()

            for (a,b) in set_of_pairs_copy:

                # self loop only in first set
                if activity in a and activity not in b:
                    # remove pair from set and self loop from pair
                    # if still not subset, add to pair set
                    set_of_pairs.discard((a,b))
                    a_new = a.difference(frozenset([activity]))
                    if a_new:
                        # check if it is subset now
                        subset = False
                        fresh_set_of_pairs_copy = set_of_pairs.copy()
                        for pair in fresh_set_of_pairs_copy:
                            if is_subset((a_new,b),pair):
                                subset = True
                        if not subset:
                            set_of_pairs.add((a_new,b))

                # self loop only in second set
                elif activity in b and activity not in a:
                    # remove pair from set and self loop from pair
                    # if still not subset, add to pair set
                    set_of_pairs.discard((a,b))
                    b_new = b.difference(frozenset([activity]))
                    if b_new:
                        # check if it is subset now
                        subset = False
                        fresh_set_of_pairs_copy = set_of_pairs.copy()
                        for pair in fresh_set_of_pairs_copy:
                            if is_subset((a,b_new),pair):
                                subset = True
                        if not subset:
                            set_of_pairs.add((a,b_new))

                # self loop in both sets
                elif activity in a and activity in b:
                    # remove pair from set and self loop from pair
                    # if still not subset, add to pair set
                    set_of_pairs.discard((a,b))
                    a_new = a.difference(frozenset([activity]))
                    b_new = b.difference(frozenset([activity]))
                    if b_new and a_new:
                        # check if it is subset now
                        subset = False
                        fresh_set_of_pairs_copy = set_of_pairs.copy()
                        for pair in fresh_set_of_pairs_copy:
                            if is_subset((a_new,b_new),pair):
                                subset = True
                        if not subset:
                            set_of_pairs.add((a_new,b_new))

        return set_of_pairs


    def pair_with_activity_names(self, set_pair):
        """
        The input parameter a_b_pair is an (A,B) pair with A and B consiting of activity ids.
        The function returns the same pair but uses activity names instead of ids.
        """
        a, b = set_pair
        a_with_activity_names = set(self.server_activity_mapping[str(x)] for x in a)
        b_with_activity_names = set(self.server_activity_mapping[str(x)] for x in b)
        return (a_with_activity_names, b_with_activity_names)


    def form_petri_net(self, set_pairs:tuple[set[int],set[int]], start_activities:set[int], end_activities:set[int]) -> tuple[PetriNet,Marking,Marking]:
        """
        Takes the (A,B)-pair set, the start activity set and the end activity set and
        returns a Petri Net.
        """
        net = PetriNet("distributed_decentralized_alpha_miner_result")

        ## Places
        # 1 place for each pair in the minimized (A,B)-pair set, 1 place for source, 1 for sink
        source = PetriNet.Place("start")
        sink = PetriNet.Place("end")
        places = [PetriNet.Place(str(self.pair_with_activity_names(x))) for x in set_pairs]
        # Add places to the petri net
        net.places.add(source)
        net.places.add(sink)
        for place in places:
            net.places.add(place)

        ## Transitions (for each activity one transition)
        transitions = [PetriNet.Transition(str(self.server_activity_mapping[str(activity)]), str(self.server_activity_mapping[str(activity)])) for activity in self.activities]
        # Add them to the petri net
        for tran in transitions:
            net.transitions.add(tran)

        ## Arcs
        # Add arcs to the petri net: place -> transition and transition -> place
        for place in places:
            # incoming arcs
            # 1 arc for each element of the A-set
            a_b_pair = eval(place.name)
            a_set = a_b_pair[0]
            for a in a_set:
                activity_corr_to_a = find_transition_to_activity_id(transitions, a)
                petri_utils.add_arc_from_to(activity_corr_to_a, place, net)

            # outgoing arcs
            # 1 arc for each element of the B-set
            b_set = a_b_pair[1]
            for b in b_set:
                activity_corr_to_b = find_transition_to_activity_id(transitions, activity_name=b)
                petri_utils.add_arc_from_to(place, activity_corr_to_b, net)

        # source -> start_activities
        for activity_id in start_activities:
            tran_corr_to_activity = find_transition_to_activity_id(transitions, self.server_activity_mapping[str(activity_id)])
            petri_utils.add_arc_from_to(source, tran_corr_to_activity, net)

        # end_activities -> sink
        for activity_id in end_activities:
            tran_corr_to_activity = find_transition_to_activity_id(transitions, self.server_activity_mapping[str(activity_id)])
            petri_utils.add_arc_from_to(tran_corr_to_activity, sink, net)

        ## Tokens
        # create initial nd final markings
        initial_marking = Marking()
        initial_marking[source] = 1
        final_marking = Marking()
        final_marking[sink] = 1

        ## Output / Visualization
        # pm4py.write_pnml(net, initial_marking, final_marking, "distributed_alpha_miner_result.pnml")
        # pm4py.view_petri_net(net, initial_marking, final_marking) # do not use when running in a docker container
        # print(f"\n\nDistributed Decentralized Alpha Miner\nNet\n{net}")
        return net, initial_marking, final_marking


# Sleep a bit to allow logging to be attached
time.sleep(2)

server_list = os.getenv('SERVER_NAME_LIST').split(',') # for direct connection
server_ip_list = os.getenv('SERVER_IP_LIST').split(',')
own_id = int(os.getenv('SERVER_ID'))
own_ip = server_list[own_id]
server_activity_mapping = literal_eval(os.getenv('SERVER_ACTIVITY_MAPPING'))

server = CentralNode(own_id, own_ip, server_list, server_ip_list, server_activity_mapping)

print(f"#### Starting Central Node {own_ip}")
httpserver.serve(server, host='0.0.0.0', port=80, threadpool_workers=NUM_THREADS, threadpool_options={"spawn_if_under": NUM_THREADS})

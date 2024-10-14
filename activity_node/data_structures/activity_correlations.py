# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import pickle
import numpy as np

class ActivityCorrelations():
    """
    Includes all functions and data structures concerning the storage and updating of the
    direct successions between the activities as well as whether an activity is a start activity or not.
    The data is independent from the specific cases.
    """

    def __init__(self, activity_node) -> None:
        self.activity_node = activity_node
        self.footprint_matrix = None
        self.seq_nmbr_vector = None     # not necessary for the synchronous communication without the optimization
        self.is_start_vector = None


    def set_variables(self,size:tuple[int,int]) -> None:
        """
        Initializes the FM, the is_start vector and their sequence number vector.
        """
        self.footprint_matrix = np.zeros(size, dtype='int')
        self.seq_nmbr_vector = np.zeros((size[0],1), dtype='int')
        self.is_start_vector = np.zeros((size[0],1), dtype='int')


    def get_sendable_footprint_matrix(self) -> str:
        """
        Returns a string version footprint_matrix.
        """
        return pickle.dumps(self.footprint_matrix).decode('latin-1')


    def get_sendable_seq_nmbr_vector(self) -> str:
        """
        Returns a string version of seq_nmbr_vector.
        """
        return pickle.dumps(self.seq_nmbr_vector).decode('latin-1')


    def get_sendable_is_start_vector(self) -> str:
        """
        Returns a string version is_start_vector.
        """
        return pickle.dumps(self.is_start_vector).decode('latin-1')


    def add_direct_succession(self, succ) -> None:
        """
        Adding a direct succession to the footprint matrix.
        A node can only add a direct succession if it is the predecessor of it.
        It adds 1 to the count of the corresponding cell and also increases the sequence number by 1.
        """
        self.footprint_matrix[self.activity_node.id][succ] += 1
        self.seq_nmbr_vector[self.activity_node.id] += 1


    def update_own_start_activity(self, is_start:bool) -> None:
        """
        Sets the start activity bool of own activity ID to value of ``is_start``.
        Increments the sequence number corresponding to own activity ID.
        """
        if is_start != self.is_start_vector[self.activity_node.id]:
            self.is_start_vector[self.activity_node.id] = is_start
            self.seq_nmbr_vector[self.activity_node.id] += 1

# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

class NeighborhoodCollection:
    """
    Manages a datastructure to keep up with all neighbors (predecessors and successors)
    per event structured by case.
    """

    def __init__(self):
        self.all = {} # key: case_id,  value: list of Neighborhood instances


    def add_neighborhood(self, case_id, event_timestamp, pred=None, pred_timestamp=None) -> None:
        """
        Adding a new neighborhood to ``all`` if it does not exist for the ``case_id`` yet.
        Possibly already adding a predecessor and its timestamp.
        """

        # If case id already exists, add to according list
        if str(case_id) in self.all:
            self.all[str(case_id)].append(self.Neighborhood(event_timestamp,pred,pred_timestamp,None,None))

        # Otherwise add new case to dict
        else:
            self.all[str(case_id)] = [self.Neighborhood(event_timestamp,pred,pred_timestamp,None,None)]


    def add_succ_to_neighborhood(self, case_id, event_timestamp, succ, succ_timestamp) -> bool:
        """
        Adding a successor and its timestamp to a specific event with given ``event_timestamp`` of a given ``case_id``.
        """

        # Find neighborhood entry of given event_timestamp and given case_id
        for i, neighborhood in enumerate(self.all[str(case_id)]):

            if neighborhood.event_timestamp == event_timestamp:

                # Check whether succ_timestamp is really better before updating
                if not neighborhood.succ:

                    self.all[str(case_id)][i].succ = succ
                    self.all[str(case_id)][i].succ_timestamp = succ_timestamp

                    return True

        print("Something went wrong. The neighborhood already had a set successor or the neighborhood could not be found.")
        return False


    def add_pred_to_neighborhood(self, case_id, event_timestamp, pred, pred_timestamp) -> bool:
        """
        Adding a predecessor and its timestamp to a specific event with given ``event_timestamp``of a given ``case_id``.
        """
        # Find neighborhood entry of given event_timestamp and given case_id
        for i, neighborhood in enumerate(self.all[str(case_id)]):

            if neighborhood.event_timestamp == event_timestamp:

                # Check whether there is no predecessor set yet
                if not neighborhood.pred:

                    self.all[str(case_id)][i].pred = pred
                    self.all[str(case_id)][i].pred_timestamp = pred_timestamp

                    return True

        print("Something went wrong. The neighborhood already had a set predecessor or the neighborhood could not be found.")
        return False


    class Neighborhood():
        """
        A neighborhood is specific for one event - so for one ``event_timestamp`.
        Each neighborhood includes a predecessor with its timestamp and a successor with its timestamp.
        """
        def __init__(self, event_timestamp, pred, pred_timestamp, succ, succ_timestamp) -> None:
            self.event_timestamp = event_timestamp
            self.pred = pred
            self.pred_timestamp = pred_timestamp
            self.succ = succ
            self.succ_timestamp = succ_timestamp




# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

class StartActivities:
    """
    Manages all data structures as well as functions concerning start activities.
    Class depends on case IDs.
    """

    def __init__(self, activity_node) -> None:
        self.activity_node = activity_node
        self.start_activities_by_case = {}      # elements: str(case_id): activity id


    def add_own_start_activity(self, case_id:str) -> None:
        """
        For ``case_id`` adding its own activity id to ``start_activities_by_case``.
        """
        size_before = len(self.start_activities_by_case)
        if str(case_id) in self.start_activities_by_case:
            print("Caution! Start activity was already set.")

        self.start_activities_by_case[case_id] = self.activity_node.id

        if size_before == 0:
            self.activity_node.activity_correlations.update_own_start_activity(True)

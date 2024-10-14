# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""
A collection of auxiliary functions.
"""
from itertools import chain, combinations, product

def print_with_name_instead_of_id(dict_activity_id_to_name, causalities, parallels):
    """
    Printing the causalities and parallelisms in a more human readable way.
    """
    print('\n')
    for (a,b) in causalities:
        print(f"{dict_activity_id_to_name[str(a)]} -> {dict_activity_id_to_name[str(b)]}")
    for (a,b) in parallels:
        print(f"{dict_activity_id_to_name[str(a)]} || {dict_activity_id_to_name[str(b)]}")


def find_transition_to_activity_id(transitions, activity_name):
    """
    When creating the petri net, all activities correspond to a transition.
    The transition object is looked up by checking which name is the same as the activity's.
    The transition object is returned.
    """
    for tran in transitions:
        if tran.name == activity_name:
            return tran


def get_all_subsets(activities):
    """
    Returning all possible non-empty subsets of the activities.
    get_all_subsets([1,2,3]) --> (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)
    """
    activities_list = list(activities)
    return list(chain.from_iterable(combinations(activities_list, el) for el in range(len(activities_list)+1)))[1:]


def is_independent_set(activity_list, choices, self_loops):
    """
    Takes a list (of activities) ``activity_list`` and a list which includes all choices,
    meaning there is no correlation between the activity-pairs in ``choices``.
    If all combinations of elements in ``activity_list`` are in ``choices``, True is returned.
    Otherwise False is returned.
    """
    if len(activity_list) == 1:
        if activity_list[0] not in self_loops:
            return True
        return False

    pairs = combinations(activity_list, 2)
    for pair in pairs:
        if pair not in choices:
            return False
    return True


def is_causality_pair(activities_1, activities_2, causalities):
    """
    Two lists of activities as well as a list of all causality relationsare given.
    If all pairs of the form ``(a,b)``, where ``a`` is in activities_1 and ``b`` is in
    ``activities_2``, are in the causality list, True is returned - otherwise False.
    """
    all_combs = product(activities_1, activities_2)
    for pair in all_combs:
        if pair not in causalities:
            return False
    return True


def is_subset(a, b):
    """
    The input parameters are pairs (a_0,a_1) and (b_0,b_1).
    The function returns True if a_0 is a subset of b_0 as well
    as a_1 of b_1.
    """
    if set(a[0]).issubset(b[0]) and set(a[1]).issubset(b[1]):
        return True
    return False

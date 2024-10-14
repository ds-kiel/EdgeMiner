# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from pm4py.objects.petri_net.obj import PetriNet

def equality_check(petri_net_1, petri_net_2):
    """
    Two petri nets are equal if they have the same transitions, the same places and the same arcs.
    We can assume the chosen names and labels to be the same. Merely the order in sets can vary.
    """
    net_1, initial_marking_1, final_marking_1 = petri_net_1
    net_2, initial_marking_2, final_marking_2 = petri_net_2

    ### compare places, arcs and transitions, start and end

    # check whether initial and final markings are the same
    if str(initial_marking_1) != str(initial_marking_2):
        print(f"initial markings do not match:  {initial_marking_1}  vs  {initial_marking_2}")
        return False
    if str(final_marking_1) != str(final_marking_2):
        print(f"final markings do not match:  {final_marking_1}  vs  {final_marking_2}")
        return False

    # check whether the places are the same
    res = __check_places(net_1.places,net_2.places)
    if not res:
        return res

    # check whether the transitions are the same
    res = __check_transitions(net_1.transitions, net_2.transitions)
    if not res:
        return res

    # check whether the arcs are the same
    # for each arc check whether source and target match
    res = __check_arcs(net_1.arcs, net_2.arcs)
    if not res:
        return res

    return True


def __check_transitions(transitions_1, transitions_2):
    """
    Checks whether the transitions of the two petri nets are the same.
    """
    transitions_1 = set(tran.label for tran in transitions_1)
    transitions_2 = set(tran.label for tran in transitions_2)
    if transitions_1 != transitions_2:
        print(f"The transitions of the petri nets do not match...\ntransitions 1: {transitions_1}\ntransitions 2: {transitions_2}")
        return False
    return True


def __check_places(places_1, places_2):
    """
    Checks whether the places of the two petri nets are the same.
    """
    if len(places_1) != len(places_2):
        print(f"The number of places in the petri nets does not match.\nplaces 1  {places_1}\nplaces 2  {places_2}")
        return False
    places_set_1 = set(place.name for place in places_1)
    places_set_2 = set(place.name for place in places_2)

    copy_1 = places_set_1.copy()
    start_found, end_found = False, False
    for place in copy_1:
        if place == "start":
            places_set_1.discard(place)
            start_found = True
        elif place == "end":
            places_set_1.discard(place)
            end_found = True
    if not start_found or not end_found:
        print("start or end is missing in the places of the first petri net.")
        return False

    copy_2 = places_set_2.copy()
    start_found, end_found = False, False
    for place in copy_2:
        if place == "start":
            places_set_2.discard(place)
            start_found = True
        elif place == "end":
            places_set_2.discard(place)
            end_found = True
    if not start_found or not end_found:
        print("start or end is missing in the places of the second petri net.")
        return False

    places_set_1 = list(eval(place) for place in list(places_set_1)) # should be set of sets
    places_set_2 = list(eval(place) for place in list(places_set_2)) # same

    for (a1,b1) in places_set_1:
        exists = False
        for (a2,b2) in places_set_2:
            if a1 == a2 and b1 == b2:
                exists = True
        if not exists:
            print("There is a place in the first petri net that does not exist in the second one.")
            return False
    return True


def __check_arcs(arcs_1, arcs_2):
    """
    Checks whether the arcs of the two petri nets are the same.
    """
    if len(arcs_1) != len(arcs_2):
        print("The number of arcs in the two petri nets does not match.")
        return False

    for arc_1 in arcs_1:

        source_1 = __transition_or_place_from_object(arc_1.source)
        target_1 = __transition_or_place_from_object(arc_1.target)

        exists = False
        for arc_2 in arcs_2:
            source_2 = __transition_or_place_from_object(arc_2.source)
            target_2 = __transition_or_place_from_object(arc_2.target)

            if source_1 == source_2 and target_1 == target_2:
                exists=True
                break
        if not exists:
            print("There exists an arc in the first petri net which does not exist in the second one.")
            return False
    return True

def __transition_or_place_from_object(x):
    """
    Gets a transition or a place and returns its label if its a transition or
    otherwise a (A,B)-set representing the place (unless its a start or end place).
    """
    if isinstance(x, PetriNet.Transition):
        # x is transition
        return x.label

    # x is place
    x = x.name
    if x in ["start","end"]:
        return x
    else:
        return eval(x)

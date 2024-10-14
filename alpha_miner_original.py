# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import pm4py


def run_original_alpha_miner(log):
    """
    Runs the original alpha miner (pm4py implementation) on the given log.
    Outputs the output of the alpha miner, meaning the net, initial marking
    and final marking.
    Optionally writes the output into a file and opens a visualization of the resulting
    Petri Net.
    """
    log['case:concept:name'] = log["case:concept:name"].apply(lambda x: str(x))
    net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(log)

    # pm4py.write_pnml(net, initial_marking, final_marking, "alpha_miner_result.pnml")
    # pm4py.view_petri_net(net, initial_marking, final_marking)
    print(f"Traditional Alpha Miner\n{net}\n\n")

    return net, initial_marking, final_marking

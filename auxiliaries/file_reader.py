# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import pandas as pd

from auxiliaries.csv_reader import read_traces_from_csv
from auxiliaries.xes_reader import read_traces_from_xes


def read_event_log(file_path):
    """
    Reading the given event log in ``csv`` or ``xes`` format
    and returning a pandas dataframe.
    """

    log = ""

    if file_path[-3:] == "xes":
        # EITHER: read an xes-file
        log = read_traces_from_xes(file_path)

    elif file_path[-3:] == "csv":
        # OR: read a csv-file
        log = read_traces_from_csv(file_path)
        log['time:timestamp'] = pd.to_datetime(log['time:timestamp'])
        log['concept:name'] = log["concept:name"].apply(str)
        log['case:concept:name'] = log["case:concept:name"].apply(str)

        # Sort traces by timestamp and case id
        log = log.sort_values(["time:timestamp", "case:concept:name"])

    else:
        print("The given file format is not supported.")
        return -1

    return log

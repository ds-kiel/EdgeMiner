# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from auxiliaries.file_reader import read_event_log
from datetime import timedelta


def no_doubled_timestamps(event_log_df):
    """
    Assumption: Dataframe is ordered by case Id and timestamp
    """
    current_case_id = -1
    last_timestamp = None

    for i, row in event_log_df.iterrows():

        case_id = row["case:concept:name"]
        timestamp = row["time:timestamp"]

        if last_timestamp and case_id == current_case_id and timestamp <= last_timestamp:
            # Add a second to timestamp to make it slightly later than the one before
            event_log_df.loc[i, "time:timestamp"] = last_timestamp + timedelta(seconds=1)

        current_case_id = case_id
        last_timestamp = event_log_df.loc[i, "time:timestamp"]

    return event_log_df


def read_and_sort(file_path):

    event_log_df = read_event_log(file_path)
    event_log_df = event_log_df.sort_values(["case:concept:name","time:timestamp"])

    return event_log_df

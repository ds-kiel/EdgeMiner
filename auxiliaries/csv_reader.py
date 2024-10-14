# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""
CsvReader includes all functions regarding reading csv files or converting them.
"""
import pandas as pd
import pm4py


# csv-reader
def read_traces_from_csv(file_path):
    """
    Reading the event log from a csv file and returning it as pandas dataframe.
    """
    dataframe = pd.read_csv(file_path, sep=',')
    return dataframe


def csv_to_xes(file_path):
    """
    Reading the event log from a csv file and converting the resuting pandas
    dataframe into an xes-file.
    """
    dataframe = read_traces_from_csv(file_path)
    # convert timestamp column to datetime
    dataframe['time:timestamp'] = pd.to_datetime(dataframe['time:timestamp'])
    dataframe['case:concept:name'] = dataframe["case:concept:name"].apply(str)
    dataframe = dataframe.loc[:, ~dataframe.columns.str.contains('^Unnamed')]

    pm4py.write_xes(dataframe, f"{file_path[:-4]}_converted.xes")
    print(f"{file_path[:-4]}_converted.xes   has been created.")

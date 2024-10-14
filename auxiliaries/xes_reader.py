# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""
XesReader includes all functions regarding reading or converting xes files.
"""
import pm4py


# xes-reader
def read_traces_from_xes(file_path):
    """
    Reading the event log from a xes file and returning it as pandas dataframe.
    """
    log = pm4py.read_xes(file_path)
    return log


def xes_to_csv(self, file_path):
    """
    Takes a file path to an xes file, reads it and converts it to a csv file.
    """
    dataframe = self.read_traces_from_xes(file_path)
    dataframe.to_csv(f"{file_path[:-4]}_converted.csv")
    print(f"{file_path[:-4]}_converted.csv   has been created.")

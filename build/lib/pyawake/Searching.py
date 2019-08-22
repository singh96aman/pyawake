'''
    Title         : Searching Module for pyAwake
    Author        : Aman Singh Thakur
    Last Edited   : 08 August, 2019
'''

import os, sys
import json
import ast
import time
import threading
from datetime import datetime
import pytz
import h5py
import csv
import sys
import multiprocessing
from multiprocessing import Process, Pool
from tempfile import NamedTemporaryFile
import shutil
import pandas as pd
import numpy as np
import csv
import math
from scipy.signal import medfilt
import matplotlib.pyplot as plt
from functools import partial
import matplotlib.image as mpimg
import glob
import time
import numpy as np
import logging

#Create and configure logger 
logging.basicConfig(filename="newfile.log", 
                    format='%(asctime)s %(message)s', 
                    filemode='w') 
  
#Creating an object 
logger=logging.getLogger() 
  
#Setting the threshold of logger to DEBUG 
logger.setLevel(logging.DEBUG) 

import analyses.frame_analysis as fa
from utilities.custom_cmap import custom_cmap
cm = custom_cmap()

filename_split = "_"
CERN_timezone = "Europe/Zurich"
no_of_files_indexed = 0
no_of_files_searched = 0
count = 0
headers = []
cache_size = 3
central_csv_name = "CentralCSV.csv"
dataset_txt_name = "dataset_hash.csv"
comment_csv_name = "comment_hash.csv"
fieldnames = ['DatasetName', 'GroupName', 'AcqStamp',  'Exception', 'Shape', 'Datatype']
dir_h5_filepath = "/eos/experiment/awake/event_data/"
dir_home = "/eos/experiment/awake/event_data/"
dir_csv_filepath = "/eos/experiment/awake/CSVFiles/"
dir_csv_central_filepath = "/eos/experiment/awake/CSVFiles/Central/"
central_csv_name = "CentralCSV.csv"
image_filepath = "/eos/user/a/amthakur/PNGFiles/"

class Searching:
      
    # Open a csv file using pandas
    def open_file(self, file_path):
        file = open(file_path, "r")
        df = pd.read_csv(file)
        return df

    # Get all files in a directory with given filetype
    def get_all_files(self, dir_filepath, file_type):
        all_files_list = os.listdir(dir_filepath)
        all_files = []
        for file in all_files_list:
            file_fields = file.split(".")
            if len(file_fields) > 1:
                if str(file_fields[1]) == str(file_type):
                    all_files.append(file)
        return all_files

    # Convert UNIX Timestamp to CERN and UTC Date Time Objects
    def convert_unix_time_to_datetype(self, filename):
        filename_array = filename.split(filename_split)
        if(str(filename_array[0]) == "optimized"):
            val = 1
        else:
            val = 0
        if len(str(filename_array[val])) == 19:
            timestamp = float(filename_array[val])
            utc_dt = datetime.utcfromtimestamp(timestamp // 1e9)
            cern_tz = pytz.timezone(CERN_timezone)
            cern_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(cern_tz)
            cern_dt = cern_tz.normalize(cern_dt)
            return (utc_dt, cern_dt, filename_array[1], filename_array[2])
      
    # USED TO CONVERT TO UTF-8
    def decode_attr(self, file):
        try:
            return file.decode('utf-8')
        except:
            return str(file)

    # GETTING DATASET ATTRIBUTES
    def get_dataset_attr(self, file):
        DatasetAttributes = {}
        for key in file.keys():
            val = self.decode_attr(file[key])
            DatasetAttributes[key] = val
        if(len(file.keys()) > 0):
            return str(DatasetAttributes)
        else:
            return ""

#     # DIVIDING DATASETS FOR THREADS
#     def divide_datasets_for_threads(self, dir_h5_filepath, no_of_threads, filetype):
#         all_HDF_files = self.get_all_files(dir_h5_filepath, filetype)
#         max_size = len(list(all_HDF_files))
#         HDF_thread_arr = []
#         k, m = divmod(max_size, no_of_threads)
#         return list(all_HDF_files[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(0, no_of_threads)), max_size

#     # DIVIDING DATASETS AS PER THREADS AND TIMESTAMP
#     def divide_datasets_for_threads_timestamp(self, all_HDF_files, no_of_threads):
#         max_size = len(list(all_HDF_files))
#         HDF_thread_arr = []
#         k, m = divmod(max_size, no_of_threads)
#         return list(all_HDF_files[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(0, no_of_threads)), max_size

    # GETTING VALUE AS PER DATATYPE
    def get_dataset_value(self, size, file):
        if(int(size) == 0 or int(size) > 1):
            return ""
        else:
            if(str(file.dtype) == "|S1"):
                return ord(file[0])
            elif "S" in str(file.dtype):
                return str(file[0].decode('utf-8'))
            else:
                return str(file[0])

    # Subroutine to get H5 file name from csv file name
    def convert_csv_to_h5(self, main_file_name):
        filename = main_file_name.split(".")
        return filename[0]+".h5"

    # Converting input time date to datetime object
    def convert_str_to_timestamp(self, timestamp):
        timestamp = datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S')
        return timestamp
    
    # Get multiple dataset value with timestamp and h5_filename from a csv
    def get_column_data(self,  h5_filename, csv_file_name, Timestamp, dataset_name, dataset_cuts):
        df = self.open_file(csv_file_name)
        cuts_arr = self.generate_stmts_cuts(dataset_cuts)
        cut_satisfied = self.apply_cuts("DatasetName", cuts_arr, df)
        column_data_retrieved = []
        if cut_satisfied:
            for dataset in dataset_name :
                df_temp = df[df['DatasetName'].str.contains(dataset)==True]
                if not df_temp.empty:
                    column_data_retrieved.append(df_temp['DatasetValue'].values[0])
        if column_data_retrieved == []:
            return None
        else:
            column_data_retrieved.insert(0, Timestamp)
            column_data_retrieved.append(h5_filename)
            return column_data_retrieved
     
    # Get dataset metadata dict values from csv 
    def get_metadata(self, h5_filename, csv_file_name, Timestamp, dataset_name, dataset_cuts):
        dataset = {}
        df = self.open_file(csv_file_name)
        cuts_arr = self.generate_stmts_cuts(dataset_cuts)
        cut_satisfied = self.apply_cuts("DatasetName", cuts_arr, df)
        if cut_satisfied:
            df_temp = df[df['DatasetName'].str.contains(dataset_name)==True]
            if not df_temp.empty:
                df_temp = df[df['DatasetName'].str.contains(dataset_name)==True]
                parentGroup = df_temp['GroupName'].values[0]
                df = df[df['GroupName'].str.contains(str(parentGroup)) == True]
                dataset['HDFFilename'] = h5_filename
                dataset['GroupName'] = parentGroup
                dataset['DatasetName'] = dataset_name
                dataset['Timestamp'] = Timestamp
                for row in df.iterrows():
                    if(str(row[1]['Shape']) == "nan" or str(row[1]['DatasetValue']) == "nan"):
                        dataset[row[1]['DatasetName']] = "nan"
                    else:
                        dataset[row[1]['DatasetName']] = row[1]['DatasetValue']
        return dataset
        
    # Get dataset dict values from csv 
    def get_dataset(self, h5_filename, csv_file_name, Timestamp, dataset_name, dataset_cuts):
        dataset = {}
        try:
            h5File = h5py.File(h5_filename, 'r')
            df = self.open_file(csv_file_name)
            cuts_arr = self.generate_stmts_cuts(dataset_cuts)
            cut_satisfied = self.apply_cuts("DatasetName", cuts_arr, df)
            if cut_satisfied:
                df_temp = df[df['DatasetName'].str.contains(dataset_name)==True]
                if not df_temp.empty:
                    df_temp = df[df['DatasetName'].str.contains(dataset_name)==True]
                    parentGroup = df_temp['GroupName'].values[0]
                    df = df[df['GroupName'].str.contains(str(parentGroup)) == True]
                    dataset['HDFFilename'] = h5_filename
                    dataset['GroupName'] = parentGroup
                    dataset['DatasetName'] = dataset_name
                    dataset['Timestamp'] = Timestamp
                    for row in df.iterrows():
                        if(str(row[1]['Shape']) == "nan"):
                            h5File = h5File[str(row[1]['GroupName'])]
                            dataset[row[1]['DatasetName']] = list(h5File[row[1]['DatasetName']])  
                        else:
                            if(str(row[1]['DatasetValue']) == "nan"):
                                h5File = h5File[str(row[1]['GroupName'])]
                                dataset[row[1]['DatasetName']] = list(
                                    h5File[row[1]['DatasetName']])
                            else:
                                dataset[row[1]['DatasetName']] = row[1]['DatasetValue']
        except OSError as e: 
            logger.warning(h5_filename+" file not found")
        return dataset


#     # SUBROUTINES TO GET ALL DATASET FILES USING MULTI-THREADING (UTIL) (FASTER)
#     def get_all_dataset_files_thread(self, dir_csv_filepath, process_csv_file, dataset_name, fieldname, no_of_threads, max_size):
#         global files_with_dataset
#         for file in process_csv_file:
#             file_fields = file.split(".")
#             utc_dt, cern_dt, fileptr1, fileptr2 = self.convert_unix_time_to_datetype(file_fields[0])
#             temp_dir_csv_filepath = dir_csv_filepath + str(cern_dt.year) + "/"
#             if(int(cern_dt.month)>=10):
#                 temp_dir_csv_filepath+=str(cern_dt.month) + "/"
#             else:
#                 temp_dir_csv_filepath+="0"+str(cern_dt.month)+"/"
#             if(int(cern_dt.day)>=10):
#                 temp_dir_csv_filepath+=str(cern_dt.day)+"/"
#             else:
#                 temp_dir_csv_filepath+="0"+str(cern_dt.day)+"/"
#             df = self.open_file(temp_dir_csv_filepath+file)
#             df = df[df[fieldname].str.contains(str(dataset_name)) == True]
#             files_with_dataset.append([file, df])
#             global no_of_files_searched
#             no_of_files_searched = no_of_files_searched+1

    # Get all dates between 2 timestamps
    def get_all_dates(self, start, end):
        all_dates = []
        start_date = str(start.year)+"-"+str(start.month)+"-"+str(start.day)
        end_date = str(end.year)+"-"+str(end.month)+"-"+str(end.day)
        timestamp_list = pd.date_range(start_date, end_date, 
                  freq='1D').tolist()
        for date in timestamp_list:
            if(int(date.month)>=10):
                month=str(date.month) + "/"
            else:
                month="0"+str(date.month)+"/"
            if(int(date.day)>=10):
                day=str(date.day)+"/"
            else:
                day="0"+str(date.day)+"/"
            all_dates.append([str(date.year)+"/", str(month), str(day)])
        return all_dates

    # Get all CSV files between 2 timestamps in different directories
    def get_all_files_tree(self, dir_csv_filepath, start_date, end_date, file_type):
        date_arr = self.get_all_dates(start_date, end_date)
        CSV_thread_arr = []
        for date in date_arr:
            date_filepath = dir_csv_filepath + str(date[0]) + "/" + str(date[1]) + "/" + str(date[2])
            try:
                all_files_list = os.listdir(date_filepath)
                for file in all_files_list:
                    file_fields = file.split(".")
                    if len(file_fields) > 1:
                        if str(file_fields[1]) == str(file_type):
                            utc_dt, cern_dt, fileptr1, fileptr2 = self.convert_unix_time_to_datetype(file_fields[0])
                            if(cern_dt.timestamp()>=start_date.timestamp() and cern_dt.timestamp()<=end_date.timestamp()):
                                CSV_thread_arr.append([date,file])
            except:
                continue
        return CSV_thread_arr

    # Get array of statement cuts from csv string
    def generate_stmts_cuts(self, dataset_cuts):
        generated_stmts = []
        if(dataset_cuts.find(",")!=-1):
            try:
                dataset_arr = dataset_cuts.split(",")
                for dataset in dataset_arr:
                    dataset_name, operator, value_cut = dataset.split(" ")
                    generated_stmts.append([dataset_name, operator, value_cut])
                return generated_stmts
            except :
                return []
        else:
            return []
    
    # Apply cuts on the dataframe loaded from csv
    def apply_cuts(self, fieldname, cuts_arr, df):
        for dataset in cuts_arr:
            df = df[df[fieldname].str.contains(str(dataset[0])) == True]
            if not df.empty:
                if dataset[1] == "<":
                    if(float(df["DatasetValue"].values[0]) > float(dataset[2])):
                        return False
                elif dataset[1] == ">":
                     if(float(df["DatasetValue"].values[0]) < float(dataset[2])):
                        return False
                elif dataset[1] == "==":
                     if(float(df["DatasetValue"].values[0]) != float(dataset[2])):
                        return False
        return True
    
    # Get all dataset dependent files by timestamp and checking if it follows cut and it's in timestamp range
    def get_all_dataset_files_by_timestamp(self, dir_csv_filepath, dataset_name, fieldname, starting_timestamp, ending_timestamp):
        starting_timestamp = self.convert_str_to_timestamp(starting_timestamp)
        ending_timestamp = self.convert_str_to_timestamp(ending_timestamp)
        selected_timestamp_arr = self.get_all_files_tree(dir_csv_filepath, starting_timestamp, ending_timestamp, "csv")
        files_with_dataset = []
        print("Checking if "+str(len(selected_timestamp_arr))+" files have dataset value and are in timestamp range.")
        for file in selected_timestamp_arr:
            file_fields = file[1].split(".")
            utc_dt, cern_dt, fileptr1, fileptr2 = self.convert_unix_time_to_datetype(file_fields[0])
            if cern_dt.timestamp() >= starting_timestamp.timestamp() and cern_dt.timestamp() <= ending_timestamp.timestamp():
                temp_dir_csv_filepath = dir_csv_filepath+file[0][0]+file[0][1]+file[0][2]
                files_with_dataset.append([cern_dt, file])
        if(files_with_dataset == []):
            print("No datasets in the timestamp range fulfilled the cut condition")
        return files_with_dataset

#     # Exhaustive search for all files with dataset name to get the dataset
#     def search_by_dataset_name(self, dir_h5_filepath, dir_csv_filepath, dataset_name, central_csv_filepath, no_of_threads):
#         final_solution = self.get_all_dataset_files(
#             dir_csv_filepath, dataset_name, 'DatasetName', no_of_threads)
#         if(len(final_solution) > 0):
#             selected_timestamp_arr = []
#             count = 0
#             for row in final_solution:
#                 utc_dt, cern_dt, fileptr1, fileptr2 = self.convert_unix_time_to_datetype(
#                     row[0])
#                 selected_timestamp_arr.append([cern_dt, final_solution[count-1]])
#                 print(str(count)+". "+str(cern_dt))
#                 count = count + 1
#             count = count-1
#             if(len(selected_timestamp_arr) > 0):
#                 val_selected = int(
#                     input("Enter Which file to load (0,"+str(count)+")"))
#                 filename = self.convert_csv_to_h5(
#                     selected_timestamp_arr[val_selected][1][0].split("."))
#                 selected_timestamp_arr[val_selected][1][0] = dir_csv_filepath + \
#                     str(selected_timestamp_arr[val_selected][1][0])
#                 return self.get_dataset(dir_h5_filepath+filename, selected_timestamp_arr[val_selected], dataset_name)
#         else:
#             print("Dataset not found")
#             return {}

    # Converting attributes from str to dict
    def get_dict_from_str(self, attr):
        try:
            return eval(attr)
        except:
            return {}
        
    def load_column_timestamp(self, dir_h5_filepath, dir_csv_filepath, dataset_name, starting_timestamp, ending_timestamp, dataset_cuts):
        final_solution = self.get_all_dataset_files_by_timestamp(
            dir_csv_filepath, dataset_name, 'DatasetName', starting_timestamp, ending_timestamp)
        if(len(final_solution) > 0):
            count = 0
            for row in final_solution:
                print(str(count)+". "+str(row[0])+" "+str(row[1][1]))
                count = count + 1
            count = count-1
            val_selected = input("Enter Which file to load (0,"+str(count)+")")
            if((val_selected.find(",")!=-1) or (val_selected.find("-"))!=-1):
                list_selected_files = []
                if(val_selected.find(",")!=-1):
                    values_selected = val_selected.split(",")
                    for value in values_selected :
                        if(value.find("-")!=-1):
                            range_vals = value.split("-")
                            start = int(range_vals[0].strip())
                            end = int(range_vals[1].strip())
                            for i in range(start, end+1):
                                list_selected_files.append(i)
                        else:
                            list_selected_files.append(int(value.strip()))
                elif(val_selected.find("-")!=-1):
                    values_selected = val_selected.split("-")
                    start = int(values_selected[0].strip())
                    end = int(values_selected[1].strip())
                    for i in range(start, end+1):
                        list_selected_files.append(i)
                start_time = time.time()
                dataset_arr = []
                for val in list_selected_files:
                    print(str(val)+". "+final_solution[val][1][1]+" is being loaded.")
                    val_selected = int(val)
                    filename = self.convert_csv_to_h5(final_solution[val_selected][1][1])
                    final_solution[val_selected][1][1] = dir_csv_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+final_solution[val_selected][1][1]
                    h5_filepath = dir_h5_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+filename
                    column_data_retrieved = self.get_column_data(h5_filepath, final_solution[val_selected][1][1], final_solution[val_selected][0], dataset_name, dataset_cuts)
                    if column_data_retrieved!=None:
                        dataset_arr.append(column_data_retrieved)
                print("--- %s Time taken (s)  ---" % (time.time() - start_time))
                return dataset_arr
            else:
                val_selected = int(val_selected)
                filename = self.convert_csv_to_h5(final_solution[val_selected][1][1].split("."))
                final_solution[val_selected][1][1] = dir_csv_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+final_solution[val_selected][1][1]
                h5_filepath = dir_h5_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+filename
                return self.get_metadata(h5_filepath, final_solution[val_selected][1][1], final_solution[val_selected][0], dataset_name, dataset_cuts)
        else:
            print("Dataset not found")
            return {}
    # Driver function to load metadata of all csv between 2 timestamp   
    def load_group_metadata_timestamp(self, dir_h5_filepath, dir_csv_filepath, dataset_name, starting_timestamp, ending_timestamp, dataset_cuts):
        final_solution = self.get_all_dataset_files_by_timestamp(
            dir_csv_filepath, dataset_name, 'DatasetName', starting_timestamp, ending_timestamp)
        if(len(final_solution) > 0):
            count = 0
            for row in final_solution:
                print(str(count)+". "+str(row[0])+" "+str(row[1][1]))
                count = count + 1
            count = count-1
            val_selected = input("Enter Which file to load (0,"+str(count)+")")
            if((val_selected.find(",")!=-1) or (val_selected.find("-"))!=-1):
                dataset_arr = []
                list_selected_files = []
                if(val_selected.find(",")!=-1):
                    values_selected = val_selected.split(",")
                    for value in values_selected :
                        if(value.find("-")!=-1):
                            range_vals = value.split("-")
                            start = int(range_vals[0].strip())
                            end = int(range_vals[1].strip())
                            for i in range(start, end+1):
                                list_selected_files.append(i)
                        else:
                            list_selected_files.append(int(value.strip()))
                elif(val_selected.find("-")!=-1):
                    values_selected = val_selected.split("-")
                    start = int(values_selected[0].strip())
                    end = int(values_selected[1].strip())
                    for i in range(start, end+1):
                        list_selected_files.append(i)
                start_time = time.time()
                for val in list_selected_files:
                    print(str(val)+". "+final_solution[val][1][1]+" is being loaded.")
                    val_selected = int(val)
                    filename = self.convert_csv_to_h5(final_solution[val_selected][1][1].split("."))
                    final_solution[val_selected][1][1] = dir_csv_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+final_solution[val_selected][1][1]
                    h5_filepath = dir_h5_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+filename
                    returned_metadata = self.get_metadata(h5_filepath, final_solution[val_selected][1][1], final_solution[val_selected][0], dataset_name, dataset_cuts)
                    if returned_metadata != {}:
                        dataset_arr.append(returned_metadata)
                print("--- %s Time taken to open csv in pandas (seconds)  ---" % (time.time() - start_time))
                return dataset_arr
            else:
                val_selected = int(val_selected)
                filename = self.convert_csv_to_h5(final_solution[val_selected][1][1].split("."))
                final_solution[val_selected][1][1] = dir_csv_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+final_solution[val_selected][1][1]
                h5_filepath = dir_h5_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+filename
                return self.get_metadata(h5_filepath, final_solution[val_selected][1][1], final_solution[val_selected][0], dataset_name, dataset_cuts)
        else:
            print("Dataset not found")
            return {}
    
    # Searching by dataset between two timestamp
    def search_by_dataset_timestamp(self, dir_h5_filepath, dir_csv_filepath, dataset_name, starting_timestamp, ending_timestamp, dataset_cuts):
        final_solution = self.get_all_dataset_files_by_timestamp(
            dir_csv_filepath, dataset_name, 'DatasetName', starting_timestamp, ending_timestamp)
        if(len(final_solution) > 0):
            count = 0
            for row in final_solution:
                print(str(count)+". "+str(row[0])+" "+str(row[1][1]))
                count = count + 1
            count = count-1
            val_selected = input("Enter Which file to load (0,"+str(count)+")")
            if((val_selected.find(",")!=-1) or (val_selected.find("-"))!=-1):
                dataset_arr = []
                list_selected_files = []
                if(val_selected.find(",")!=-1):
                    values_selected = val_selected.split(",")
                    for value in values_selected :
                        if(value.find("-")!=-1):
                            range_vals = value.split("-")
                            start = int(range_vals[0].strip())
                            end = int(range_vals[1].strip())
                            for i in range(start, end+1):
                                list_selected_files.append(i)
                        else:
                            list_selected_files.append(int(value.strip()))
                elif(val_selected.find("-")!=-1):
                    values_selected = val_selected.split("-")
                    start = int(values_selected[0].strip())
                    end = int(values_selected[1].strip())
                    for i in range(start, end+1):
                        list_selected_files.append(i)
                for val in list_selected_files:
                    print(str(val)+". "+final_solution[val][1][1]+" is being loaded.")
                    val_selected = int(val)
                    filename = self.convert_csv_to_h5(final_solution[val_selected][1][1])
                    final_solution[val_selected][1][1] = dir_csv_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+final_solution[val_selected][1][1]
                    h5_filepath = dir_h5_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+filename
                    returned_dataset = self.get_dataset(h5_filepath, final_solution[val_selected][1][1], final_solution[val_selected][0], dataset_name, dataset_cuts)
                    if returned_dataset != {}:
                        dataset_arr.append(returned_dataset)
                return dataset_arr
            else:
                val_selected = int(val_selected)
                filename = self.convert_csv_to_h5(final_solution[val_selected][1][1].split("."))
                final_solution[val_selected][1][1] = dir_csv_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+final_solution[val_selected][1][1]
                h5_filepath = dir_h5_filepath+final_solution[val_selected][1][0][0]+final_solution[val_selected][1][0][1]+final_solution[val_selected][1][0][2]+filename
                return self.get_dataset(h5_filepath, final_solution[val_selected][1][1], final_solution[val_selected][0], dataset_name, dataset_cuts)
        else:
            print("Dataset not found")
            return {}

    # MAIN ROUTINE TO SEARCH BY DATASET QUERY
#     def search_by_dataset_query(self, dir_h5_filepath, dir_csv_filepath, query, central_csv_filepath, no_of_threads):
#         final_solution, dataset_name = self.get_all_dataset_files_by_query(
#             dir_csv_filepath, dir_csv_filepath, query, central_csv_filepath, no_of_threads)
#         if(len(final_solution) > 0):
#             selected_timestamp_arr = []
#             count = 0
#             for row in final_solution:
#                 utc_dt, cern_dt, fileptr1, fileptr2 = self.convert_unix_time_to_datetype(
#                     row[0])
#                 selected_timestamp_arr.append([cern_dt, final_solution[count-1]])
#                 print(str(count)+". "+str(cern_dt))
#                 count = count + 1
#             count = count-1
#             if(len(selected_timestamp_arr) > 0):
#                 val_selected = int(
#                     input("Enter Which file to load (0,"+str(count)+")"))
#                 filename = self.convert_csv_to_h5(
#                     selected_timestamp_arr[val_selected][1][0].split("."))
#                 selected_timestamp_arr[val_selected][1][0] = dir_csv_filepath + \
#                     str(selected_timestamp_arr[val_selected][1][0])
#                 return self.get_dataset(dir_h5_filepath+filename, selected_timestamp_arr[val_selected], dataset_name)
#         else:
#             print("Dataset not found")
#             return {}

    #Load Central dataset list and the comment dict
    def load_comment_dataset(self, central_csv_filepath):
        dataset_txt = [] 
        comment_csv = {}
        try:
            global dataset_txt_name, comment_csv_name
            with open(central_csv_filepath+dataset_txt_name, "r") as csvfile:
                reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in reader:
                    dataset_txt.append(row)
            with open(central_csv_filepath+comment_csv_name, "r") as csvfile:
                reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in reader:
                    comment_csv[row[0]] = row[1]
        except:
            pass
        return dataset_txt, comment_csv

    # Searching a dataset name to look in dataset list
    def search_dataset_name(self, all_datasets, dataset_name):
        final_solution = set()
        if(dataset_name != ""):
            for dataset in all_datasets:
                dataset_temp = dataset[0].lower()
                if " " in dataset_name:
                    dataset_arr = dataset_name.split(" ")
                    count = 0;
                    for x in dataset_arr:
                        if(x.lower() in dataset_temp):
                            count = count+1
                    if count == len(dataset_arr):
                        final_solution.add(dataset[0])
                else:
                    if(dataset_name.lower() in dataset[0].lower()):
                        final_solution.add(dataset[0])
        return final_solution
    
    #Searching keywords in comment and returns dataset with these comments 
    def search_dataset_comment(self, comment_csv, comment):
        final_solution = set()
        if(comment != ""):
            for key in comment_csv:
                if comment in key.lower():
                    final_solution.add(comment_csv[key])
        return final_solution

    #Loading only columns (csv) in the form of list from dataset
    def load_column(self, dataset_name, comment, starting_timestamp, ending_timestamp, dataset_cuts):
        dataset_txt, comment_csv = self.load_comment_dataset(dir_csv_central_filepath)
        all_selected_datasets = self.search_dataset_name(dataset_txt, dataset_name)
        all_selected_datasets = all_selected_datasets.union(self.search_dataset_comment(comment_csv, comment))
        all_selected_datasets = list(all_selected_datasets)
        if len(all_selected_datasets) == 1:
            dataset_name_selected = all_selected_datasets[0]
            if(dataset_name == ""):
                print(dataset_name_selected)
            return self.load_column_timestamp(dir_h5_filepath, dir_csv_filepath, dataset_name_selected, starting_timestamp, ending_timestamp, dataset_cuts)
        elif len(all_selected_datasets) != 1 and len(all_selected_datasets) != 0:
            count = 0
            for dataset in all_selected_datasets:
                print(str(count)+". "+str(dataset))
                count = count+1
            val_selected = input("Enter which dataset to load (0,"+str(count-1)+")")
            dataset_name_selected = []
            if(val_selected.find(",")!=-1):
                val_arr = val_selected.split(",")
                for val in val_arr:
                    dataset_name_selected.append(all_selected_datasets[int(val.strip())])
            else:
                dataset_name_selected.append(all_selected_datasets[int(val_selected.strip())])
            return self.load_column_timestamp(dir_h5_filepath, dir_csv_filepath, dataset_name_selected, starting_timestamp, ending_timestamp, dataset_cuts)    
        else:
            print("No Dataset Found!")
            return {}
    
    #Master search function to return 
    def search(self, dataset_name, comment, starting_timestamp, ending_timestamp, dataset_cuts):
        dataset_txt, comment_csv = self.load_comment_dataset(dir_csv_central_filepath)
        all_selected_datasets = self.search_dataset_name(dataset_txt, dataset_name)
        all_selected_datasets = all_selected_datasets.union(self.search_dataset_comment(comment_csv, comment))
        all_selected_datasets = list(all_selected_datasets)
        if len(all_selected_datasets) == 1:
            dataset_name_selected = all_selected_datasets[0]
            if(dataset_name == ""):
                print(dataset_name_selected)
            return self.search_by_dataset_timestamp(dir_h5_filepath, dir_csv_filepath, dataset_name_selected, starting_timestamp, ending_timestamp, dataset_cuts)
        elif len(all_selected_datasets) != 1 and len(all_selected_datasets) != 0:
            count = 0
            for dataset in all_selected_datasets:
                print(str(count)+". "+str(dataset))
                count = count+1
            val_selected = int(
                    input("Enter which dataset to load (0,"+str(count-1)+")"))
            dataset_name_selected = all_selected_datasets[val_selected]
            return self.search_by_dataset_timestamp(dir_h5_filepath, dir_csv_filepath, dataset_name_selected, starting_timestamp, ending_timestamp, dataset_cuts)    
        else:
            print("No Dataset Found!")
            return {}
        
    def load_group_metadata(self, dataset_name, comment, starting_timestamp, ending_timestamp, dataset_cuts):
        dataset_txt, comment_csv = self.load_comment_dataset(dir_csv_central_filepath)
        all_selected_datasets = self.search_dataset_name(dataset_txt, dataset_name)
        all_selected_datasets = all_selected_datasets.union(self.search_dataset_comment(comment_csv, comment))
        all_selected_datasets = list(all_selected_datasets)
        if len(all_selected_datasets) == 1:
            dataset_name_selected = all_selected_datasets[0]
            if(dataset_name == ""):
                print(dataset_name_selected)
            return self.load_group_metadata_timestamp(dir_h5_filepath, dir_csv_filepath, dataset_name_selected, starting_timestamp, ending_timestamp, dataset_cuts)
        elif len(all_selected_datasets) != 1 and len(all_selected_datasets) != 0:
            count = 0
            for dataset in all_selected_datasets:
                print(str(count)+". "+str(dataset))
                count = count+1
            val_selected = int(
                    input("Enter which dataset to load (0,"+str(count-1)+")"))
            dataset_name_selected = all_selected_datasets[val_selected]
            return self.load_group_metadata_timestamp(dir_h5_filepath, dir_csv_filepath, dataset_name_selected, starting_timestamp, ending_timestamp, dataset_cuts)    
        else:
            print("No Dataset Found!")
            return {}

searching = Searching()
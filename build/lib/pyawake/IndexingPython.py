'''
    Title         : Indexing Python Module for pyAwake
    Author        : Aman Singh Thakur
    Last Edited   : 08 August, 2019
'''

import os,sys
import json
import ast
import time
import threading
import shutil
from datetime import datetime
import pytz
import h5py
import csv
import sys
import multiprocessing
from tempfile import NamedTemporaryFile
import shutil
import pandas as pd
import numpy as np
import csv
import math
from scipy.signal import medfilt
import matplotlib.pyplot as plt
from functools import partial

filename_split = "_"
CERN_timezone = "Europe/Zurich"
no_of_files_indexed = 0
no_of_files_searched = 0
count = 0
files_with_dataset = []
files_with_dataset_process = multiprocessing.Queue()
headers = []
cache_size = 3
comment_csv = {}
dataset_txt = set()
central_csv_name = "CentralCSV.csv"
dataset_txt_name = "dataset.csv"
comment_csv_name = "comments.csv"

fieldnames = ['DatasetName', 'GroupName', 'Dataset Attributes','DatasetValue', 'Shape', 'Datatype']
fieldnames_do_optimize = ['DatasetName', 'GroupName','DatasetValue', 'Shape', 'Datatype']

class IndexingPython:

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

    # Load Central CSV File
    def load_central_database(self, central_csv_filepath):
        central_csv_db = {}
        try:
            with open(central_csv_filepath, 'r') as csvfile:
                reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                count = 1
                for row in reader:
                    central_csv_db[row[0]] = [count, row]
                    count=count+1
        except : return central_csv_db
        return central_csv_db

    def clean_database(self, dir_filepath):
        shutil.rmtree(dir_filepath)

    def decode_attr(self, file):
        try:
            return file.decode('utf-8')
        except:
            return str(file)

    def get_dataset_attr(self, file):
        DatasetAttributes = {}
        for key in file.keys():
            val = decode_attr(file[key])
            DatasetAttributes[key] = val
        if(len(file.keys()) > 0):
            return str(DatasetAttributes)
        else:
            return ""      


    # DIVIDING DATASETS FOR THREADS
    def divide_datasets_for_threads(self, dir_h5_filepath, no_of_threads, filetype):
        all_HDF_files = self.get_all_files(dir_h5_filepath, filetype)
        max_size = len(list(all_HDF_files))
        HDF_thread_arr = []
        k, m = divmod(max_size, no_of_threads)
        return list(all_HDF_files[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(0, no_of_threads)), max_size

    # DIVIDING DATASETS AS PER THREADS AND TIMESTAMP
    def divide_datasets_for_threads_timestamp(self, all_HDF_files, no_of_threads):
        max_size = len(list(all_HDF_files))
        HDF_thread_arr = []
        k, m = divmod(max_size, no_of_threads)
        return list(all_HDF_files[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(0, no_of_threads)), max_size

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
        
    def init_csv_file(self, dir_h5_filepath, dir_csv_name, HDF_thread_arr, max_size, no_of_threads, central_csv_db, do_optimize):
        for file in HDF_thread_arr:
            csv_file_name = file[:-3]
            utc_dt, cern_dt, filename_ptr1, filename_ptr2 = self.convert_unix_time_to_datetype(
                csv_file_name)
            dir_csv_path = dir_csv_name + str(cern_dt.year)+"/"
            try:
                if not (os.path.exists(dir_csv_path)):
                    os.mkdir(dir_csv_path)
            except:
                pass
            dir_csv_path = dir_csv_path + str(cern_dt.month)+"/"
            try:
                if not (os.path.exists(dir_csv_path)):
                    os.mkdir(dir_csv_path)
            except:
                pass
            if(int(cern_dt.day)<10):
                day = "0"+str(cern_dt.day)
            else:
                day = str(cern_dt.day)
            dir_csv_path = dir_csv_path + day +"/"
            try: 
                if not (os.path.exists(dir_csv_path)):
                    os.mkdir(dir_csv_path)
            except:
                pass
            with open(dir_csv_path+"/"+csv_file_name+".csv", mode='w', newline="") as csv_file:
                writer = csv.writer(csv_file, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
                if do_optimize:
                    writer.writerow(fieldnames_do_optimize)
                else:
                    writer.writerow(fieldnames)
                file = h5py.File(dir_h5_filepath+str(file), 'r')
                self.write_into_CSV(file, writer, central_csv_db, do_optimize)
                csv_file.close()

    # Creating CSV File MULTI-THREADING FUNCTION
    def init_thread_method(self, dir_h5_filepath, dir_csv_files, central_csv_filepath, no_of_threads, dir_home):
        global central_csv_name
        dir_csv_central_filepath = central_csv_filepath+central_csv_name
        if not os.path.exists(dir_csv_files):
            os.mkdir(dir_csv_files)
        parent_directory = [x[0] for x in os.walk(dir_h5_filepath)]
        for path in parent_directory:
            temp_path = path[len(dir_home):]
            if("/" in temp_path):
                directory_arr = temp_path.split("/")
                if(len(directory_arr) == 3):
                    path += "/"
                    print(path)
                    thread_arr = []
                    HDF_thread_arr, max_size = self.divide_datasets_for_threads(
                        path, no_of_threads, "h5")
                    central_csv_db = self.load_central_database(dir_csv_central_filepath)
                    for i in range(0, no_of_threads):
                        thread_arr.append(threading.Thread(target=self.init_csv_file(
                                path, dir_csv_files, HDF_thread_arr[i],  max_size, 1, central_csv_db), name='t'+str(i)))
                        thread_arr[i].start()
                    for i in range(0, no_of_threads):
                        thread_arr[i].join()

    # Creating CSV File MULTI-PROCESSING FUNCTION
    def init_process_method(self, dir_h5_filepath, dir_csv_files, central_csv_filepath, no_of_threads, dir_home, do_optimize):
        global central_csv_name
        dir_csv_central_filepath = central_csv_filepath+central_csv_name
        if not os.path.exists(dir_csv_files):
            os.mkdir(dir_csv_files)
        parent_directory = [x[0] for x in os.walk(dir_h5_filepath)]
        for path in parent_directory:
            temp_path = path[len(dir_home):]
            if("/" in temp_path):
                directory_arr = temp_path.split("/")
                if(len(directory_arr) == 3):
                    path += "/"
                    print(path)
                    HDF_thread_arr, max_size = self.divide_datasets_for_threads(
                        path, no_of_threads, "h5")
                    process_arr = []
                    central_csv_db = self.load_central_database(dir_csv_central_filepath)
                    for i in range(0, no_of_threads):
                        process_arr.append(multiprocessing.Process(target=self.init_csv_file, args=(
                            path, dir_csv_files, HDF_thread_arr[i], max_size, no_of_threads, central_csv_db, do_optimize), name='t'+str(i)))
                        process_arr[i].start()
                    for i in range(0, no_of_threads):
                        process_arr[i].join()

    def write_into_CSV(self, file, writer, central_csv_db, do_optimize):
        if(isinstance(file, h5py.Group)):
            for sub in file.keys():
                if(isinstance(file[sub], h5py.Dataset)):
                    if not file[sub].name in central_csv_db:
                        try:
                            size = str(file[sub].size)
                        except:
                            size = "0"
                        try:
                            dataset_value = str(self.get_dataset_value(size, file[sub])).strip()
                        except:
                            dataset_value = ""
                        try:
                            datatype = str(file[sub].dtype).strip()
                        except:
                            datatype = ""
                        if do_optimize :
                            writer.writerow([file[sub].name, file.name,
                                         dataset_value, file[sub].shape, datatype])
                        else :
                            DatasetAttributes = self.get_dataset_attr(file[sub].attrs)
                            writer.writerow([file[sub].name, file.name, DatasetAttributes,
                                         dataset_value, file[sub].shape, datatype])
                    else :
                        writer.writerow([file[sub].name, file.name, central_csv_db[file[sub].name][0], "", ""])
                elif (isinstance(file[sub], h5py.Group)):
                        self.write_into_CSV(file[sub], writer, central_csv_db, do_optimize)

    def write_external_metadata(self, dir_csv_filepath, central_csv_filepath):
        global dataset_txt_name, comment_csv_name, central_csv_name, fieldnames
        count=0 
        if not os.path.exists(central_csv_filepath):
            os.mkdir(central_csv_filepath)
        parent_directory = [x[0] for x in os.walk(dir_csv_filepath)]
        comment_csv = {}
        dataset_hash = set()
        non_unique_rows = []
        similar_rows = {}
        ptr = 0
        for path in parent_directory:
            temp_path = path[len(dir_csv_filepath):]
            if("/" in temp_path):
                directory_arr = temp_path.split("/")
                if(len(directory_arr) == 3):
                    path += "/"
                    all_files = self.get_all_files(path, "csv")
                    for file in all_files:
                        with open(path+file, 'r') as csvfile:
                            reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                            ptr = 0
                            for row in reader:
                                if row[2] != "" :
                                    comment_csv[row[2]] = row[0]
                                if (row[0] in non_unique_rows):
                                    continue
                                else:
                                    if(row[0] in similar_rows):
                                        if(row[3] == str(similar_rows[row[0]][3]).strip()):
                                            continue
                                        else:
                                            print(file, row[2], row[0],str(similar_rows[row[0]][3]).strip(),row[3])
                                            similar_rows.pop(row[0], None)
                                            non_unique_rows.append(row[0])
                                    else:
                                            similar_rows[row[0]] = row
                                dataset_hash.add(row[0])
            count = count+1
        count=0
        with open(central_csv_filepath+dataset_txt_name, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in dataset_hash:
                writer.writerow([row])
                count+=1
            csvfile.close()
        with open(central_csv_filepath+comment_csv_name, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for key in comment_csv.keys():
                writer.writerow([key, comment_csv[key]])
                count+=1
            csvfile.close()
        with open(central_csv_filepath+central_csv_name, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(fieldnames_do_optimize)
            for key in similar_rows.keys():
                writer.writerow([similar_rows[key][0], similar_rows[key][1], similar_rows[key][3], similar_rows[key][4], similar_rows[key][5]])
                count+=1
            csvfile.close()
            
indexingPython = IndexingPython()
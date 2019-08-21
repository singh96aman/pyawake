'''
    Title         : Indexing Spark Module for pyAwake
    Author        : Aman Singh Thakur
    Last Edited   : 08 August, 2019
'''

import os
import h5py, io
import shutil
import csv
from csv import reader
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.functions import *
import re

prefix_domain_user = "root://eosuser.cern.ch/"
prefix_domain = "root://eospublic.cern.ch/"
dir_csv_home = "/eos/experiment/awake/CSVFiles/"
dir_spark_files = "/eos/user/a/amthakur/CSVFiles/Spark2016first/"
dir_home = "/eos/experiment/awake/event_data/"
dir_h5_filepath = "/eos/experiment/awake/event_data/2016/"
final_pair = {}

class IndexingSpark:

    def get_all_files(self, dir_filepath, file_type):
        all_files = []
        #print(dir_filepath)
        for file in os.listdir(dir_filepath):
            file_fields = file.split(".")
            if len(file_fields) > 1:
                if str(file_fields[1]) == str(file_type):
                    all_files.append(prefix_domain+dir_filepath+str(file))
                    #all_files.append(dir_filepath+str(file))
        return all_files

    def get_all_files_home_util(self, dir_h5_filepath, depth):
        if depth == 0:
            return self.get_all_files(dir_h5_filepath, "h5")
        else:
            temp_arr = os.listdir(dir_h5_filepath)
            all_H5_files = []
            for val in temp_arr:
                if val != "Central":
                    all_H5_files+=self.get_all_files_home_util(dir_h5_filepath+val+"/", depth-1)
        return all_H5_files

    def get_all_files_home(self, dir_h5_filepath, depth):
        year_directories = os.listdir(dir_h5_filepath)
        all_H5_files = []
        all_H5_files = self.get_all_files_home_util(dir_h5_filepath, depth)
        return all_H5_files

    def mkdir_structure(self, filename):
        if not os.path.exists(dir_csv_home):
            os.mkdir(dir_csv_home)
        filename = filename[int(len(prefix_domain+dir_home)-1):]
        filename_arr = filename.split(".")
        file_arr = filename.split("/")
        result = dir_csv_home
        for i in range(0, len(file_arr)-1):
            result+=file_arr[i]+"/"
            if not os.path.exists(result):
                os.mkdir(result)
            if i == 0:
                year = file_arr[i]
            elif i == 1:
                month = file_arr[i]
            elif i==2:
                day = file_arr[i]
        return year, month, day, filename_arr[0]

    def read_input(self, filepath):
        all_h5_files = []
        with open(filepath, 'r') as file:
            files_list = file.read().split(",")
            for file in files_list:
                if(file!=""):
                    all_h5_files.append(file)
        return all_h5_files

    def write_input(self, all_h5_files, filename):
        if not os.path.exists(dir_csv_home):
            os.mkdir(dir_csv_home)
        with open(dir_csv_home+filename, 'w') as file:
            for row in all_h5_files:
                file.write(row+",")

    def clean_database(self, dir_filepath):
        shutil.rmtree(dir_filepath)

    #clean_database(dir_csv_home)
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

    def decode_attr(self, file):
        try:
            return file.decode('utf-8')
        except:
            return str(file)

    def get_dataset_attr(self, file):
        comment = ""
        acqStamp = "nan"
        exception = "False"
        for key in file.keys():
            if "comment" in key:
                comment = decode_attr(file[key])
            if "acqStamp" in key:
                acqStamp = str(file[key])
            if "exception" in key:
                exception = str(file[key])
        return [comment, acqStamp, exception]

    def write_into_CSV(self, file, values, comment_hash, dataset_hash, group_attrs):
        if(isinstance(file, h5py.Group)):
            for sub in file.keys():
                try:
                    group_attrs = get_dataset_attr(file.attrs)
                    #print(file.name+" Group "+str(group_attrs))
                except:
                    group_attrs = ["", "nan", "False"]
                if(isinstance(file[sub], h5py.Dataset)):
                    try:
                        size = str(file[sub].size)
                    except:
                        size = "0"
                    try:
                        dataset_value = str(get_dataset_value(size, file[sub])).strip()
                    except:
                        dataset_value = ""
                    try:
                        datatype = str(file[sub].dtype).strip()
                    except:
                        datatype = ""
                    if (group_attrs[0]=="" and group_attrs[1]=="nan" and group_attrs[2]=="False"):
                        group_attrs = get_dataset_attr(file[sub].attrs)
                    #print(file[sub].name+" "+str(group_attrs))
                    dataset_hash.add(file[sub].name)
                    if group_attrs[0] != "":
                        comment_hash[group_attrs[0]] = file[sub].name
                    values.append([file[sub].name, file.name, group_attrs[1], group_attrs[2],
                                          dataset_value, "\""+str(file[sub].shape)+"\"", str(datatype)])
                elif (isinstance(file[sub], h5py.Group)):
                        self.write_into_CSV(file[sub], values, comment_hash, dataset_hash, group_attrs)
        return values, comment_hash, dataset_hash

    def clean_database(self, dir_filepath):
        shutil.rmtree(dir_filepath)

    #clean_database(dir_csv_home)
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

    def decode_attr(self, file):
        try:
            return file.decode('utf-8')
        except:
            return str(file)

    def get_dataset_attr(self, file):
        comment = ""
        acqStamp = "nan"
        exception = "False"
        for key in file.keys():
            if "comment" in key:
                comment = self.decode_attr(file[key])
            if "acqStamp" in key:
                acqStamp = str(file[key])
            if "exception" in key:
                exception = str(file[key])
        return [comment, acqStamp, exception]

    def write_into_CSV(self, file, values, comment_hash, dataset_hash, group_attrs):
        if(isinstance(file, h5py.Group)):
            for sub in file.keys():
                try:
                    group_attrs = self.get_dataset_attr(file.attrs)
                    #print(file.name+" Group "+str(group_attrs))
                except:
                    group_attrs = ["", "nan", "False"]
                if(isinstance(file[sub], h5py.Dataset)):
                    try:
                        size = str(file[sub].size)
                    except:
                        size = "0"
                    try:
                        dataset_value = str(get_dataset_value(size, file[sub])).strip()
                    except:
                        dataset_value = ""
                    try:
                        datatype = str(file[sub].dtype).strip()
                    except:
                        datatype = ""
                    if (group_attrs[0]=="" and group_attrs[1]=="nan" and group_attrs[2]=="False"):
                        group_attrs = self.get_dataset_attr(file[sub].attrs)
                    #print(file[sub].name+" "+str(group_attrs))
                    dataset_hash.add(file[sub].name)
                    if group_attrs[0] != "":
                        comment_hash[group_attrs[0]] = file[sub].name
                    values.append([file[sub].name, file.name, group_attrs[1], group_attrs[2],
                                          dataset_value, "\""+str(file[sub].shape)+"\"", str(datatype)])
                elif (isinstance(file[sub], h5py.Group)):
                        self.write_into_CSV(file[sub], values, comment_hash, dataset_hash, group_attrs)
        return values, comment_hash, dataset_hash

    def restructure_part():
        global dir_csv_home,dir_spark_files
        prefix_part = "part-"
        counter=0
        file_csv_counter=0
        if not os.path.exists(dir_csv_home+"Central/"):
            os.mkdir(dir_csv_home+"Central/")
        #files = os.listdir(dir_spark_files)
        comment_hash = {}
        dataset_hash = set()

        if os.path.exists(dir_csv_home+"Central/"+"comment_hash.csv"):
            with open(dir_csv_home+"Central/"+"comment_hash.csv", 'r') as csvfile:
                reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in reader:
                    comment_hash[row[0]]=row[1]

        if os.path.exists(dir_csv_home+"Central/"+"dataset_hash.csv"):
            with open(dir_csv_home+"Central/"+"dataset_hash.csv", 'r') as csvfile:
                reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in reader:
                    dataset_hash.add(row[0])

        while(counter<2):
            file_name = prefix_part+str(counter).zfill(5)
            print(file_name)
            try:
                with open(dir_spark_files+file_name) as file:
                    input_partition=file.readlines()
                    for partition in input_partition:
                        partition_dict = eval(partition)
                        for key in partition_dict.keys():
                            if("comment" in key):
                                #print(key)
                                for row in partition_dict[key].splitlines():
                                    values = row.split(",")
                                    if(len(values)==2):
                                        #print(values[0])
                                        comment_hash[values[0]]=values[1]
                            elif("dataset" in key):
                                for row in partition_dict[key].split(","):
                                    dataset_hash.add(row)
                            else:
                                file_csv_counter+=1
                                year, month, day, filename = self.mkdir_structure(key)
                                file_path = dir_csv_home+filename+".csv"
                                with open(str(file_path), 'w') as write_file:
                                    write_file.write(partition_dict[key])
                                    write_file.close()
                    file.close
                os.remove(dir_spark_files_files+file_name)
            except:
                print(file_name+" not found")
                pass
            counter = counter+1

indexingspark = IndexingSpark()
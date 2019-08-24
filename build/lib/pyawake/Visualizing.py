'''
    Title         : Viusualizing Module for pyAwake
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

import analyses.frame_analysis as fa
from utilities.custom_cmap import custom_cmap
cm = custom_cmap()


################################################################
################# Visualizing Modules ! ########################
################################################################
class Visualizing:
    
    #Prepares Image by using medfilt filter and reshaping image
    def prepare_image(self, dataset):
        ImageData = []
        ImageWidth = 0
        ImageHeight = 0
        for key in dataset.keys():
            if(isinstance(dataset[key], list) == False):
                if ("width" in key) or (key.lower().find("width") != -1):
                    ImageWidth = int(dataset[key])
                if ("height" in key) or (key.lower().find("height") != -1):
                    ImageHeight = int(dataset[key])
        if ImageWidth == 0 or ImageHeight == 0:
            print("This dataset cannot be visualized")
        else:
            ImageData = np.reshape(dataset[dataset['DatasetName']], (ImageHeight, ImageWidth))
            ImageData = medfilt(ImageData)
        return ImageData, ImageHeight, ImageWidth
    
    #Visualize a single image
    def visualize_image(self, dataset):
        if dataset == []:
            print("Empty Dataset cannot be visualized")
        else:
            ImageData, ImageHeight, ImageWidth = self.prepare_image(dataset)
            if ImageWidth == 0 or ImageHeight == 0:
                pass
            else:
                fig = plt.figure(figsize=(ImageWidth/100, ImageHeight/100))
                ax = fig.add_subplot(111)
                ax.get_xaxis().set_visible(False)
                ax.get_yaxis().set_visible(False)
                ax.set_frame_on(False)
                plt.imshow(ImageData)
                plt.show()

    #Save single/multiple images
    def save_image(self, dataset_values, image_filepath, session_name):
        if(len(dataset_values)>0):
            if(isinstance(dataset_values, list)):
                for dataset in dataset_values:
                    if dataset == []:
                        print("Empty Dataset cannot be saved")
                    else:
                        if not os.path.exists(image_filepath+session_name):
                            os.mkdir(image_filepath+session_name)
                        ImageData, ImageHeight, ImageWidth = self.prepare_image(dataset)
                        fig = plt.figure(figsize=(ImageWidth/100, ImageHeight/100))
                        ax = fig.add_subplot(111)
                        ax.get_xaxis().set_visible(False)
                        ax.get_yaxis().set_visible(False)
                        ax.set_frame_on(False)
                        dataset_arr = dataset['DatasetName'].split("/")
                        path = image_filepath+session_name+"/"+str(dataset_arr[len(dataset_arr)-1])+"_"+str(int(dataset['Timestamp'].timestamp()))+".png"
                        plt.imshow(ImageData)
                        plt.savefig(path)
                        plt.close()
            else:
                if dataset_values == []:
                        print("Empty Dataset cannot be saved")
                else:
                    if not os.path.exists(image_filepath+session_name):
                        os.mkdir(image_filepath+session_name)
                    ImageData, ImageHeight, ImageWidth = self.prepare_image(dataset_values)
                    fig = plt.figure(figsize=(ImageWidth/100, ImageHeight/100))
                    ax = fig.add_subplot(111)
                    ax.get_xaxis().set_visible(False)
                    ax.get_yaxis().set_visible(False)
                    ax.set_frame_on(False)
                    dataset_arr = dataset_values['DatasetName'].split("/")
                    path = image_filepath+session_name+"/"+str(dataset_arr[len(dataset_arr)-1])+"_"+str(int(dataset_values['Timestamp'].timestamp()))+".png"
                    plt.imshow(ImageData)
                    plt.savefig(path)
                    plt.close()
        else:
            print("No Dataset Found while saving!")

    #Visualize multiple images (~15 images)
    def visualize_multiple_image(self, dataset_list):
        total = len(dataset_list)
        count=0
        if(total>0):
            plt.figure(figsize=(70,40))
            columns = 3
            for dataset in dataset_list:
                ImageData, ImageHeight, ImageWidth = self.prepare_image(dataset)
                plt.subplot(total / columns + 1, columns, count + 1)
                plt.title(str(dataset['Timestamp']), fontsize=60)
                plt.axis('off')
                plt.imshow(ImageData)
                count = count + 1
            plt.show()
        else:
            print("Empty Dataset cannot be visualized")

    #Visualize all images in given timestamp range (~15 images)
    def visualize_datasets_in_timestamp(self, dataset_arr, dir_h5_filepath, dir_csv_filepath, dataset_name, starting_timestamp, ending_timestamp, central_csv_filepath, no_of_threads):
        if(len(dataset_arr) > 0):
            starting_timestamp = self.convert_str_to_timestamp(starting_timestamp)
            ending_timestamp = self.convert_str_to_timestamp(ending_timestamp)
            selected_timestamp_arr = []
            dataset_list = []
            count = 0
            for row in dataset_arr:
                utc_dt, cern_dt, fileptr1, fileptr2 = self.convert_unix_time_to_datetype(
                    row[0])
                if cern_dt.timestamp() >= starting_timestamp.timestamp() and cern_dt.timestamp() <= ending_timestamp.timestamp():
                    selected_timestamp_arr.append([cern_dt, dataset_arr[count]])
                    filename = convert_csv_to_h5(
                        selected_timestamp_arr[count][1][0].split("."))
                    selected_timestamp_arr[count][1][0] = dir_csv_filepath + \
                        str(selected_timestamp_arr[count][1][0])
                    dataset_list.append(get_dataset(dir_h5_filepath+filename, selected_timestamp_arr[count], dataset_name))
                count = count + 1
            self.visualize_multiple_image(dataset_list)
        else:
            print("Dataset not found")
            return {}
    
    # Display Movie using awake_analysis_tools 
    def displayMovie(self,dataset_list):
        if(len(dataset_list)!=0):
            for dataset in dataset_list:
                DatasetName = dataset['DatasetName']
                ImageData, ImageHeight, ImageWidth = self.prepare_image(dataset)
                width_axis = np.linspace(-7.5,7.5,ImageWidth)
                stk_ana = fa.FrameAna(ImageData,width_axis,ImageHeight)
                stk_ana.median_filter = True
                stk_ana.roi = [-4, 4, ImageWidth, ImageHeight]
                fig = plt.figure(1)
                im = plt.imshow(stk_ana.frame, extent=(stk_ana.roi[0], stk_ana.roi[1], stk_ana.roi[3], stk_ana.roi[2]), aspect='auto',cmap = cm)
                plt.colorbar(im)
                plt.title(DatasetName, fontsize=8)
                plt.xlabel('X [mm]',fontsize=14)
                plt.ylabel('T [ps]',fontsize=14)
                fig.canvas.draw()
                plt.clf()

            fig = plt.figure(1)
            im = plt.imshow(stk_ana.frame, 
                                extent=(stk_ana.roi[0], stk_ana.roi[1], stk_ana.roi[3], stk_ana.roi[2]), aspect='auto',cmap = cm)
            plt.colorbar(im)
            plt.title(DatasetName, fontsize=8)
            plt.xlabel('X [mm]',fontsize=14)
            plt.ylabel('T [ps]',fontsize=14)
            fig.canvas.draw()
        else:
            print("Empty Dataset List")
        
visualizing = Visualizing()
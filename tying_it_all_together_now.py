#!/usr/bin/python

import warnings
warnings.filterwarnings('ignore')

import os

from skopt.space import Integer
from skopt import Optimizer as BO
import numpy as np
import logging
import sys,signal

import threading
import pathlib
from time import sleep
from time import time
from argparse import ArgumentParser
import subprocess

import pandas as pd
import logging as logger
import pycurl
import statistics

import multiprocessing 
from multiprocessing import Manager


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
logger.basicConfig(format=log_FORMAT, 
                    datefmt='%m/%d/%Y %I:%M:%S %p', 
                    level=logger.INFO)

output_directory = 'data/'

target_throughput = 0

finished_file_bytes = 0
thread_list = []

lock_sample_list = multiprocessing.Lock()
lock_active_transfer_list = multiprocessing.Lock()

def download_monitor (shared_dict):
    global finished_file_bytes, concurrency
    last_size = 0
    last_concurrency_update_t = 0
    throughput_list = []

    while True:
        temp_sample_list = list(shared_dict['sample_list'])
        temp_active_transfer_list = list(shared_dict['active_transfer_list'])
        temp_ccs = list(shared_dict['ccs'])
        temp_average_throughputs = list(shared_dict['average_throughputs'])
        temp_offset_dict = dict(shared_dict['offset_dict'])
        sleep(1)
        size = finished_file_bytes
        if len(temp_sample_list) == 0 and len(temp_active_transfer_list) == 0:
           return
        for f in temp_active_transfer_list:
            try:
                temp_offset_dict[f] = pathlib.Path(f).stat().st_size
                shared_dict['offset_dict'] = temp_offset_dict
                size += pathlib.Path(f).stat().st_size
            except:
                continue
        throughput = ((size-last_size)*8)/(1000*1000.0)
        if throughput == 0:
            continue
        throughput_list.append(throughput)
        print ("Throughput {} Mbps, target {} Mbps, Active files: {}, Remaining files {}".format(throughput, target_throughput, len(temp_active_transfer_list), len(temp_sample_list)))
        last_size = size
        if len(throughput_list) > 6 and time() > last_concurrency_update_t + 7:
            if len(temp_sample_list) < 2:
                continue

            temp_average_throughputs.append(statistics.mean(throughput_list[-5:]))
            shared_dict['average_throughputs'] = temp_average_throughputs
            # average_throughputs_length = len(average_throughputs)

            # set temp to the concurrency value that is passed

            if concurrency != temp_ccs[-1]:
                # if ccs[-1] > concurrency add threads
                # else remove threads (maybe kill)
                # file might not be done downloading
                # One method:
                # start where the file left off

                if temp_ccs[-1] - concurrency > 0:
                    add_more_processes(temp_ccs[-1] - concurrency, shared_dict)
                else:
                    remove_some_processes(concurrency - temp_ccs[-1], shared_dict)
                concurrency = temp_ccs[-1]
            
            last_concurrency_update_t = time()
            throughput_list = []
    return

def file_downloader (shared_dict):
    global finished_file_bytes
    print("Running thread...")
    curl = pycurl.Curl()
    
    temp_sample_list = list(shared_dict['sample_list'])
    temp_active_transfer_list = list(shared_dict['active_transfer_list'])
    temp_offset_dict = dict(shared_dict['offset_dict'])
    while True:
        lock_sample_list.acquire()
        if len(sample_list) == 0:
            print("Exiting thread...")
            return
        # Fetch a file from file list and add is to currently transferred file list
        # Synchronized operation due to using concurrency
        filename = temp_sample_list.pop()
        shared_dict['sample_list'] = temp_sample_list
        lock_sample_list.release()

        lock_active_transfer_list.acquire()
        file_path = output_directory + filename

        temp_active_transfer_list.append(file_path)
        shared_dict['active_transfer_list'] = temp_active_transfer_list

        lock_active_transfer_list.release()


        # initialize and start curl file download
        sample_ftp_url = discover_ftp_paths([filename])[0]
        #print("Starting to download " + filename, sample_ftp_url)
        fp = open(file_path, "wb")
        curl.setopt(pycurl.URL, sample_ftp_url)
        curl.setopt(pycurl.WRITEDATA, fp)
        curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)
        curl.setopt(pycurl.LOW_SPEED_TIME, 2)
        
        range_string = 'Range: bytes=' + str(temp_offset_dict[file_path]) + '-'

        header = [range_string]
        curl.setopt(pycurl.HTTPHEADER, header)

        #curl.setopt(curl.NOPROGRESS, False)
        #curl.setopt(curl.XFERINFOFUNCTION, status)
        retry = 0
        while retry < 3:
            try:
                curl.perform()
                file_size = pathlib.Path(file_path).stat().st_size
                finished_file_bytes += file_size
                offset_dict[filename] = finished_file_bytes
                shared_dict['offset_dict'] = offset_dict
                print("Finished {} size: {} MB".format(filename, (file_size/(1024*1024))))
            except pycurl.error as exc:
                print("Unable to download file %s (%s)" % (filename, exc))
                retry +=1
            finally:
                # Remove file from currently transferred file list
                fp.close()
                lock_active_transfer_list.acquire()

                temp_active_transfer_list.remove(file_path)
                shared_dict['active_transfer_list'] = temp_active_transfer_list

                lock_active_transfer_list.release()
                break
        if retry == 3:
            print("Download attempt for file %s (%s) failed () times" % (filename, exc, retry))
            lock_sample_list.acquire()

            temp_sample_list.append(filename)
            shared_dict['sample_list'] = temp_sample_list

            lock_sample_list.release()
    curl.close()

def discover_ftp_paths (sample_list):
    command = "srapath " + " ".join(sample_list)
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE)
    paths = result.stdout.decode('utf-8').splitlines()
    #print ("command", command, " paths:", paths, "hah")
    return paths


fileList = []

def add_more_processes(count, shared_dict):
    for i in range(count):
        thread = multiprocessing.Process(target=file_downloader, \
                                  args=(shared_dict,), daemon=True)
        print("Creating new thread ")
        thread.start()
        thread_list.append(thread)

def remove_some_processes(count, shared_dict):

    temp_sample_list = list(shared_dict['sample_list'])
    temp_active_transfer_list = list(shared_dict['active_transfer_list'])
    temp_offset_dict = dict(shared_dict['offset_dict'])    
    for i in range(count):
        lock_active_transfer_list.acquire()
        filename = temp_active_transfer_list.pop()
        temp_offset_dict[filename] = pathlib.Path(f).stat().st_size
        shared_dict['offset_dict'] = temp_offset_dict
        shared_dict['active_transfer_list'] = temp_active_transfer_list
        lock_active_transfer_list.relese()

        lock_sample_list.acquire()
        temp_sample_list.append(filename[4:len(filename)])
        shared_dict['sample_list'] = temp_sample_list
        lock_sample_list.release()

        x = thread_list.pop()
        x.terminate()

def harp_response(params, count, shared_dict):
    global max_cc, thrpt
    cc = params[0]
    logger.info("Iteration {0} Starts ...".format(count))
    logger.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    thrpt = 0
    while True:
        temp_average_throughputs = list(shared_dict['average_throughputs'])
        while thrpt != temp_average_throughputs[-1]:
            try:
                thrpt = temp_average_throughputs[-1]
                if thrpt is not None:
                    break
                
            except Exception as e:
                logger.exception(e)
                thrpt = -1
                    
        if thrpt == -1:
            logger.info("Optimizer Exits ...")
            exit(1)
        else:
            score = (thrpt/(1.02)**cc) * (-1)
        
        logger.info("Sample Transfer -- Throughput: {0}Mbps, Score: {1}".format(
            np.round(thrpt), score))
        return score


def gradient(black_box_function, shared_dict):
    global thrpt
    max_thread, count = max_cc, 0
    soft_limit, least_cost = max_thread, 0
    values = []
    theta = 0

    while True:
        sleep(0.05)
        temp_average_throughputs = list(shared_dict['average_throughputs'])
        temp_ccs = list(shared_dict['ccs'])
        while len(temp_average_throughputs) > 0 and thrpt != temp_average_throughputs[-1]:
            count += 1
            values.append(black_box_function([temp_ccs[-1]], count, shared_dict))
            if values[-1] < least_cost:
                least_cost = values[-1]
                soft_limit = min(ccs[-1]+10, max_thread)
            
            if len(temp_ccs) == 1:
                temp_ccs.append(2)
                shared_dict['ccs'] = temp_ccs
            
            else:
                dist = max(1, np.abs(temp_ccs[-1] - temp_ccs[-2]))
                if temp_ccs[-1]>temp_ccs[-2]:
                    gradient = (values[-1] - values[-2])/dist
                else:
                    gradient = (values[-2] - values[-1])/dist
                
                if values[-2] !=0:
                    gradient_change = np.abs(gradient/values[-2])
                else:
                    gradient_change = np.abs(gradient)
                
                if gradient>0:
                    if theta <= 0:
                        theta -= 1
                    else:
                        theta = -1
                        
                else:
                    if theta >= 0:
                        theta += 1
                    else:
                        theta = 1
            

                update_cc = int(theta * np.ceil(temp_ccs[-1] * gradient_change))
                next_cc = min(max(temp_ccs[-1] + update_cc, 2), soft_limit)
                logger.info("Gradient: {0}, Gradient Change: {1}, Theta: {2}, Previous CC: {3}, Choosen CC: {4}".format(gradient, gradient_change, theta, temp_ccs[-1], next_cc))
                temp_ccs.append(next_cc)
                shared_dict['ccs'] = temp_ccs


if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument('-i', '--input',
                  action="store", dest="sample_list",
                  help="input file for sample list", default="samples.tsv")
    parser.add_argument('-o', '--output',
                      action="store", dest="output_directory",
                      help="output directory to save sample files", default="data")
    parser.add_argument('-t', '--target',
                      action="store", dest="target_throughput", type=int,
                      help="target throughput for the transfer", default=0)

    args = parser.parse_args()

    sample_file = pd.read_csv(args.sample_list, sep="\s+", dtype=str).set_index("sample", drop=False)

    manager = Manager()
    shared_dict = manager.dict()

    active_transfer_list = []
    sample_list = sample_file["sample"].values.tolist()

    offset_dict = {}
    for i in sample_list:
        offset_dict['data'+i] = 0

    average_throughputs = []
    ccs = [1]

    shared_dict['active_transfer_list'] = active_transfer_list
    shared_dict['sample_list'] = sample_list
    shared_dict['offset_dict'] = offset_dict

    shared_dict['average_throughputs'] = average_throughputs
    shared_dict['ccs'] = ccs

    max_cc = 100
    thrpt = 0
    concurrency = ccs[-1]

    output_directory = args.output_directory
    pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)


    t = multiprocessing.Process(target=download_monitor, \
                     args=(shared_dict, )) # TODO: POSSIBLY ADD DAEMON = TRUE BACK TO THIS PROCESS
    gradient_thread = multiprocessing.Process(target=gradient, args=((harp_response, shared_dict)))
    t.start()
    gradient_thread.start()
    add_more_processes(concurrency, shared_dict)
    t.join()

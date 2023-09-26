#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
import os
import yaml
import json
import logging
from shutil import copy2

"""
Python methods for common tasks
"""
def findFileFromDirectoryRecursively(filename, search_path):
    """
    This function used as utility to search files , files are sorted as per OS. 
    Parameters
    ----------
        filename: str
            Name of file for serach
        serch_path: str
            search directory path
    Examples:
        files = findFileFromDirectoryRecursively("test.txt", ".") # single dot for current directory and .. for parent directory 
                or one directory up
        Output :["/test/text.txt"]
        
    Returns
    -------
     result: list
        returns list of files
    """
  
    result = []
    # Walking top-down from the root
    try:
        for root, dir, files in os.walk(search_path):
            if filename in files:
                result.append(os.path.join(root, filename))
        logging.debug(f"file list:{result}")
        return result
    except Exception as e:
        logging.error(f"In Exception****: {e}")
        logging.debug(f"Parameter:filename:{filename} and searach_path:{search_path}")
        raise e
        
        
    

def readJsonFile(file):
    """
    this function used for read json file 
    Parameters
    ----------
        file: str
            Name of file
    Returns
    -------
        keys: dict
            Returns keys
    
    """
    try:
        filePath = findFileFromDirectoryRecursively(file,"..")
        logging.debug(f"filepath :{filePath}")
        with open(filePath[0]) as fh:
            keys = json.load(fh)
        #logging.debug(f"keys :{keys}")
        return keys
    except Exception as e:
        logging.error(f"In Exception****: {e}")
        logging.debug(f"Parameters file:{file}")
        raise e
        
        
    

def readYamlFile(file):
    """
    this function used for Read Yaml file
    Parameters
    ----------
        file: str
            Name of file 
    Returns
    -------
        parameter_list: dict
            Returns parameter dictionary
    """
    try:
        filePath = findFileFromDirectoryRecursively(file,"..")
        logging.debug(f"filepath :{filePath}")
        if filePath!=[]:
            with open(filePath[0]) as file:
                parameter_list = yaml.load(file, Loader=yaml.FullLoader)
            logging.debug(f"parameter list: {parameter_list}")
            return parameter_list
    except Exception as e:
        logging.error(f"In Exception****: {e}")
        logging.debug(f"Parameters file::{file}")
        #raise ErrorHandler(type(e).__name__,e.args[0])
        raise e
   
       
        
        
    

def readTextFile(file):
    """
    this function used for Read Text file
    Parameters
    ----------
        file: str
            Name of file 
    Returns
    -------
        var: str
            Returns text string
    """
    try:
        filePath = findFileFromDirectoryRecursively(file,"..")
        logging.debug(f"filepath :{filePath}")
        vars = open(filePath[0], "r")
        logging.debug(f"vars :{vars}")
        return vars
    
    except Exception as e:
        logging.error(f"In Exception****: {e}")
        logging.debug(f" Parameters file::{file}")
        #raise ErrorHandler(type(e).__name__,e.args[0])
        raise e
        
    

def writeJsonFile(file,data):
    """
    this function used for write json file 
    Parameters
    ----------
        file: str
            Name of file 
        data: str
            data with contains string
    Returns
    -------
        None
    """
    try:
        with open(file, 'w') as outfile:
            json.dump(data, outfile, indent=2, sort_keys=True)
    
    except Exception as e:
        logging.error(f"In Exception****: {e}")
        logging.error(f"Parameters file:{file} and data:{data}")
        raise e
        

def copy_file(filename, targetpath):
    """ 
    This function used for copy file from source to target # used in Static Analysis in PR flow. 
    Parameters
    ----------
        filename: str
            Name of file
        targetpath: str
            Path of folder where we copy file
    Returns
    -------
        result: str
            return path of copied file   
    
    """
    filepath = []
    try: 
        logging.debug(filename)
        if("/" in filename):
            filename = filename.split("/")[-1]
        filepath = findFileFromDirectoryRecursively(filename, "..")
        logging.debug(filepath)
        if len(filepath) != 0:
            result = copy2(filepath[0],targetpath)
            return result
    
    except Exception as e:
        logging.error(f"In Exception****: {e}")
        logging.error(f"Parameters file:{filename}and Targetpath:{targetpath}")
        raise e
    

def writetextFile(file,data):
    """
    this function used for write Text file 
    Parameters
    ----------
        file: str
            Name of file 
        data: str
            data with contains string
    Returns
    -------
        None
    """
    try:
        logging.debug(f"file writing start: {file}")
        f= open(file,"a")        
        f.write(data)
        f.close() 
        logging.debug(f"file writing end: {file}")
    
    except Exception as e:
        logging.error(f"In Exception****: {e}")
        logging.error(f"Parameters file:{file} and data:{data}")
        raise e
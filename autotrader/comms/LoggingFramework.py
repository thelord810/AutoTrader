import os
import yaml
import logging.config
import logging
from os import path
import re
from autotrader_custom_repo.AutoTrader.autotrader.comms import fileTools
import json
import pprint

def setup_logging(name=__name__, default_level=logging.INFO):
    """ Function to setup logging.
    Parameters
    ---------
    name: str
        Name of logger. This name has to be one of entries in logger section of "logging.yaml" in config folder.

    default_level: str
        This overrides root logger settings. 

    Raises
    ------
    Yaml load exception given by yaml class.

    Returns
    ------
    Logger object

    """
    log_file_path = fileTools.findFileFromDirectoryRecursively("logging.yaml","..")
    if os.path.exists(log_file_path[0]):
        with open(log_file_path[0], 'rt') as f:
            try:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
            except Exception as e:
                print('Error in Logging Configuration. Using default configs')
                print(e)                
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        print('Failed to load configuration file. Using default configs')
        raise Exception("Failed to load configuration file. Using default configs")

    logger = logging.getLogger(name)
    logger.propagate = False
    return logger



class RedactingFilter(logging.Filter):

    """
    This class for filter log or mask our secret credential
        Filter class used to inherit filter

    """
    
    def __init__(self, patterns):
        """
        It is used for pass patterns for masking
        
        """
        super(RedactingFilter, self).__init__()
        self._patterns = patterns
        

    def filter(self, record):
        """
        filter class to filer log messages
        Parameters
        ----------
            record: obj
            emit log record
        Returns
        -------

            True or False
        """
        record.msg = self.redact(record.msg)
        return True

    def redact(self, msg):
        """
        This function used for replace messages
        
        Parameters
        ----------

            msg: log message

                This is log message 
        
        Returns
        -------
        Return log message with masking

        """
        change_str = 0
        if isinstance(msg,str):
            try: 
                msg =msg.replace('\\"',"'").replace('"',"'")
                msg = json.loads(msg)
                change_str = 1
            except ValueError as e: 
                change_str = 0
               
        for pattern in self._patterns:
            regex_str = re.compile(pattern)
            result=re.sub(regex_str, r"\1******'", str(msg) if not isinstance(msg,str) else msg)
            if change_str == 1:
                msg = json.dumps(eval(result),indent=3)
            else:
                msg = result
        msg = msg.replace("'",'"')
        return msg

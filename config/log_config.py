import logging

LOG_CONFIG = {
    'version':1,
    'root':{
        'handlers' : ['console', 'file'],
        'level': 'DEBUG'
    },
    'handlers':{
        'console':{
            'formatter': 'std_out',
            'class': 'logging.StreamHandler',
            'level': 'DEBUG'
        },
        "file":{
            "formatter":"std_out",
            "class":"logging.FileHandler",
            "level":"WARNING",
            "filename":"error_log.log",
            'mode': 'w'
        }

    },
    'formatters':{
        'std_out': {
            'format': '%(asctime)s : %(levelname)s : %(name)s : %(message)s',
            'datefmt':'%Y-%m-%d %I:%M:%S'
        }
    },
    'disable_existing_loggers': False
}

def disable_third_party_loggers(): 
    logging.getLogger('urllib3').setLevel(logging.CRITICAL+1)
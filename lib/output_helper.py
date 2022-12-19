import os

def get_logging_dir():
    return create('log')

def get_output_base_dir():
    return create('output')

def create(path):

    try:
        os.mkdir(path)
    except:
        pass

    return path
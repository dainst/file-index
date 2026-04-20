import os

def get_logging_dir(target_directory=""):
    return create(f'log/{target_directory}')

def get_output_base_dir(target_directory=""):
    return create(f'output/{target_directory}')

def create(path):

    try:
        os.makedirs(path)
    except:
        pass

    return path
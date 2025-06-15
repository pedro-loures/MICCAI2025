################################
# This code parse through all dicon files in a given directory and subdirectories,
#   conserve the file structure, and merge them into a single CSV file.
#   The tags colected are the ones necessary to statically classify the sequence
################################

# data handling imports
import json
import pandas as pd
import numpy as np

# system imports
import os
from datetime import datetime, timedelta
import argparse

# Logging imports
import logging

# Utility imports
from tqdm import tqdm
import random

# DICOM imports
import pydicom

# Constants
STARTING_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_PATH = f"logs/{STARTING_TIME}_merger.log"


# Ensure the logs directory exists
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)


# Create the logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

if logger.hasHandlers():
    logger.handlers.clear()

# File Handler
file_handler = logging.FileHandler(LOG_PATH, mode='w')
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
file_handler.setLevel(logging.DEBUG)

# Console Handler
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter('TERMINAL: %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
console_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


logger.info(f"Log path: {LOG_PATH}")


def list_dicom_files(path):
    """
    List all DICOM files in the given path and its subdirectories.
    
    arguments:
        - (str) path: The path to search for DICOM files.
    
    returns:
        - (list) A list of paths to DICOM files.
    """
    dicom_files = []
    for root, _, files in os.walk(path):
        for file in files:
            if file.lower().endswith('.dcm'):
                dicom_files.append(os.path.join(root, file))
    return dicom_files

def convert_string_to_tag(tag_str):
    # Convert string "(gggg,eeee)" to Tag object
    tag_tuple = tuple(int(x, 16) for x in tag_str.strip("()").split(","))
    tag = pydicom.tag.Tag(tag_tuple)
    return tag

def main():
    """
    Main function to merge DICOM files into a single CSV file.
    
    arguments:
        - (str) path_shared_folder: The path to the shared folder containing DICOM files.
        - (str) output_dir: The path where the merged CSV file will be saved. (optional, default: f'results/{shared_folder}.csv')

    """
    # parse arguments
    parser = argparse.ArgumentParser(prog="merger.py", 
                                     description="Merge DICOM files into a single CSV file.",
                                     epilog="Example usage: python merger.py /path/to/shared_folder -o /path/to/output.csv")
    parser.add_argument("path_shared_folder", type=str, help="The path to the shared folder containing DICOM files.")
    parser.add_argument('-o', '--output_dir', type=str, default=None, help="The path where the merged CSV file will be saved. (optional, default: f'results/{shared_folder}.csv')")
    args = parser.parse_args()

    path_shared_folder = args.path_shared_folder
    shared_folder = os.path.basename(os.path.normpath(path_shared_folder))
    if args.output_dir is None:
        output_dir = f'results/{shared_folder}.csv'
    else:
        output_dir = args.output_dir
        if not output_dir.endswith('.csv'):
            logger.warning(f"The output directory {output_dir} does not end with '.csv'. Appending '.csv'.")
            output_dir += '.csv'
    
    logger.info(f"Shared folder path: {path_shared_folder}")
    logger.info(f"Shared folder: {shared_folder}")
    logger.info(f"Output directory: {output_dir}")    
    
    # Ensure the output directory exists
    if not os.path.exists(path_shared_folder):
        logger.warning(f"The shared folder {path_shared_folder} does not exist. Creating one...")
        os.makedirs(os.path.dirname(output_dir), exist_ok=True)

    # List all DICOM files in the shared folder
    logger.info("Listing all DICOM files in the shared folder...")
    dicom_files = list_dicom_files(path_shared_folder)
    if not dicom_files:
        logger.error("No DICOM files found in the shared folder.")
        return 1
    logger.info(f"Found {len(dicom_files)} DICOM files.")
    logger.debug(f"DICOM files: {random.sample(dicom_files, 10)}")  # Log random files for debugging

    # Read selected_tags.json to get the tags to be extracted
    tags_path = 'selected_tags.json'
    if not os.path.exists(tags_path):
        logger.error(f"The tags file {tags_path} does not exist.")
        return 1
    with open(tags_path, 'r') as f:
        try:
            selected_tags = json.load(f)
            logger.info(f"Loaded {len(selected_tags)} tags from {tags_path}.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {tags_path}: {e}")
            return 1
    logger.debug(f"Selected tags: {selected_tags}")
    
    # Merge DICOM files into a single csv without loading all data into memory
    logger.info("Merging DICOM files into a single CSV file...")
    logger.info(f"Creating output file at {output_dir}")
    output_file = open(output_dir, 'w')
    output_file.write(';'.join(list(selected_tags.keys()) + ['FilePath']) + '\n')  # Write header
    logger.info("Header written to the output file.")
    logger.debug(f"Header: {list(selected_tags.keys()) + ['FilePath']}")
    logger.info("Processing DICOM files...")
    for file in tqdm(dicom_files, desc="Processing DICOM files"):
        try:
            # Read the DICOM file
            ds = pydicom.dcmread(file, stop_before_pixels=True)
            ds_keys = list(ds.keys())


            logger.debug(f" Header1 : {ds_keys[0]} selected_tag1: {list(selected_tags.keys())[0]}")

            # Extract the selected tags
            data = {}
            for tag_str in selected_tags.keys():
                try:
                    tag = convert_string_to_tag(tag_str)
                    value = ds.get(tag, None)
                    value = value.value if value is not None else None  # Get the value, or None if not found
                    
                except Exception as e:
                    logger.warning(f"Failed to parse or get tag {tag_str}: {e}")
                    value = None
                value = str(value).replace(';', ',') if value is not None else ''  # Replace semicolons to avoid CSV issues
                data[tag_str] = value
            logger.debug(f"Extracted data for {file}: {data}")  # Log the extracted data for debugging
            
            data['FilePath'] = file  # Add the file path to the data
            output_file.write(';'.join(str(data.get(tag, '')) for tag in list(selected_tags.keys()) + ['FilePath']) + '\n')
            logger.debug(f"Processed file: {file}")

        except Exception as e:
            logger.error(f"Error reading {file}: {e}")

    output_file.close()
    return 1

if __name__ == "__main__":
    logger.info("Starting the merger script...")
    start_time = datetime.now()
    success = True
    try:
        main()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        success = False
        
    if success:
        logger.info("Script completed successfully.")
    else:
        logger.error("Script encountered an error during execution.")
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    logger.info(f"Script completed in {elapsed_time}.")
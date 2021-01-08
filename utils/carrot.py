#!/usr/bin/python
import argparse
import json
import logging
import os
import sys

# non native packages
import pathspec

EXCLUDED_FILES = ['carrot.py', 'carrot.json', 'LICENSE', 'README.md', '.gitignore', 'requirements.txt' ]
EXCLUDED_EXTS = ['.enc']
PRINT_ONLY = False
APPLICATION_BASE_NAME = "Renamer"
LOG_FORMATTER = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

#----------------------------------------------------------------
# Data Plane 


def should_process( root, name):
    return name not in EXCLUDED_FILES and os.path.splitext(name)[1] not in EXCLUDED_EXTS and not SPEC.match_file(os.path.join(root, name))

def rename_if_needed(root, name):
    raw_name, ext = os.path.splitext(name)
    
    if raw_name in RENAME_MAP:
        filename = os.path.join(root, name)
        new_filename = os.path.join(root, name.replace(raw_name, RENAME_MAP[raw_name]))
        logger.info(f"renaming {filename} to {new_filename}")
        if not PRINT_ONLY:
            os.rename(filename,new_filename)

def search_replace_file(root, name):
    filedata = None
    filename = os.path.join(root, name)

    with open(filename, 'r+') as f:
        orig_data = filedata = f.read()
        # Replace the target strings
        for k, v in RENAME_MAP.items():
            filedata = filedata.replace(k,v)
        
        if orig_data != filedata:
            logger.info(f"replacing file contents for {name}")
            if not PRINT_ONLY:
                f.seek(0)
                f.write(filedata)
                f.truncate()

def handle_dirs(root, dirs):
    for name in dirs:
        if should_process(root, name):
            logger.debug(f"checking dir {name}")
            rename_if_needed(root, name)

def handle_files(root, files):
    for name in files:
        if should_process(root, name):
            logger.debug(f"checking file {name}")
            try:
                search_replace_file(root, name)
            except UnicodeDecodeError:
                logger.info(f'unable to decode file {name}. skipping.')

            rename_if_needed(root, name)

#----------------------------------------------------------------
# Control Plane 

if __name__ == '__main__':
    
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument("rename_map", help="json file used to rename. text / filenames matching keys will be mapped to values.")
    parser.add_argument("gitignore", help="path to gitignore file")
    parser.add_argument("--print_only", help="only print operations that will be applied", action="store_true")
    parser.add_argument("--root", help="path to root directory", default="../")
    parser.add_argument("--reverse", help="renames keys to values from rename_map json file", action="store_true")
    parser.add_argument("--loglevel", help="sets the log level. defaults to INFO.", default="INFO")
    args = parser.parse_args()

    # set up logging
    logging.basicConfig(format=LOG_FORMATTER,stream=sys.stdout)
    logger = logging.getLogger(APPLICATION_BASE_NAME)
    logger.setLevel(args.loglevel)

    logger.info(f"carrot starting with {args}")
    
    with open(args.gitignore, 'r') as fh:
        SPEC = pathspec.PathSpec.from_lines('gitwildmatch', fh)

    with open(args.rename_map, 'r') as f:
        RENAME_MAP = json.load(f)

    if args.reverse:
        RENAME_MAP = {v: k for k, v in RENAME_MAP.items()}

    if args.print_only:
        PRINT_ONLY = True

    logger.info(f"loaded mapping {RENAME_MAP}")

    for root, dirs, files in os.walk(args.root, topdown=False):
        handle_files(root, files)
        handle_dirs(root, dirs)

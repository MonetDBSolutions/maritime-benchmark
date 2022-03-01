#!/usr/bin/env python3

import argparse
import logging
import os

parser = argparse.ArgumentParser()

parser.add_argument('--data', type=str, default=os.getcwd()+'/data',
                    help='Absolute path to the dataset directory')
parser.add_argument('--scale', type=float, nargs='+', default=0,
                    help='Scale factors (only values < 1 allowed)',
                    required=True)

args = parser.parse_args()
logger = logging.getLogger(__name__)

# for the time being we only scale dynamic AIS data
bench_files = [
'[P1] AIS Data/nari_dynamic.csv',
]

def scaleCSV(file, scale):
    # create the scaled file name and check if exists 
    scaled_file = f'{file[:-4]}_SF_{scale}.csv'
    if os.path.isfile(scaled_file):
        logger.warning(f'Scaled file {scaled_file} already exists!')
        return
    with open(file, 'r') as input_file, \
         open(scaled_file, 'w+') as output_file:
        try:
            lines = input_file.readlines()
            new_lines_cnt = int(len(lines) * scale)
            output_file.writelines(lines[0:new_lines_cnt])
            logger.debug(f'Scaled {file} from {len(lines)} to {new_lines_cnt} lines')
        except IOError as msg:
            logger.warning('Scaling failed for file:{file} SF:{scale}')
            logger.exception(msg)
    return

if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    # TODO: do we really need a stream handler?
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    for sf in args.scale:
        for file in bench_files:
            data_dir = os.path.abspath(args.data)
            scaleCSV(f'{data_dir}/{file}', sf)


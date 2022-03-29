#!/usr/bin/env python3

import argparse
import logging
import os

parser = argparse.ArgumentParser()

parser.add_argument('--data', type=str, default=os.getcwd()+'/data',
                    help='Absolute path to the dataset directory')
parser.add_argument('--scale', type=float, nargs='+', required=True,
                    help='Scale factors (only values < 1 allowed)')
parser.add_argument('--target', type=str, default=os.getcwd()+'/gen',
                    help='Path to the directory where the scaled '
                    'datasets will be placed')

args = parser.parse_args()
logger = logging.getLogger(__name__)

# for the time being we only scale dynamic AIS data
bench_files = [
'[P1] AIS Data/nari_dynamic.csv',
]

def scaleCSV(src_lines, dst, scale):
    with open(dst, 'w+') as output_file:
        try:
            new_lines_cnt = int(len(src_lines) * scale)
            output_file.writelines(src_lines[0:new_lines_cnt])
            logger.info(f'Scales {len(src_lines)} to {new_lines_cnt} lines')
        except IOError as msg:
            logger.warning('Scaling failed for file:{src} SF:{scale}')
            logger.exception(msg)


def main():

    # for every file to be scaled
    for file in bench_files:
        
        # read the lines of the input once 
        src_lines = None
        with open(f'{args.data}/{file}', 'r') as src_file:
            src_lines = src_file.readlines()
        if len(src_lines) == 0:
            raise IOError('Source file {file} seems to be empty!')
        logger.info(f'Going to scale {file}')

        # for every scale factor
        for sf in args.scale:
            scaled_filename = f'{args.target}/{file[:-4]}_SF_{sf}.csv'
            
            # check if the scaled file exists 
            if os.path.isfile(scaled_filename):
                logger.info(f'Scaled file {scaled_filename} already exists!')
                continue
            else:
                # check if the dir of the scaled file exists
                directory = os.path.dirname(scaled_filename)
                if not os.path.exists(directory):
                    logger.info(f'Creating directory {directory}')
                    os.makedirs(directory)
                
            # scale the file
            scaleCSV(src_lines, scaled_filename, sf)


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    # TODO: do we really need a stream handler?
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    main()


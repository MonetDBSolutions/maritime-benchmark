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
parser.add_argument('--target', type=str, default=os.getcwd()+'/gen',
                    help='Path to the directory where the scaled '
                    'datasets will be placed')

args = parser.parse_args()
logger = logging.getLogger(__name__)

# for the time being we only scale dynamic AIS data
bench_files = [
'[P1] AIS Data/nari_dynamic.csv',
]

def scaleCSV(src, dst, scale):
    with open(src, 'r') as input_file, \
         open(dst, 'w+') as output_file:
        try:
            lines = input_file.readlines()
            new_lines_cnt = int(len(lines) * scale)
            output_file.writelines(lines[0:new_lines_cnt])
            logger.debug(f'Scaled {src} from {len(lines)} to {new_lines_cnt} lines')
        except IOError as msg:
            logger.warning('Scaling failed for file:{src} SF:{scale}')
            logger.exception(msg)
    return

def main():
    for sf in args.scale:
        for file in bench_files:
             
            scaled_file = f'{args.target}/{file[:-4]}_SF_{sf}.csv'
            
            # check if the scaled file exists 
            if os.path.isfile(scaled_file):
                logger.info(f'Scaled file {scaled_file} already exists!')
                continue
            else:
                logger.info(f'Creating {scaled_file}')
                # check first if the dir of the scaled file exists
                directory = os.path.dirname(scaled_file)
                if not os.path.exists(directory):
                    logger.info(f'Creating directory {directory}')
                    os.makedirs(directory)
                # create the scaled file
                fd = os.open(scaled_file, os.O_CREAT)
                os.close(fd)
           
            # scale the file
            scaleCSV(f'{args.data}/{file}', scaled_file, sf)


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    # TODO: do we really need a stream handler?
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    main()


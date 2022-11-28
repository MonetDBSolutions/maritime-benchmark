#!/usr/bin/env python3

import argparse
import logging
import os
import shutil

intro = """
        This script scales the Zenodo maritime AIS data according to the
        scale factor given. Multiple scale factors could be provided in
        a single run. The files to be scaled are hardcoded in the script
        for the time being.
        """

parser = argparse.ArgumentParser(description=intro)

parser.add_argument('--data', type=str, default=os.getcwd()+'/data',
                    help='Path to the dataset directory')
parser.add_argument('--scale', type=float, nargs='+', required=True,
                    help='Scale factors (only values < 1 allowed)')
parser.add_argument('--target', type=str, default=os.getcwd()+'/gen',
                    help='Path to the directory where the scaled '
                    'datasets will be placed')

args = parser.parse_args()
logger = logging.getLogger(__name__)

# for the time being we only scale dynamic AIS data
bench_files = [
    {'file': '[P1] AIS Data/nari_dynamic.csv', 'scale': True },
    # {'file': '[C1] Ports of Brittany/port.shp', 'scale': False },
    # {'file': '[C4] FAO Maritime Areas/FAO_AREAS.shp', 'scale': False },
    # {'file': '[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp', # 'scale': False },
    # {'file': '[C1] World Port Index/WPI.shp', 'scale': False }
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

def scaledFileExists(pathfile):
    if os.path.isfile(pathfile):
        logger.info(f'Scaled file {pathfile} already exists!')
        return True
    return False
        
def makeDirOfFileIfNotExists(pathfile):
    directory = os.path.dirname(pathfile)
    if not os.path.exists(directory):
        logger.info(f'Creating directory {directory}')
        os.makedirs(directory)

def main():
    # for every file to be scaled
    for file in bench_files:
       
        src_lines = None
        # if the file is to be scaled read it ONCE 
        if file['scale']:
            # read the lines of the input once 
            with open(f"{args.data}/{file['file']}", 'r') as src_file:
                src_lines = src_file.readlines()
            if len(src_lines) == 0:
                raise IOError("Source file {file['file']} seems to be empty!")
            logger.info(f"Going to scale {file['file']}")

        # for every scale factor that we want to target no matter if we
        # really scale the file or not
        for sf in args.scale:
            
            sf_tag = f'SF_{sf}' if file['scale'] else 'NOT_SCALED'
            scaled_filename = f"{args.target}/{file['file'][:-4]}_{sf_tag}.csv"
           
            # if the target file exists do nothing and continue
            if scaledFileExists(scaled_filename):
                continue
            # if we create the target file check if the base dir exists
            makeDirOfFileIfNotExists(scaled_filename)
      
            if file['scale']:
                scaleCSV(src_lines, scaled_filename, sf)
            else:
                # copy to the target unscaled
                shutil.copy2(f"{args.data}/{file['file']}", scaled_filename)


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    # TODO: do we really need a stream handler?
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    main()


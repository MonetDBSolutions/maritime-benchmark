#!/usr/bin/env python3

import argparse
import logging
import os

intro = """
        This script reads a single SQL file and splits its individual
        queries into separate files named 'q01.sql', 'q02.sql' etc.
        """
    
parser = argparse.ArgumentParser(description=intro)
parser.add_argument('--file', type=str, required=True,
                    help='Path to the queries file')
parser.add_argument('--target', type=str, default=os.getcwd()+'/gen/queries',
                    help='Path to the directory where the split '
                    'queries will be placed')
parser.add_argument('--print-queries', dest='print', action='store_true',
                    help='Print the queries to the log')

args = parser.parse_args()
logger = logging.getLogger(__name__)


def main():
    # check if the target exists
    if not os.path.exists(args.target):
        logger.info(f'Creates {args.target} directory')
        os.makedirs(args.target)

    # read from the file with all the queries
    with open(args.file, 'r') as queries_file:
        # split the queries on ';' and exclude the empty last one
        queries = queries_file.read().split(';')[:-1]
        # save every query to a new file
        for i in range(len(queries)):
            if args.print: 
                logger.info(f'Writting q{i+1}.sql\n{queries[i]}\n')
            with open(f'{args.target}/q{i+1}.sql', 'w+') as qfile:
                qfile.write(queries[i])
        

if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    # TODO: do we really need a stream handler?
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    main()

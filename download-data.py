#!/usr/bin/env python3

import os
import pathlib
import time
import urllib
import zipfile
import requests
import argparse
   
intro = """
        This script downloads and unzips the full Zenodo Maritme dataset
        (record id: 1167595) from the web. If you are interested in the
        Monet maritime benchmark 
        """

parser = argparse.ArgumentParser(description=intro)
parser.add_argument('--benchmark-set-only', action='store_true', dest='bench_only',
                    help='Load only the datasets needed for MonetDB geom module. '
                         'By default all Maritime data are loaded')
parser.add_argument('--target', required=False, default='data',
                    help='Path to the directory where the data will be'
                         'placed (default \'data\')')

args = parser.parse_args() 

# 'bench' key describes if the dataset is required by the Geom benchmark
sources = [
    {'url': '%5BC1%5D%20Ports%20of%20Brittany.zip', 'bench': False},
    {'url': '%5BC1%5D%20SeaDataNet%20Port%20Index.zip', 'bench': False},
    {'url': '%5BC2%5D%20European%20Coastline.zip', 'bench': False},
    {'url': '%5BC1%5D%20World%20Port%20Index.zip', 'bench': False},
    {'url': '%5BC2%5D%20European%20Maritime%20Boundaries.zip', 'bench': False},
    {'url': '%5BC2%5D%20IHO%20World%20Seas.zip', 'bench': False},
    {'url': '%5BC2%5D%20World%20EEZ.zip', 'bench': False},
    {'url': '%5BC4%5D%20FAO%20Maritime%20Areas.zip', 'bench': False},
    {'url': '%5BC4%5D%20Fishing%20Areas%20%28European%20commission%29.zip', 'bench': False},
    {'url': '%5BC5%5D%20Fishing%20Constraints.zip', 'bench': False},
    {'url': '%5BC5%5D%20Marine%20Protected%20Areas%20%28EEA%20Natura%202000%29.zip', 'bench': False},
    {'url': '%5BC6%5D%20ANFR%20Vessel%20List.zip', 'bench': False},
    {'url': '%5BC6%5D%20EU%20Fishing%20Vessels.zip', 'bench': False},
    {'url': '%5BE1%5D%20Ocean%20Conditions.zip', 'bench': False},
    {'url': '%5BE2%5D%20Weather%20Conditions.zip', 'bench': False},
    {'url': '%5BP1%5D%20AIS%20Data.zip', 'bench': True},
    {'url': '%5BP1%5D%20AIS%20Status%2C%20Codes%20and%20Types.zip', 'bench': False},
    {'url': '%5BP1%5D%20Brest%20Receiver.zip', 'bench': False},
    {'url': '%5BQ1%5D%20Integration%20Queries.zip', 'bench': False}
]

def main():
    pwd = os.getcwd()

    datadir = os.path.abspath(args.target)

    if os.path.exists(datadir):
        print(f"ERROR: {datadir} already exists")
        exit(1)
    
    os.makedirs(datadir)
    try: 
        download_data(datadir)
    except:
        os.rmdir(datadir)
        raise
    
    print("data are downloaded")

def download_data(datadir):
    HOME = "https://zenodo.org/record/1167595/files/"
    links = sources if not args.bench_only else [
            s['url'] for s in filter(lambda s: s['bench'], sources)]

    for link in links:
        url = f"{HOME}{link}?download=1"
        print(f'downloading: {url}')

        # retry 5 times
        for i in range(0, 5):
            try:
                r = requests.get(url, stream=True, allow_redirects=True)
                break
            except:
                # if a conn error occurs, sleep for 20 seconds then
                print("Downloading threw connection error, retrying in 20 seconds...")
                time.sleep(20)
                continue

        sz = int(r.headers.get('content-length', 0))
        print(f"file size: {round(sz/1024,1)} kiB")

        filename = urllib.parse.unquote(link)
        _zipLoc = f"{datadir}/{filename}"
        print(f"writing to {_zipLoc}")
        with open(_zipLoc, 'wb') as _zip:
            for chunk in r.iter_content(chunk_size=None):
                _zip.write(chunk)
        unzip_and_rename(_zipLoc, datadir, filename)
        os.remove(_zipLoc)

def unzip_and_rename(z, d, f):
    f = f.split('.')[0].strip()
    with zipfile.ZipFile(z, 'r') as zip_ref:
        target = f"{d}/{f}"
        print("creating and extracting to:", target)
        os.mkdir(target)
        zip_ref.extractall(target)


if __name__ == '__main__':
    main()

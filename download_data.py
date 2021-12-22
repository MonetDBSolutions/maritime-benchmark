#!/usr/bin/env python3

import os
import shutil
import pathlib
import time
import urllib
import zipfile
import requests

def main():
    pwd = os.getcwd()

    datadir = os.path.join(pwd + "/data")

    if os.path.exists(datadir):
        print(f"ERROR: {datadir} already exists")
        exit(1)
    
    os.mkdir(datadir)
    download_data(datadir)
    print("data are downloaded")

def download_data(datadir):
    HOME = "https://zenodo.org/record/1167595/files/"
    links = ['%5BC1%5D%20Ports%20of%20Brittany.zip', '%5BC1%5D%20SeaDataNet%20Port%20Index.zip', "%5BC2%5D%20European%20Coastline.zip",
        '%5BC1%5D%20World%20Port%20Index.zip', '%5BC2%5D%20European%20Maritime%20Boundaries.zip',
        '%5BC2%5D%20IHO%20World%20Seas.zip', '%5BC2%5D%20World%20EEZ.zip', '%5BC4%5D%20FAO%20Maritime%20Areas.zip',
        '%5BC4%5D%20Fishing%20Areas%20%28European%20commission%29.zip', '%5BC5%5D%20Fishing%20Constraints.zip',
        '%5BC5%5D%20Marine%20Protected%20Areas%20%28EEA%20Natura%202000%29.zip', '%5BC6%5D%20ANFR%20Vessel%20List.zip',
        '%5BC6%5D%20EU%20Fishing%20Vessels.zip', '%5BE1%5D%20Ocean%20Conditions.zip', '%5BE2%5D%20Weather%20Conditions.zip',
        '%5BP1%5D%20AIS%20Data.zip', '%5BP1%5D%20AIS%20Status%2C%20Codes%20and%20Types.zip', '%5BP1%5D%20Brest%20Receiver.zip',
        '%5BQ1%5D%20Integration%20Queries.zip'
    ]

    for link in links:
        url = f"{HOME}{link}?download=1"
        print(f'downloading: {url}')

        for i in range(0, 5):
            try:
                r = requests.get(url, allow_redirects=True)
                break
            except:
                # if a conn error occurs,
                # sleep for 20 seconds then retry.
                # max retries is 5 times.
                print("Downloading threw connection error, retrying in 20 seconds...")
                time.sleep(20)
                continue

        filename = urllib.parse.unquote(link)
        _zipLoc = f"{datadir}/{filename}"
        print(f"writing to {_zipLoc}")
        _zip = open(_zipLoc, 'wb')
        _zip.write(r.content)
        unzip_and_rename(_zipLoc, datadir, filename)
        _zip.close()
        os.remove(_zipLoc)

def unzip_and_rename(z, d, f):
    f = f.split('.')[0].strip()
    with zipfile.ZipFile(z, 'r') as zip_ref:
        print(f"creating and extracting to: {d}/{f}")
        os.mkdir(f"{d}/{f}")
        zip_ref.extractall(f"{dir}/{f}")


if __name__ == '__main__':
    main()

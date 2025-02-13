# Last ditch dataset creation scenario to use 
# open_mfdataset as PGF stalling and irregular chunks limit virtualization 

import numpy as np
import xarray as xr
from distributed import Client 
import fsspec 
import os
import s3fs 

access_key_id = os.environ['access_key_id']
secret_access_key = os.environ['secret_access_key']

# ran on large instance
client = Client()
print(client)


# NOTE: data has already been moved from zenodo to leap OSN
input_urls = [
    f'https://nyu1.osn.mghpcc.org/leap-pangeo-manual/CASM/CASM_SM_{year}.nc'
    for year in np.arange(2002, 2020 + 1)
]


opened_files = [fsspec.open(url).open() for url in input_urls]
cds = xr.open_mfdataset(paths=opened_files, parallel=True)

endpoint_url = "https://nyu1.osn.mghpcc.org"
fs = s3fs.S3FileSystem(
       key= access_key_id,secret=secret_access_key,client_kwargs={'endpoint_url': endpoint_url}
)

cds.chunk({'date':200,'lat':200, 'lon':300}).rename({'date':'time'}).to_zarr(fs.get_mapper('leap-pangeo-pipeline/CASM/casm.zarr'))
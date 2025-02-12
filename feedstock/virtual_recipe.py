import dask
import fsspec
import numpy as np
import xarray as xr
from virtualizarr import open_virtual_dataset

# data has already been moved from zenodo to leap OSN
input_urls = [
    f'https://nyu1.osn.mghpcc.org/leap-pangeo-manual/CASM/CASM_SM_{year}.nc'
    for year in np.arange(2002, 2020 + 1)
]

fs = fsspec.filesystem('https')
ds = xr.open_dataset(fs.open(input_urls[0]))


@dask.delayed
def open_url(input_url: str) -> xr.Dataset:
    vds = open_virtual_dataset(input_url)
    vds = vds.rename({'date': 'time'})
    return vds


delayed_list = [open_url(input_url) for input_url in input_urls]

futures = dask.persist(*delayed_list)

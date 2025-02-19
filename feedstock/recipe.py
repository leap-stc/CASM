"""
CASM recipe
"""

# zenodo data link: https://zenodo.org/records/7072512#.Y01cQi-B2-u
# data managment issue: https://github.com/leap-stc/data-management/issues/188

from dataclasses import dataclass
import os 
import apache_beam as beam
import numpy as np
import xarray as xr
import s3fs 
import obstore as obs
from obstore.fsspec import AsyncFsspecStore
from obstore.store import from_url, S3Store
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    StoreToZarr,
)
from pangeo_forge_recipes.storage import FSSpecTarget


input_urls = [
    f'https://nyu1.osn.mghpcc.org/leap-pangeo-manual/CASM/CASM_SM_{year}.nc'
    for year in np.arange(2002, 2020 + 1)
]
pattern = pattern_from_file_sequence(input_urls, concat_dim='time')

# access_key_id = os.environ['access_key_id']
# secret_access_key = os.environ['secret_access_key']
read_store = S3Store(
    "leap-pangeo-manual/CASM",
    aws_endpoint="https://nyu1.osn.mghpcc.org",
    access_key_id="",
    secret_access_key="",
)
read_fss = AsyncFsspecStore(read_store)



from dataclasses import dataclass
@dataclass
class OpenXarrayObsStore(beam.PTransform):
    read_fss: AsyncFsspecStore
    def _obs(self, url: str) -> xr.Dataset:
        _, suffix = url.rsplit('/',1)
        return xr.open_dataset(self.read_fss.open(suffix), engine='h5netcdf').rename({'date': 'time'})

    def expand(self, pcoll):
        return pcoll | 'obs' >> beam.MapTuple(lambda k, v: (k, self._obs(v)))




fs = s3fs.S3FileSystem(
       key= "",
       secret = "",
       client_kwargs={"endpoint_url":"https://nyu1.osn.mghpcc.org"}
)
target_root = FSSpecTarget(fs, 'leap-pangeo-pipeline/CASM/obstore')

with beam.Pipeline() as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenXarrayObsStore(read_fss)
        | StoreToZarr(
            target_root=target_root,
            store_name='casm.zarr',
            combine_dims=pattern.combine_dim_keys
        )
        | ConsolidateDimensionCoordinates()
        | ConsolidateMetadata()
        )


# casm = (
#     beam.Create(pattern.items())
#     | OpenXarrayObsStore(read_fss)
#     | StoreToZarr(
#         store_name='casm.zarr',
#         combine_dims=pattern.combine_dim_keys,
#         target_chunks={'time':200,'lat':200, 'lon':300},
#     )
#     # | ConsolidateDimensionCoordinates()
#     # | ConsolidateMetadata()
# )

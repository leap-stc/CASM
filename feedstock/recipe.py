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

import obstore as obs
from obstore.fsspec import AsyncFsspecStore
from obstore.store import from_url, S3Store
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    StoreToZarr,
)


input_urls = [
    f'https://nyu1.osn.mghpcc.org/leap-pangeo-manual/CASM/CASM_SM_{year}.nc'
    for year in np.arange(2002, 2020 + 1)
]
pattern = pattern_from_file_sequence(input_urls, concat_dim='time')

access_key_id = os.environ['access_key_id']
secret_access_key = os.environ['secret_access_key']
read_store = S3Store(
    "leap-pangeo-manual/CASM",
    aws_endpoint="https://nyu1.osn.mghpcc.org",
    access_key_id=access_key_id,
    secret_access_key=secret_access_key,
)
read_fss = AsyncFsspecStore(read_store)


@dataclass
class RenameDate(beam.PTransform):
    def _rename_date(self, ds: xr.Dataset) -> xr.Dataset:
        ds = ds.rename({'date': 'time'})
        return ds

    def expand(self, pcoll):
        return pcoll | '_rename_date' >> beam.MapTuple(lambda k, v: (k, self._rename_date(v)))



from dataclasses import dataclass
@dataclass
class OpenXarrayObsStore(beam.PTransform):
    read_fss: AsyncFsspecStore
    def _obs(self, url: str) -> xr.Dataset:
        _, suffix = url.rsplit('/',1)
        return xr.open_dataset(self.read_fss.open(suffix), engine='h5netcdf')

    def expand(self, pcoll):
        return pcoll | 'obs' >> beam.MapTuple(lambda k, v: (k, self._obs(v)))
    

pattern = pattern.prune()
with beam.Pipeline() as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenXarrayObsStore(read_fss)
        | RenameDate()
        | StoreToZarr(
            target_root='.',
            store_name='casm.zarr',
            combine_dims=pattern.combine_dim_keys
        )
        | ConsolidateDimensionCoordinates()
        | ConsolidateMetadata()
        )


# casm = (
#     beam.Create(pattern.items())
#     | beam.Create(pattern.items())
    # | OpenXarrayObsStore(read_fss)
#     | RenameDate()
#     | StoreToZarr(
#         store_name='casm.zarr',
#         combine_dims=pattern.combine_dim_keys,
#         target_chunks={
#             '#todo!
#         },
#     )
#     | ConsolidateDimensionCoordinates()
#     | ConsolidateMetadata()
# )

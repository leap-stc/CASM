"""
CASM recipe
"""

# zenodo data link: https://zenodo.org/records/7072512#.Y01cQi-B2-u
# data managment issue: https://github.com/leap-stc/data-management/issues/188

from dataclasses import dataclass

import apache_beam as beam
import numpy as np
import xarray as xr

# from leap_data_management_utils.data_management_transforms import InjectAttrs
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)

# from pangeo_forge_recipes.storage import FSSpecTarget


# input netcdf download links
input_urls = [
    f'https://zenodo.org/records/7072512/files/CASM_SM_{year}.nc'
    for year in np.arange(2002, 2020 + 1)
]


@dataclass
class RenameDate(beam.PTransform):
    def _rename_date(self, ds: xr.Dataset) -> xr.Dataset:
        ds = ds.rename({'date': 'time'})
        return ds

    def expand(self, pcoll):
        return pcoll | '_rename_date' >> beam.MapTuple(lambda k, v: (k, self._rename_date(v)))


pattern = pattern_from_file_sequence(input_urls, concat_dim='time')

# for local testing

# import fsspec
# fs = fsspec.get_filesystem_class("file")()
# target_root = FSSpecTarget(fs, 'casm')
# # pattern = pattern.prune(2)
# with beam.Pipeline() as p:
#     (
#         p
#         | beam.Create(pattern.items())
#         | OpenURLWithFSSpec()
#         | OpenWithXarray()
#         | RenameDate()
#         | StoreToZarr(
#             store_name='casm.zarr',
#             combine_dims=pattern.combine_dim_keys
#         )
#         )

casm = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray()
    | RenameDate()
    | StoreToZarr(
        store_name='casm1.zarr',
        combine_dims=pattern.combine_dim_keys,
        # target_chunks={
        #     '#todo!
        # },
    )
    # | InjectAttrs()
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
)

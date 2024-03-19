# %% imports
import netCDF4 as nc
import numpy as np
import itertools
import json
from tqdm import tqdm
from gwfvis_db import Dimension, Location, Options, Value, Variable, generate_gwfvis_db, Info

# %% configs
# TODO modify the configs if needed
db_path = 'output/permafrost.gwfvisdb'
nc_file_path = 'data/permafrost/Tmin_max_spinning.nc'
replace_invalid_values_with_null = True

# %% getting ready
dataset = nc.Dataset(nc_file_path)
lats = dataset.variables['lat'][:]
lons = dataset.variables['lon'][:]

lat_count_reduced_by = 10
lon_count_reduced_by = 10
lat_count = len(lats) // lat_count_reduced_by
lon_count = len(lons) // lon_count_reduced_by

# %% info
id = 0
matrix = []
for lat in range(lat_count):
    row = []
    for lon in range(lon_count):
        row.append(id)
        id = id + 1
    matrix.append(row)
matrix_info = {
    "minLatitude": lats.min() + (lats.max() - lats.min()) / lat_count / 2,
    "maxLatitude": lats.max() - (lats.max() - lats.min()) / lat_count / 2,
    "minLongitude": lons.min() + (lons.max() - lons.min()) / lon_count / 2,
    "maxLongitude": lons.max() - (lons.max() - lons.min()) / lon_count / 2,
    "idMatrix": matrix
}
json_string = json.dumps(matrix_info)

info = [
    Info(key='name', value='permafrost', label='Name'),
    Info(key='description', value='something...', label='Description'),
    Info(key='location_matrix', value=json_string)
]

# %% locations
id = 0
locations = []
for lat in range(lat_count):
    for lon in range(lon_count):
        geometry = {
            'type': 'Point',
            'coordinates': [
                matrix_info['minLongitude'] + (matrix_info['maxLongitude'] - matrix_info['minLongitude']) / lon_count * lon,
                matrix_info['minLatitude'] + (matrix_info['maxLatitude'] - matrix_info['minLatitude']) /lat_count * lat
            ]
        }
        location = Location(id=id, geometry=geometry)
        locations.append(location)
        id = id + 1

# %% dimensions
dimension_cycle = Dimension(
    id=0, name='cycle', size=dataset.dimensions['cycle'].size)
dimension_gru = Dimension(
    id=1, name='gru', size=dataset.dimensions['gru'].size)
dimension_level = Dimension(
    id=2, name='level', size=dataset.dimensions['level'].size)
dimension_time = Dimension(
    id=3, name='time', size=dataset.dimensions['time'].size)
dimensions = [
    dimension_cycle,
    dimension_gru,
    dimension_level,
    dimension_time
]

# %% variables
variable_TSOL_MIN = Variable(id=0, name='TSOL_MIN', dimensions=dimensions)
variable_TSOL_MAX = Variable(id=1, name='TSOL_MAX', dimensions=dimensions)
variables = [
    variable_TSOL_MIN,
    variable_TSOL_MAX
]

# %% values
def values_generator():
    for location in tqdm(locations):
        for variable in variables:
            dimension_value_ranges = []
            for dimension in variable.dimensions:
                dimension_value_ranges.append(range(dimension.size))
            dimension_indices_combinations = itertools.product(
                *dimension_value_ranges)
            dimension_dicts = list(map(lambda dimension_indices_combination: dict(map(lambda index_and_dimension: (
                index_and_dimension[1], dimension_indices_combination[index_and_dimension[0]]), enumerate(dimensions))), dimension_indices_combinations))
            for dimension_dict in dimension_dicts:
                dimension_cycle_index = dimension_dict[dimension_cycle]
                dimension_gru_index = dimension_dict[dimension_gru]
                dimension_level_index = dimension_dict[dimension_level]
                dimension_time_index = dimension_dict[dimension_time]
                lat_index = int(location.id / lon_count)
                lon_index = location.id % lon_count
                lat_index_range = [
                    max(round(lat_index * lat_count_reduced_by - lat_count_reduced_by / 2), 0),
                    min(round(lat_index * lat_count_reduced_by + lat_count_reduced_by / 2), len(lats))
                ]
                lon_index_range = [
                    max(round(lon_index * lon_count_reduced_by - lon_count_reduced_by / 2), 0),
                    min(round(lon_index * lon_count_reduced_by + lon_count_reduced_by / 2), len(lons))
                ]
                value = float(np.array(dataset.variables[variable.name][
                    dimension_cycle_index,
                    dimension_gru_index, 
                    dimension_level_index, 
                    dimension_time_index, 
                    lat_index_range[0]:lat_index_range[1], 
                    lon_index_range[0]:lon_index_range[1]
                ]).mean())
                if replace_invalid_values_with_null and value < 0:
                    value = None
                yield Value(location=location, variable=variable,
                            dimension_dict=dimension_dict, value=value)
values = values_generator()

# %% main function
generate_gwfvis_db(db_path, Options(
    info=info,
    locations=locations,
    dimensions=dimensions,
    variables=variables,
    values=values
))

# %% finished

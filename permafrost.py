# %% imports
import netCDF4 as nc
import numpy as np
import itertools
import json
from gwfvis_db import Dimension, Location, Options, Value, Variable, generate_gwfvis_db, Info

# %% configs
# TODO modify the configs if needed
db_path = 'output/permafrost.gwfvisdb'
nc_file_path = 'data/permafrost/Tmin_max_spinning.nc'

# %% getting ready
dataset = nc.Dataset(nc_file_path)
lats = dataset.variables['lat'][:]
lons = dataset.variables['lon'][:]

# %% info
id = 0
matrix = []
for lat in lats:
    row = []
    for lon in lons:
        row.append(id)
        id = id + 1
    matrix.append(row)
matrix_info = {
    "minLatitude": lats.min(),
    "maxLatitude": lats.max(),
    "minLongitude": lons.min(),
    "maxLongitude": lons.max(),
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
for lat in lats:
    for lon in lons:
        geometry = {
            'type': 'Point',
            'coordinates': [lon, lat]
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

# %% dimensions (REDUCED)
# TODO uncomment following code to use reduced dimension sizes
# dimension_cycle = Dimension(
#     id=0, name='cycle', size=2)
# dimension_gru = Dimension(
#     id=1, name='gru', size=2)
# dimension_level = Dimension(
#     id=2, name='level', size=3)
# dimension_time = Dimension(
#     id=3, name='time', size=1)
# dimensions = [
#     dimension_cycle,
#     dimension_gru,
#     dimension_level,
#     dimension_time
# ]
# dataset.variables['TSOL_MIN'] = dataset.variables['TSOL_MIN'][7:9, 7:9, 2:5]
# dataset.variables['TSOL_MAX'] = dataset.variables['TSOL_MAX'][7:9, 7:9, 2:5]

# %% variables
variable_TSOL_MIN = Variable(id=0, name='TSOL_MIN', dimensions=dimensions)
variable_TSOL_MAX = Variable(id=1, name='TSOL_MAX', dimensions=dimensions)
variables = [
    variable_TSOL_MIN,
    variable_TSOL_MAX
]

# %% values
values = []
lons_size = len(lons)
for location in locations:
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
            lat_index = int(location.id / lons_size)
            lon_index = location.id % lons_size
            value = float(np.array(dataset.variables[variable.name][dimension_cycle_index,
                          dimension_gru_index, dimension_level_index, dimension_time_index, lat_index, lon_index]))
            values.append(Value(location=location, variable=variable,
                          dimension_dict=dimension_dict, value=value))

# %% main function
generate_gwfvis_db(db_path, Options(
    info=info,
    locations=locations,
    dimensions=dimensions,
    variables=variables,
    values=values
))

# %% finished

# %% imports
from typing import Sequence
import shapefile
import netCDF4 as nc
from gwfvis_db import Dimension, Location, Options, Value, Variable, generate_gwfvis_db, Info

# %% configs
# TODO modify the configs if needed
db_path = 'output/mesh.gwfvisdb'
nc_db_file_path = 'data/mesh/Drainage_database/BowBanff_MESH_drainage_database.nc'
shape_file_path = 'data/mesh/Shape/bow_distributed.shp'

layers = [1, 2, 3]
nc_file_base_path = 'data/mesh/MESH_state'
nc_file_path_and_variable_name_pairs = [
    [f'{nc_file_base_path}/STGW_M_GRD.nc', 'STGW',
        'Total water storage [mm]', 'mm', False],
    [f'{nc_file_base_path}/SNO_M_GRD.nc', 'SNO',
        'Liquid water content of the snow [mm]', 'mm', False],
    [f'{nc_file_base_path}/LQWSSNO_M_GRD.nc', 'LQWSSNO',
        'Liquid water content of the snow [mm]', 'mm', False],
    [f'{nc_file_base_path}/LQWSPND_M_GRD.nc', 'LQWSPND',
        'Liquid water storage of ponded water [mm]', 'mm', False],
    [f'{nc_file_base_path}/LQWSSOL_M_IG$$layer$$_GRD.nc', 'LQWSSOL',
        'Liquid water storage in the soil [mm]', 'mm', True],
    [f'{nc_file_base_path}/FZWSSOL_M_IG$$layer$$_GRD.nc', 'FZWSSOL',
        'Frozen water storage in the soil [mm]', 'mm', True],
    [f'{nc_file_base_path}/LQWSCAN_M_GRD.nc', 'LQWSCAN',
        'Liquid water interception in the canopy [mm]', 'mm', False],
    [f'{nc_file_base_path}/FZWSCAN_M_GRD.nc', 'FZWSCAN',
        'Frozen water interception in the canopy [mm]', 'mm', False],
    [f'{nc_file_base_path}/STGGW_M_GRD.nc', 'STGGW',
        'Groundwater zone storage [mm]', 'mm', False],
    [f'{nc_file_base_path}/RFF_M_GRD.nc',
        'RFF', 'Total runoff [mm]', 'mm', False],
]

# %% getting ready
nc_file_path = nc_file_path_and_variable_name_pairs[0][0]

reader = shapefile.Reader(shape_file_path)
nc_db = nc.Dataset(nc_db_file_path)
dataset = nc.Dataset(nc_file_path)

# %% info
info = [
    Info(key='name', value='mesh', label='Name'),
    Info(key='description', value='something...', label='Description')
]

# %% locations
ids = list(nc_db.variables['seg_id'][:])
locations: Sequence[Location] = []
for shape_record in reader.shapeRecords():
    id = int(shape_record.record['COMID'])
    geometry = shape_record.shape.__geo_interface__
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    metadata = dict(zip(field_names, shape_record.record))
    locations.append(
        Location(id=id, geometry=geometry, metadata=metadata)
    )

# %% dimensions
time_size = dataset.dimensions['time'].size
layer_size = len(layers)
dimension_time = Dimension(id=0, name='time', size=time_size)
dimension_layer = Dimension(id=1, name='layer', size=layer_size,
                            description=None, value_labels=['1', '2', '3'])
dimensions = [
    dimension_time,
    dimension_layer
]

# %% variables
variables: Sequence[Variable] = []
variable_and_nc_file_path_dict = {}
for i in range(len(nc_file_path_and_variable_name_pairs)):
    [nc_file_path, variable_name, description, unit,
        has_layers] = nc_file_path_and_variable_name_pairs[i]
    dimensions_for_the_variable = [dimension_time]
    if has_layers:
        dimensions_for_the_variable.append(dimension_layer)
    variable = Variable(
        id=i,
        name=variable_name,
        dimensions=dimensions_for_the_variable,
        unit=unit,
        description=description
    )
    variables.append(variable)
    variable_and_nc_file_path_dict[variable] = nc_file_path

# %% values
values: Sequence[Value] = []


def set_value(locations, variable, layer=None):
    current_variable = dataset.variables[variable.name]
    for time in range(dimension_time.size):
        for location in locations:
            location_index = ids.index(location.id)
            dimension_dict = {dimension_time: time}
            if layer is not None:
                dimension_dict[dimension_layer] = layer
            values.append(Value(
                location=location,
                variable=variable,
                value=float(current_variable[time][location_index][0]),
                dimension_dict=dimension_dict
            ))


for variable in variables:
    nc_file_path = variable_and_nc_file_path_dict[variable]
    if len(list(filter(lambda d: d.name == 'layer', variable.dimensions))) > 0:
        for layer in layers:
            dataset = nc.Dataset(nc_file_path.replace('$$layer$$', str(layer)))
            set_value(locations, variable, layer=layer - 1)
    else:
        dataset = nc.Dataset(nc_file_path)
        set_value(locations, variable)

# %% main function
generate_gwfvis_db(db_path, Options(
    info=info,
    locations=locations,
    dimensions=dimensions,
    variables=variables,
    values=values
))

# %% finished

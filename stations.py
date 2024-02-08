#%% imports
import csv
from typing import Sequence

from gwfvis_db import Dimension, Info, Location, Options, Value, Variable, generate_gwfvis_db

#%% configs 
# TODO modify the configs if needed
db_path = 'output/stations.gwfvisdb'
csv_path = 'data/stations/station_metadata_real_time.csv'

# %% getting ready
stations: Sequence[dict] = []
with open(csv_path, 'r', encoding='utf-8-sig') as csv_file:
    reader = csv.DictReader(csv_file)
    for station in reader:
        stations.append(station)

#%% info
info = [
    Info(key='name', value='Weather Stations', label='Name'),
    Info(key='source', value='https://wateroffice.ec.gc.ca/map/index_e.html?type=real_time', label='Data Source')
]

#%% locations
locations: Sequence[Location] = []
for i, station in enumerate(stations):
    geometry = {
            'type': 'Point',
            'coordinates': [float(station['Longitude']), float(station['Latitude'])]
        }
    metadata = dict(station)
    del metadata['Most Recent Water Level (m)']
    del metadata['Most Recent Discharge (m3/s)']
    location = Location(id=i, geometry=geometry, metadata=metadata)
    locations.append(location)

#%% dimensions
default_dimension = Dimension(id=0, name="-", size=1)
dimensions = [default_dimension]

#%% variables
water_level_variable = Variable(id=0, name="Most Recent Water Level", dimensions=dimensions, unit='m')
discharge_variable = Variable(id=1, name="Most Recent Discharge", dimensions=dimensions, unit='m3/s')
variables = [
    water_level_variable,
    discharge_variable
]

#%% values
values: Sequence[Value] = []
for i, station in enumerate(stations):
    value = station['Most Recent Water Level (m)']
    if value is not None and value != '':
        values.append(Value(location=locations[i], variable=water_level_variable, value=float(value), dimension_dict={default_dimension: 0}))
    value = station['Most Recent Discharge (m3/s)']
    if value is not None and value != '':
        values.append(Value(location=locations[i], variable=discharge_variable, value=float(value), dimension_dict={default_dimension: 0}))

# %% main function
generate_gwfvis_db(db_path, Options(
    info=info,
    locations=locations,
    dimensions=dimensions,
    variables=variables,
    values=values
))

# %% finished

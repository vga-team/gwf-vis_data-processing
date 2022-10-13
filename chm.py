import netCDF4 as nc
import sqlite3
import json
import os

# region configs
# TODO modify the configs if needed
db_path = 'output/chm.gwfvisdb'
nc_file_path = 'data/chm.nc'

variable_names = [
    't'
]
# endregion

# region getting ready
try:
    os.remove(db_path)
except:
    pass

db_connection = sqlite3.connect(db_path)
db_cursor = db_connection.cursor()

dataset = nc.Dataset(nc_file_path)
# endregion

# region create info table
db_cursor.execute('''
    create table info (
        key varchar primary key,
        value text,
        label varchar
    )
''')
db_cursor.execute('''
    insert into info (key, value, label) values ('name', 'chm', 'Name')
''')
db_cursor.execute('''
    insert into info (key, value, label) values ('description', 'something...', 'Description')
''')
# endregion

# region create dimension table
db_cursor.execute('''
    create table dimension (
        id integer primary key,
        name varchar not null,
        size int not null,
        description text,
        value_labels text
    )
''')
dimension_id_and_name_pairs = []
time_size = dataset.dimensions['time'].size
db_cursor.execute('''
        insert into dimension (id, name, size, description, value_labels) values (?, ?, ?, ?, ?)
    ''', [0, 'time', time_size, None, None])
dimension_id_and_name_pairs.append([0, 'time'])
# endregion

# region create variable and variable_dimension table
db_cursor.execute('''
    create table variable (
        id integer primary key,
        name varchar not null,
        unit varchar,
        description text
    )
''')
db_cursor.execute('''
    create table variable_dimension (
        variable integer not null,
        dimension integer not null,
        foreign key (variable) references variable (id),
        foreign key (dimension) references dimension (id),
        primary key (variable, dimension)
    )
''')
variable_id_and_name_pairs = []
index = 0
for variable_name in variable_names:
    db_cursor.execute('''
        insert into variable (id, name, unit, description) values (?, ?, ?, ?)
    ''', [index, variable_name, None, None])
    db_cursor.execute('''
        insert into variable_dimension (variable, dimension) values (?, ?)
    ''', [index, 0])  # for only one dimension (time)
    variable_id_and_name_pairs.append([index, variable_name])
    index += 1
# endregion

# region create location table
db_cursor.execute('''
    create table location (
        id integer primary key,
        geometry text,
        metadata text
    )
''')

location_size = 100000  # dataset.dimensions['nMesh2_face'].size
for location_index in range(location_size):
    print(f'location {location_index + 1} of {location_size}')
    id = int(dataset.variables['global_id'][location_index])

    [node1, node2, node3] = dataset.variables['Mesh2_face_nodes'][location_index]
    node1_x = float(dataset.variables['Mesh2_node_x'][node1])
    node1_y = float(dataset.variables['Mesh2_node_y'][node1])
    node2_x = float(dataset.variables['Mesh2_node_x'][node2])
    node2_y = float(dataset.variables['Mesh2_node_y'][node2])
    node3_x = float(dataset.variables['Mesh2_node_x'][node3])
    node3_y = float(dataset.variables['Mesh2_node_y'][node3])
    geometry = {
        'type': 'Polygon',
        'coordinates': [
            [
                [node1_x, node1_y],
                [node2_x, node2_y],
                [node3_x, node3_y]
            ]
        ]
    }
    geometryJson = json.dumps(geometry, indent=2)

    metadata = {
        'global_id': id,
        'Mesh2_face_nodes': f'{int(node1)}, {int(node2)}, {int(node3)}'
    }
    metadataJson = json.dumps(metadata, indent=2)

    db_cursor.execute('''
        insert into location (id, geometry, metadata) values (?, ?, ?)
    ''', [id, geometryJson, metadataJson])
# endregion

# region create value table
new_line_character = '\n'
dimension_column_names = list(
    map(lambda x: f'dimension_{x[0]}', dimension_id_and_name_pairs))
db_cursor.execute(f'''
    create table value (
        location integer not null,
        variable integar not null,
        {f' integar,{new_line_character}'.join(dimension_column_names)},
        value float,
        foreign key (variable) references variable (id),
        primary key (location, variable, {f', '.join(dimension_column_names)})
    )
''')

for variable_id_and_name_pair in variable_id_and_name_pairs:
    # for only one dimension (time)
    current_variable = dataset.variables[variable_id_and_name_pair[1]]
    for time in range(dataset.dimensions['time'].size):
        print(f'{variable_id_and_name_pair[0]} - {time}')
        for location_index in range(location_size):
            print(
                f'values for time {time} {location_index + 1} of {location_size}')
            value = current_variable[time][location_index]
            db_cursor.execute(f'''
                insert into value (location, variable, dimension_0, value) values (?, ?, ?, ?)
            ''', [location_index, variable_id_and_name_pair[0], time, value])
# endregion

db_connection.commit()


# %% imports
from dataclasses import dataclass
import json
import os
import sqlite3
from typing import Dict, Sequence

# %% data structures


@dataclass
class Info:
    key: str
    value: str = None
    label: str = None

    def __hash__(self):
        return id(self)


@dataclass
class Location:
    id: int
    geometry: dict
    metadata: dict = None

    def __hash__(self):
        return id(self)


@dataclass
class Dimension:
    id: int
    name: str
    size: int
    description: str = None
    value_labels: Sequence[str] = None

    def __hash__(self):
        return id(self)


@dataclass
class Variable:
    id: int
    name: str
    dimensions: Sequence[Dimension]
    unit: str = None
    description: str = None

    def __hash__(self):
        return id(self)


@dataclass
class Value:
    location: Location
    variable: Variable
    value: float
    dimension_dict: Dict[Dimension, int] = None

    def __hash__(self):
        return id(self)


@dataclass
class Options:
    info: Sequence[Info]
    locations: Sequence[Location]
    dimensions: Sequence[Dimension]
    variables: Sequence[Variable]
    values: Sequence[Value]

    def __hash__(self):
        return id(self)


# %% helpers
NEW_LINE_CHARACTER = '\n'


def _open_database(path: str):
    return sqlite3.connect(path)


def _create_database(path: str):
    try:
        os.remove(path)
    except:
        pass
    return _open_database(path)


def _execute_sql(db_connection: sqlite3.Connection, sql: str, params: Sequence = None):
    db_cursor = db_connection.cursor()
    if params is None:
        db_cursor.execute(sql)
    else:
        db_cursor.execute(sql, params)


def _create_info_table(db_connection: sqlite3.Connection):
    sql = '''
        CREATE TABLE info (
            key VARCHAR PRIMARY KEY,
            value TEXT,
            label VARCHAR
        )
    '''
    _execute_sql(db_connection, sql)


def _create_location_table(db_connection: sqlite3.Connection):
    sql = '''
        CREATE TABLE location (
            id INTEGER PRIMARY KEY,
            geometry TEXT,
            metadata TEXT
        )
    '''
    _execute_sql(db_connection, sql)


def _create_dimension_table(db_connection: sqlite3.Connection):
    sql = '''
        CREATE TABLE dimension (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            size INTEGER NOT NULL,
            description TEXT,
            value_labels TEXT
        )
    '''
    _execute_sql(db_connection, sql)


def _create_variable_table(db_connection: sqlite3.Connection):
    sql = '''
        CREATE TABLE variable (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            unit VARCHAR,
            description TEXT
        )
    '''
    _execute_sql(db_connection, sql)


def _create_variable_dimension_table(db_connection: sqlite3.Connection):
    sql = '''
        CREATE TABLE variable_dimension (
            variable INTEGER NOT NULL,
            dimension INTEGER NOT NULL,
            FOREIGN KEY (variable) REFERENCES variable (id),
            FOREIGN KEY (dimension) REFERENCES dimension (id),
            PRIMARY KEY (variable, dimension)
        )
    '''
    _execute_sql(db_connection, sql)


def _create_value_table(db_connection: sqlite3.Connection, dimensions: Sequence[Dimension]):
    dimension_column_names = list(
        map(lambda dimension: f'dimension_{dimension.id}', dimensions))
    sql = f'''
        CREATE TABLE value (
            location INTEGER NOT NULL,
            variable INTEGER NOT NULL,
            {f' INTEGER, {NEW_LINE_CHARACTER}'.join(dimension_column_names)}{', ' if dimension_column_names is not None else ''}
            value FLOAT,
            FOREIGN KEY (variable) REFERENCES variable (id),
            PRIMARY KEY (location, variable{', ' if dimension_column_names is not None else ''}{', '.join(dimension_column_names)})
        )
    '''
    _execute_sql(db_connection, sql)


def _create_tables(db_connection: sqlite3.Connection, dimensions: Sequence[Dimension]):
    _create_info_table(db_connection)
    _create_location_table(db_connection)
    _create_dimension_table(db_connection)
    _create_variable_table(db_connection)
    _create_variable_dimension_table(db_connection)
    _create_value_table(db_connection, dimensions)


def _fill_info_table(db_connection: sqlite3.Connection, info: Sequence[Info]):
    sql = f'''
        INSERT INTO info (key, value, label) values (?, ?, ?)
    '''
    for inf in info:
        _execute_sql(db_connection, sql, [inf.key, inf.value, inf.label])


def _fill_location_table(db_connection: sqlite3.Connection, locations: Sequence[Location]):
    sql = f'''
        INSERT INTO location (id, geometry, metadata) values (?, ?, ?)
    '''
    for location in locations:
        _execute_sql(db_connection, sql, [
                     location.id, json.dumps(location.geometry, indent=2), json.dumps(location.metadata, indent=2)])


def _fill_dimension_table(db_connection: sqlite3.Connection, dimensions: Sequence[Dimension]):
    sql = f'''
        INSERT INTO dimension (id, name, size, description, value_labels) values (?, ?, ?, ?, ?)
    '''
    for dimension in dimensions:
        _execute_sql(db_connection, sql, [
                     dimension.id, dimension.name, dimension.size, dimension.description, json.dumps(dimension.value_labels)])


def _fill_variable_table(db_connection: sqlite3.Connection, variables: Sequence[Variable]):
    sql = f'''
        INSERT INTO variable (id, name, unit, description) values (?, ?, ?, ?)
    '''
    for variable in variables:
        _execute_sql(db_connection, sql, [
                     variable.id, variable.name, variable.unit, variable.description])


def _fill_variable_dimension_table(db_connection: sqlite3.Connection, variables: Sequence[Variable]):
    sql = f'''
        INSERT INTO variable_dimension (variable, dimension) values (?, ?)
    '''
    for variable in variables:
        if variable.dimensions is not None:
            for dimension in variable.dimensions:
                _execute_sql(db_connection, sql, [variable.id, dimension.id])


def _fill_value_table(db_connection: sqlite3.Connection, values: Sequence[Value]):
    for value in values:
        dimensions = list(value.dimension_dict.keys())
        dimension_column_names = list(
            map(lambda dimension: f'dimension_{dimension.id}', dimensions))
        sql = f'''
            INSERT INTO value (
                location, 
                variable, 
                {f', {NEW_LINE_CHARACTER}'.join(dimension_column_names)}{', ' if dimension_column_names is not None else ''}
                value
            ) values (?, ?, {''.join(map(lambda dimension: '?, ', dimension_column_names))} ?)
        '''
        params = [value.location.id, value.variable.id]
        for dimension in dimensions:
            params.append(value.dimension_dict[dimension])
        params.append(value.value)
        _execute_sql(db_connection, sql, params)


def _fill_tables(db_connection: sqlite3.Connection, options: Options):
    _fill_info_table(db_connection, options.info)
    _fill_location_table(db_connection, options.locations)
    _fill_dimension_table(db_connection, options.dimensions)
    _fill_variable_table(db_connection, options.variables)
    _fill_variable_dimension_table(db_connection, options.variables)
    _fill_value_table(db_connection, options.values)


def _query_db_table(db_connection: sqlite3.Connection, table_name: str, columns: Sequence[str] = None):
    db_cursor = db_connection.cursor()
    columns_to_query = columns
    if columns is None:
        columns_to_query = ['*']
    sql = f'''
        SELECT {', '.join(columns_to_query)} FROM {table_name}
    '''
    db_cursor.execute(sql)
    headers = list(map(lambda d: d[0], db_cursor.description))
    rows = db_cursor.fetchall()
    return list(map(lambda row: dict(zip(headers, row)), rows))


# %% exported functions

def read_gwfvis_db(path: str):
    db_connection = _open_database(path)
    query_result = _query_db_table(
        db_connection=db_connection, table_name='info')
    info = list(map(lambda d: Info(
        key=d['key'], value=d['value'], label=d['label']), query_result))

    query_result = _query_db_table(
        db_connection=db_connection, table_name='location')
    locations = list(map(lambda d: Location(id=d['id'], geometry=json.loads(
        d['geometry']), metadata=json.loads(d['metadata'])), query_result))

    query_result = _query_db_table(
        db_connection=db_connection, table_name='dimension')
    dimensions = list(map(lambda d: Dimension(id=d['id'], name=d['name'], size=d['size'],
                      description=d['description'], value_labels=json.loads(d['value_labels'])), query_result))

    query_result = _query_db_table(
        db_connection=db_connection, table_name='variable')
    variables = list(map(lambda d: Variable(
        id=d['id'], name=d['name'], unit=d['unit'], description=d['description'], dimensions=[]), query_result))

    query_result = _query_db_table(
        db_connection=db_connection, table_name='variable_dimension')
    for q in query_result:
        variable_id = q['variable']
        dimension_id = q['dimension']
        variable = next(
            filter(lambda variable: variable.id == variable_id, variables), None)
        dimension = next(
            filter(lambda dimension: dimension.id == dimension_id, dimensions), None)
        if (variable is not None and dimension is not None):
            variable.dimensions.append(dimension)

    query_result = _query_db_table(
        db_connection=db_connection, table_name='value')
    values = list(
        map(
            lambda value: Value(
                location=value['location'],
                variable=next(
                    variable for variable in variables if variable.id == value['variable']),
                value=value['value'],
                dimension_dict=dict(
                    map(
                        lambda key: (next(dimension for dimension in dimensions if dimension.id == int(
                            key[10:])), value[key]),
                        list(filter(lambda key: key.startswith(
                            'dimension_'), value.keys()))
                    )
                )
            ),
            query_result
        )
    )

    return Options(info=info, locations=locations, dimensions=dimensions, variables=variables, values=values)


def generate_gwfvis_db(path: str, options: Options):
    db_connection = _create_database(path)
    _create_tables(db_connection, options.dimensions)
    _fill_tables(db_connection, options)
    db_connection.commit()


def clone_gwfvis_db(source_path: str, destination_path: str):
    source_db_connection = _open_database(source_path)
    destination_db_connection = _create_database(destination_path)
    source_db_connection.backup(destination_db_connection)
    source_db_connection.close()
    return destination_db_connection


# %%

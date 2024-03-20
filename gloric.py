# %% imports
from typing import Sequence
import shapefile
import netCDF4 as nc
from gwfvis_db import (
    Dimension,
    Location,
    Options,
    Value,
    Variable,
    generate_gwfvis_db,
    Info,
)

# %% configs
# TODO modify the configs if needed
db_path = "output/gloric.gwfvisdb"
shape_file_path = "data/GloRiC/GloRiC_Canada_v10_shapefile/GloRiC_Canada_v10.shp"
variable_fields = [
    {
        "name": "Log_Q_avg",
        "unit": "m^3/s",
        "description": "Log-10 of long-term average discharge",
    },
    {
        "name": "Log_Q_var",
        "unit": "-",
        "description": "Log-10 of flow regime variability",
    },
    {
        "name": "Temp_av",
        "unit": "degrees Celsius",
        "description": "Long-term average of annual air temperature",
    },
    {
        "name": "Temp_rg",
        "unit": "degrees Celsius",
        "description": "Long-term range of annual air temperature",
    },
    {
        "name": "Log_spow",
        "unit": "kW/m^2",
        "description": "Log-10 of total stream power",
    },
]
bbox = [-110, 49, -105, 54]


# %% getting ready
reader = shapefile.Reader(shape_file_path)

# %% info
info = [
    Info(key="name", value="GloRiC", label="Name"),
    Info(key="description", value="something...", label="Description"),
]

# %% locations
variable_field_names = list(map(lambda x: x["name"], variable_fields))
fields = [field for field in reader.fields[1:] if field[0] not in variable_field_names]
locations_variable_data: Sequence[dict] = []
locations: Sequence[Location] = []
for shape_record in reader.shapeRecords():
    if (
        shape_record.shape.bbox[0] < bbox[0]
        or shape_record.shape.bbox[1] < bbox[1]
        or shape_record.shape.bbox[2] > bbox[2]
        or shape_record.shape.bbox[3] > bbox[3]
    ):
        continue
    id = int(shape_record.record["OBJECTID"])
    geometry = shape_record.shape.__geo_interface__
    field_names = [field[0] for field in fields]
    metadata = dict(zip(field_names, shape_record.record))
    locations.append(Location(id=id, geometry=geometry, metadata=metadata))
    locations_variable_data.append(
        {
            field: shape_record.record[field]
            for field in variable_field_names
        }
        # dict(zip([field["name"] for field in variable_fields], shape_record.record))
    )

# %% dimensions
default_dimension = Dimension(id=0, name="-", size=1)
dimensions = [default_dimension]

# %% variables
variables = [
    Variable(
        id=i,
        name=variable["name"],
        unit=variable["unit"],
        description=variable["description"],
        dimensions=dimensions,
    )
    for i, variable in enumerate(variable_fields)
]

# %% values
values: Sequence[Value] = []
for i, location in enumerate(locations):
    for variable in variables:
        value = locations_variable_data[i][variable.name]
        if value is not None:
            values.append(
                Value(
                    location=location,
                    variable=variable,
                    value=float(value),
                    dimension_dict={default_dimension: 0},
                )
            )

# %% main function
generate_gwfvis_db(
    db_path,
    Options(
        info=info,
        locations=locations,
        dimensions=dimensions,
        variables=variables,
        values=values,
    ),
)

# %% finished

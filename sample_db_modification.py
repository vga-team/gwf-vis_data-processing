# %% imports
import copy
from gwfvis_db import Variable, generate_gwfvis_db, read_gwfvis_db

# %% read the db file
options = read_gwfvis_db('./output/mesh.gwfvisdb')

# %% generate a custom variable
new_variable_name = 'new'
max_variable_id: int = max(
    map(lambda variable: variable.id, options.variables))
variable_STGW = next(
    filter(lambda variable: variable.name == 'STGW', options.variables), None)
variable_SNO = next(filter(lambda variable: variable.name ==
                    'SNO', options.variables), None)
new_variable = Variable(id=max_variable_id + 1, name=new_variable_name,
                        dimensions=variable_STGW.dimensions, unit=variable_STGW.unit)

# %% generate the values
values_for_STGW = list(
    filter(lambda value: value.variable == variable_STGW, options.values))
values_for_SNO = list(
    filter(lambda value: value.variable == variable_SNO, options.values))
new_values = []
for value_for_STGW in values_for_STGW:
    new_value = copy.copy(value_for_STGW)
    value_for_SNO = next(filter(lambda val: val.location == value_for_STGW.location and all(
        val.dimension_dict.get(k) == v for k, v in value_for_STGW.dimension_dict.items()), values_for_SNO), None)
    new_value.value = value_for_STGW.value - value_for_SNO.value
    new_value.variable = new_variable
    new_values.append(new_value)

# %% update the options
options.variables.append(new_variable)
options.values = options.values + new_values

# %% generate a new db file
generate_gwfvis_db('./output/mesh.new.gwfvisdb', options)

# %%

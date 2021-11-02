

from generate_data_json import INITIAL_DATE_STRING, ColumnInfo, Granularity, RowInfo, VariableDefinition, generateDataJSONFilesFromNetCDF, generateTimestampsForYears, obtainValuesOfPropertyFromGeoJSONFile
from generate_geo_json import BoundingBox, Filter, MetadataDefinition, generateGeoJSONFileFromShpFile
from utils import MINUTES_PER_HOUR, SECONDS_PER_MINUTE, tryRemoveDirectory

outputPath = './testdata/out'

tryRemoveDirectory(outputPath)

shpFilePath = './testdata/catchment/bow_distributed_elevation_zone.shp'
parameterNameForId = 'HRU_ID'
geoJSONOutputPath = f'{outputPath}/catchment.json'
boundingBox = BoundingBox(48.98022, 59.91098, -119.70703, -101.77735)
filter: Filter = lambda getValue: getValue('HRU_ID') > -1
metadataDefinition = MetadataDefinition('HRU_ID_', f'{outputPath}/metadata')
generateGeoJSONFileFromShpFile(
    shpFilePath=shpFilePath,
    parameterNameForId=parameterNameForId,
    boundingBox=boundingBox,
    filter=filter,
    outputPath=geoJSONOutputPath,
    metadataDefinition=metadataDefinition
)

netCDFFilePath = './testdata/SUMMA/run1_day.nc'
yearRange = range(2008, 2013)
variableDefinitions = [
    VariableDefinition('scalarSWE'),
    VariableDefinition('scalarAquiferBaseflow'),
    VariableDefinition(
        'scalarSWE * 2', lambda value: value('scalarSWE') * 2),
    VariableDefinition(
        'scalarSWE - scalarAquiferBaseflow', lambda value: value('scalarSWE') - value('scalarAquiferBaseflow'))
]
columnInfo = ColumnInfo(
    'hruId', obtainValuesOfPropertyFromGeoJSONFile(geoJSONOutputPath, 'id'))
timestampsForYears = generateTimestampsForYears(
    INITIAL_DATE_STRING, yearRange, 1 * SECONDS_PER_MINUTE * MINUTES_PER_HOUR)
rowInfo = RowInfo(
    'time',
    timestampsForYears,
    yearRange.stop - yearRange.start + 1
)
parameterNameForId = 'HRU_ID'
dataJSONOutputPath = f'{outputPath}/data'
generateDataJSONFilesFromNetCDF(
    netCDFFilePath=netCDFFilePath,
    yearRange=yearRange,
    variableDefinitions=variableDefinitions,
    columnInfo=columnInfo,
    rowInfo=rowInfo,
    parameterNameForId=parameterNameForId,
    granularities=[Granularity.DAILY],
    outputPath=dataJSONOutputPath
)

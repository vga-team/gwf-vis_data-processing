from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, List, Optional
import json
import netCDF4 as nc
import numpy as np

from utils import calculateDaysForTheYear, generateLoopingRange, logExecutionTime, writeFile

INITIAL_DATE_STRING = '1990-01-01'
SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24

TimeInSeconds = float


@dataclass
class VariableDefinition:
    name: str
    expression: Optional[Callable[[Callable[[str], np.ndarray]], np.ndarray]] = None


@dataclass
class ColumnInfo:
    name: str
    ids: list


@dataclass
class RowInfo:
    name: str
    ids: list
    # TODO to make sure what exactly this means
    iterationItemCount: int


class Granularity(Enum):
    HOURLY = 1
    DAILY = 2
    MONTHLY = 3


@dataclass
class DataItem:
    total: float
    min: float
    max: float
    average: float
    value: List[float]


@logExecutionTime('generating data JSON files')
def generateDataJSONFilesFromNetCDF(
    netCDFFilePath: str,
    yearRange: range,
    variableDefinitions: List[VariableDefinition],
    columnInfo: ColumnInfo,
    rowInfo: RowInfo,
    parameterNameForId: str,
    granularity: List[Granularity],
    outputPath: str
):
    if granularity == Granularity.DAILY:
        for variableDefinition in variableDefinitions:
            dailyData = {}
            columnIds = np.around(columnInfo.ids).astype(int)

            rawDailyData, actualColumnIndexes, actualRowIndexes = generateDailyDataFromDailyNetCDFData(
                netCDFFilePath, variableDefinition, columnInfo, rowInfo, yearRange)
            for columnIndex in actualColumnIndexes.tolist():
                for year in generateLoopingRange(yearRange):
                    dailyDataForCurrentYear = {}
                    columnValuesForCurrentYear = rawDailyData[year][:, columnIndex].tolist(
                    )

                    # TODO the days seem having problems
                    for day in range(0, 365):
                        dataForCurrentDay = DataItem(
                            round(columnValuesForCurrentYear[day], 3),
                            round(columnValuesForCurrentYear[day], 3),
                            round(columnValuesForCurrentYear[day], 3),
                            round(columnValuesForCurrentYear[day], 3),
                            [columnValuesForCurrentYear[day]]
                        )
                        dailyDataForCurrentYear[day] = asdict(
                            dataForCurrentDay)
                    dailyData[year] = dailyDataForCurrentYear
                filePath = f'{outputPath}/{variableDefinition.name}/daily/id_{str(columnIds[columnIndex])}.json'
                jsonString = json.dumps(
                    {
                        parameterNameForId: str(columnIds[columnIndex]),
                        'peroid_type': 'Daily',
                        'data': dailyData
                    },
                    indent=2
                )
                writeFile(filePath, jsonString)


def generateDailyDataFromDailyNetCDFData(
    netCDFFilePath: str,
    variableDefinition: VariableDefinition,
    columnInfo: ColumnInfo,
    rowInfo: RowInfo,
    yearRange: range
):
    dataset = nc.Dataset(netCDFFilePath)

    columnIds = np.around(columnInfo.ids).astype(int)
    columnIdsOfDataset = np.around(dataset[columnInfo.name][:]).astype(int)
    columnIndexes = np.where(np.isin(columnIdsOfDataset, columnIds))[0]

    rowIds = np.around(rowInfo.ids).astype(int)
    rowIdsFromDataset = np.around(dataset[rowInfo.name][:]).astype(int)
    rowIndexes = np.where(np.isin(rowIdsFromDataset, rowIds))[0]

    if variableDefinition.expression:
        valuesPerDay = variableDefinition.expression(
            lambda variableName: np.array(
                dataset[variableName][rowIndexes, columnIndexes])
        ).tolist()
    else:
        valuesPerDay = np.array(
            dataset[variableDefinition.name][rowIndexes, columnIndexes]).tolist()

    dailyData = {}

    # TODO seems having problems about days
    dayIndex = 0
    dayCount = 365
    for year in range(yearRange.start, yearRange.stop + 1):
        # daysForTheYear = calculateDaysForTheYear(year)
        dailyData[year] = np.around(
            valuesPerDay[dayIndex:(dayIndex+dayCount)], 3)
        dayIndex += dayCount

    return dailyData, columnIndexes, rowIndexes


def obtainValuesOfPropertyFromGeoJSONFile(
    filePath: str,
    propertyName: str
):
    propertyValues = []
    with open(filePath) as file:
        data = json.load(file)
        for item in data['features']:
            propertyValue = item['properties'][propertyName]
            propertyValues.append(propertyValue)

    return propertyValues


def generateTimestampsForYears(
    initialDateString: str,
    yearRange: range,
    timeStep: TimeInSeconds
):
    loopingYearRange = generateLoopingRange(yearRange)
    timestampsForYears = []
    for year in loopingYearRange:
        timeStempsForCurrentYear = generateTimestamps(
            initialDateString=initialDateString,
            startDateString=f'{year}-01-01',
            endDateString=f'{year + 1}-01-01',
            timeStep=timeStep
        )
        timestampsForYears.extend(timeStempsForCurrentYear)

    return timestampsForYears


def generateTimestamps(
    initialDateString: str,
    startDateString: str,
    endDateString: str,
    timeStep: TimeInSeconds
):
    initialDate = datetime.strptime(initialDateString, '%Y-%m-%d')
    startDate = datetime.strptime(startDateString, '%Y-%m-%d')
    endDate = datetime.strptime(endDateString, '%Y-%m-%d')

    timestamps = []
    date = startDate
    while date < endDate:
        timeDiff = date - initialDate
        timeDiffInSeconds = timeDiff.total_seconds()
        timestamps.append(timeDiffInSeconds)
        date += timedelta(seconds=timeStep)

    return timestamps


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
    'hruId', obtainValuesOfPropertyFromGeoJSONFile('./testdata/catchment.json', 'id'))
timestampsForYears = generateTimestampsForYears(
    INITIAL_DATE_STRING, yearRange, 1 * SECONDS_PER_MINUTE * MINUTES_PER_HOUR)
rowInfo = RowInfo(
    'time',
    timestampsForYears,
    yearRange.stop - yearRange.start + 1
)
parameterNameForId = 'HRU_ID'

generateDataJSONFilesFromNetCDF(
    netCDFFilePath,
    yearRange,
    variableDefinitions,
    columnInfo,
    rowInfo,
    parameterNameForId,
    Granularity.DAILY,
    './testdata/data'
)

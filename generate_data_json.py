from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, List, Optional
import json
import netCDF4 as nc
import numpy as np
from numpy.lib.function_base import average

from utils import calculateDaysForTheYear, generateLoopingRange, logExecutionTime, writeFile

INITIAL_DATE_STRING = '1990-01-01'

TimeInSeconds = float


@dataclass
class VariableDefinition:
    name: str
    expression: Optional[Callable[[
        Callable[[str], np.ndarray]], np.ndarray]] = None


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
    HOURLY = 'hourly'
    DAILY = 'daily'
    MONTHLY = 'monthly'


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
    granularities: List[Granularity],
    outputPath: str
):
    for variableDefinition in variableDefinitions:
        if Granularity.DAILY in granularities:
            generateDataJSONFile(netCDFFilePath, yearRange, columnInfo,
                                 rowInfo, parameterNameForId, outputPath, variableDefinition, Granularity.DAILY)
        if Granularity.MONTHLY in granularities:
            generateDataJSONFile(netCDFFilePath, yearRange, columnInfo,
                                 rowInfo, parameterNameForId, outputPath, variableDefinition, Granularity.MONTHLY)


def generateDataJSONFile(
    netCDFFilePath: str,
    yearRange: range,
    columnInfo: ColumnInfo,
    rowInfo: RowInfo,
    parameterNameForId: str,
    outputPath: str,
    variableDefinition: VariableDefinition,
    granularity: Granularity
):
    data = {}
    columnIds = np.around(columnInfo.ids).astype(int)

    rawDailyData, actualColumnIndexes, actualRowIndexes = generateDailyDataFromDailyNetCDFData(
        netCDFFilePath, variableDefinition, columnInfo, rowInfo, yearRange)
    for columnIndex in actualColumnIndexes.tolist():
        for year in generateLoopingRange(yearRange):
            dataForCurrentYear = {}
            columnValuesForCurrentYear = rawDailyData[year][:, columnIndex].tolist(
            )

            if granularity == Granularity.DAILY:
                # TODO the days can be 365 or 366
                for day in range(0, 365):
                    dataForCurrentDay = DataItem(
                        round(columnValuesForCurrentYear[day], 3),
                        round(columnValuesForCurrentYear[day], 3),
                        round(columnValuesForCurrentYear[day], 3),
                        round(columnValuesForCurrentYear[day], 3),
                        [columnValuesForCurrentYear[day]]
                    )
                    dataForCurrentYear[day] = asdict(dataForCurrentDay)
            if granularity == Granularity.MONTHLY:
                # TODO the February days can be 28 or 29
                daysOfMonths = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                for month in range(0, 12):
                    dayValues = []
                    for day in range(daysOfMonths[month]):
                        dayValue = np.around(
                            columnValuesForCurrentYear[month], 3)
                        dayValues.append(dayValue)

                    dataForCurrentMonth = DataItem(
                        round(sum(dayValues), 3),
                        round(min(dayValues), 3),
                        round(max(dayValues), 3),
                        round(average(dayValues), 3),
                        dayValues
                    )
                    dataForCurrentYear[month] = asdict(dataForCurrentMonth)

            data[year] = dataForCurrentYear
        filePath = f'{outputPath}/{variableDefinition.name}/{granularity.value}/id_{str(columnIds[columnIndex])}.json'
        jsonString = json.dumps(
            {
                parameterNameForId: str(columnIds[columnIndex]),
                'peroid_type': 'Daily',
                'data': data
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

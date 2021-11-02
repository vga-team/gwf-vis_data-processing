from dataclasses import dataclass
from typing import Callable
from utils import logExecutionTime, writeFile
import shapefile
import json


@dataclass
class BoundingBox:
    minLatitude: float
    maxLatitude: float
    minLongitude: float
    maxLongitude: float


Filter = Callable[[Callable[[str], int]], bool]


def generateGeoJSONFileFromShpFile(
    shpFilePath: str,
    parameterNameForId: str,
    outputPath: str,
    boundingBox: BoundingBox = None,
    filter: Filter = None
):
    content = generateGeoJSONStringFromShpFile(
        shpFilePath,
        parameterNameForId,
        boundingBox,
        filter
    )
    writeFile(outputPath, content)


@logExecutionTime('generating Geo-JSON string')
def generateGeoJSONStringFromShpFile(
    shpFilePath: str,
    parameterNameForId: str,
    boundingBox: BoundingBox = None,
    filter: Filter = None,
) -> str:
    reader = shapefile.Reader(shpFilePath)
    features = generateFeatures(
        parameterNameForId, boundingBox, filter, reader)
    jsonString = generateJsonString(parameterNameForId, features)

    return jsonString


def generateJsonString(parameterNameForId, features):
    jsonString = json.dumps(
        {
            # TODO might want to rename it
            'id_param_name': parameterNameForId,
            'type': 'FeatureCollection',
            'features': features
        },
        indent=2
    ) + '\n'

    return jsonString


def generateFeatures(parameterNameForId, boundingBox, filter, reader):
    features = []
    for shapeRecord in reader.shapeRecords():
        if checkIfShouldSkip(boundingBox, filter, shapeRecord):
            continue
        id = int(shapeRecord.record[parameterNameForId])
        properties = {'id': id}
        geometry = shapeRecord.shape.__geo_interface__
        features.append({
            'type': 'Feature',
            'properties': properties,
            'geometry': geometry
        })

    return features


def checkIfShouldSkip(boundingBox, filter, shapeRecord):
    return not any([
        checkInBoundary(boundingBox, shapeRecord),
        checkFilter(filter, shapeRecord)
    ])


def checkFilter(filter, shapeRecord):
    if not filter:
        return True

    def getValue(parameterName): return int(shapeRecord.record[parameterName])
    if filter(getValue):
        return True
    else:
        return False


def checkInBoundary(boundingBox, shapeRecord):
    minX, minY, maxX, maxY = shapeRecord.shape.bbox
    if not boundingBox:
        return True
    if minX < boundingBox.minLongitude or \
            maxX > boundingBox.maxLongitude or \
            minY < boundingBox.minLatitude or \
            maxY > boundingBox.maxLatitude:
        return False
    else:
        return True


# TODO remove the testing later
boundingBox = BoundingBox(48.98022, 59.91098, -119.70703, -101.77735)
filter: Filter = lambda getValue: getValue('COMID') > -1
generateGeoJSONFileFromShpFile(
    'river_network/bow_river_network_from_merit_hydro.shp', 'COMID', './test.json', boundingBox, filter)

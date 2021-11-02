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


@dataclass
class MetadataDefinition:
    fileNamePrefix: str
    directoryPath: str


@logExecutionTime('generating Geo-JSON')
def generateGeoJSONFileFromShpFile(
    shpFilePath: str,
    parameterNameForId: str,
    outputPath: str,
    boundingBox: BoundingBox = None,
    filter: Filter = None,
    metadataDefinition: MetadataDefinition = None
):
    reader = shapefile.Reader(shpFilePath)

    if(metadataDefinition):
        generateMetadataFiles(parameterNameForId, metadataDefinition, reader)

    generateGeoJSONFile(parameterNameForId, outputPath,
                        boundingBox, filter, reader)


def generateMetadataFiles(parameterNameForId, metadataDefinition, reader):
    fields = reader.fields[1:]
    fieldNames = [field[0] for field in fields]
    for shapeRecord in reader.shapeRecords():
        id = int(shapeRecord.record[parameterNameForId])
        filePath = f'{metadataDefinition.directoryPath}/{metadataDefinition.fileNamePrefix}{str(id)}.json'
        data = dict(zip(fieldNames, shapeRecord.record))
        content = json.dumps({'data': data}, indent=2)
        writeFile(filePath, content)


def generateGeoJSONFile(parameterNameForId, outputPath, boundingBox, filter, reader):
    features = generateFeatures(
        parameterNameForId, boundingBox, filter, reader)
    jsonString = json.dumps(
        {
            # TODO might want to rename it
            'id_param_name': parameterNameForId,
            'type': 'FeatureCollection',
            'features': features
        },
        indent=2
    ) + '\n'
    writeFile(outputPath, jsonString)


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

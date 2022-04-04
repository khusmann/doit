from __future__ import annotations
import typing as t
from pydantic import (
    StrictStr,
    StrictInt,
    StrictBool,
    StrictFloat,
)

from .base import *

### Source Table Types (TODO: Clean these up)

SourceColumnTypeStr = t.Literal['bool', 'ordinal', 'real', 'text', 'integer']
SourceColumnDataType = t.Union[StrictBool, StrictStr, StrictFloat, StrictInt]
SourceColumnName = t.NewType('SourceColumnName', str)

RemoteServiceName = t.Literal['qualtrics']
SourceFormatType = t.Literal['qualtrics']

### StudyTable Types
OrdinalStudyColumnTypeStr = t.Literal['ordinal', 'categorical', 'index']
SimpleStudyColumnTypeStr = t.Literal['text', 'real', 'integer', 'bool']

StudyColumnTypeStr = t.Union[
    OrdinalStudyColumnTypeStr,
    SimpleStudyColumnTypeStr,
]

### Relative Names (used by specs)

RelativeMeasureNodeName = t.NewType('RelativeMeasureNodeName', str)
RelativeIndexColumnName = t.NewType('RelativeIndexName', str)
RelativeCodeMapName = t.NewType('RelativeCodeMapName', str)

### CodeMap Values

CodeValue = t.NewType('CodeValue', int)
CodeValueTag = t.NewType('CodeValueTag', str)
RecodeTransform = t.Mapping[str, t.Optional[str]]

### Entity Ids

CodeMapId = t.NewType('CodeMapId', int)
MeasureId = t.NewType("MeasureId", int)
ColumnInfoNodeId = t.NewType('ColumnInfoId', int)
InstrumentId = t.NewType('InstrumentId', int)
InstrumentNodeId = t.NewType('InstrumentNodeId', int)
StudyTableId = t.NewType('TableId', int)

StudyEntityId = t.Union[
    CodeMapId,
    MeasureId,
    ColumnInfoNodeId,
    InstrumentId,
    InstrumentNodeId,
    StudyTableId,
]

### Entity Names

MeasureName = t.NewType("MeasureName", str)
StudyTableName = t.NewType('TableName', str)
InstrumentName = t.NewType('InstrumentName', str)
class CodeMapName(Uri): pass
class ColumnName(Uri): pass # e.g. measure.group.item

StudyEntityName = t.Union[
    MeasureName,
    InstrumentName,
    StudyTableName,
    CodeMapName,
    ColumnName,
]


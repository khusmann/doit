from __future__ import annotations
import typing as t
import hashlib
import csv
import io
from ...common import ImmutableBaseModel
from ...common.table import (
    Omitted,
    Some,
    cast_fn,
    cast_fn_seq,
    value_if_none_fn,
)
from ..model import UnsanitizedCodedColumnInfo, UnsanitizedColumnId, UnsanitizedColumnInfo, UnsanitizedSimpleColumnInfo, UnsanitizedTable, UnsanitizedTableData, UnsanitizedTableRowView

class SurveyBlock(ImmutableBaseModel):
    type: t.Literal['block']
    blockItems: t.Tuple[SurveyDataItem, ...]
    has_data: t.Literal[False] = False

### ChoiceItem

class ChoiceItemResponse(ImmutableBaseModel):
    rTx: str
    rDVal: int

class ChoiceItem(ImmutableBaseModel):
    sQuId: int
    type: t.Literal['question']
    qTy: t.Literal[0]
    qTx: str # prompt
    responses: t.Tuple[ChoiceItemResponse, ...]
    has_data: t.Literal[True] = True

### TextItem

class TextItem(ImmutableBaseModel):
    sQuId: int
    type: t.Literal['question']
    qTy: t.Literal[11]
    qTx: str # prompt
    has_data: t.Literal[True] = True

### MultiselectItem

class MultiselectResponse(ImmutableBaseModel):
    rTx: str # Label
    rDVal: int # Value of the anchor

class MultiselectItem(ImmutableBaseModel):
    sQuId: int
    type: t.Literal['question']
    qTy: t.Literal[7]
    qTx: str # prompt
    responses: t.Tuple[MultiselectResponse, ...]
    has_data: t.Literal[True] = True

### SliderItem

class SliderAnchor(ImmutableBaseModel):
    rTx: str # Label
    rDVal: int # Value of the anchor

class SliderItem(ImmutableBaseModel):
    sQuId: int
    type: t.Literal['question']
    qTy: t.Literal[3]
    qTx: str # prompt
    responses: t.Tuple[SliderAnchor, ...]
    has_data: t.Literal[True] = True

### MultisliderItem

class MultisliderResponse(ImmutableBaseModel):
    assRId: int # ID of the response
    msd: str

class MultisliderItem(ImmutableBaseModel):
    sQuId: int
    type: t.Literal['question']
    qTy: t.Literal[17]
    qTx: str # prompt
    responses: t.Tuple[MultisliderResponse, ...]
    has_data: t.Literal[True] = True

class EndBlockItem(ImmutableBaseModel):
    sQuId: int
    type: t.Literal['question']
    qTy: t.Literal[21]
    has_data: t.Literal[False] = False

SurveyQuestion = t.Union[
    MultisliderItem,
    SliderItem,
    MultiselectItem,
    ChoiceItem,
    TextItem,
]

SurveyDataItem = t.Union[
    SurveyBlock,
    SurveyQuestion,
    EndBlockItem,
]

class SurveySchema(ImmutableBaseModel):
    surveyTitle: str
    surveyDescription: str
    surveyDataItems: t.Tuple[SurveyDataItem, ...]

class WearitSchema(ImmutableBaseModel):
    days_on: int
    days_off: int
    survey: SurveySchema

SurveyBlock.update_forward_refs()

def flatten_schema_items(schema_items: t.Sequence[SurveyDataItem]) -> t.Tuple[t.Tuple[str, SurveyQuestion], ...]:
    questions = tuple(
        ("Q_{}".format(i.sQuId), i) for i in schema_items if i.has_data is True
    )

    children = tuple(
        child
            for i in schema_items if i.type == 'block'
                for child in flatten_schema_items(i.blockItems)
    )

    return questions + children

class MultiWearitColumnHelper(t.NamedTuple):
    wearit_item: MultisliderItem
    info_map: t.Mapping[int, UnsanitizedColumnInfo]

def parse_column(item_lookup: t.Mapping[str, SurveyQuestion], item_str: str) -> UnsanitizedColumnInfo | MultiWearitColumnHelper:
    if item_str in ["submit_date", "complete_date", "pid"]:
        return UnsanitizedSimpleColumnInfo(
            id=UnsanitizedColumnId(item_str),
            prompt=item_str,
            is_safe=True,
        )

    item = item_lookup.get(item_str)

    if not item:
        return UnsanitizedSimpleColumnInfo(
            id=UnsanitizedColumnId(item_str),
            prompt="Deleted question",
            is_safe=False,
        )

    match item:
        case MultisliderItem(qTx=prompt):
            return MultiWearitColumnHelper(
                wearit_item=item,
                info_map={
                    i.assRId: UnsanitizedSimpleColumnInfo(
                        id=UnsanitizedColumnId("{}_{}".format(item_str, i.assRId)),
                        prompt="{} - {}".format(prompt, i.msd),
                        is_safe=True,
                    ) for i in item.responses
                }
            )
        case SliderItem(qTx=prompt):
            return UnsanitizedSimpleColumnInfo(
                id=UnsanitizedColumnId(item_str),
                prompt=prompt,
                is_safe=True,
            )
        case MultiselectItem(qTx=prompt):
            return UnsanitizedCodedColumnInfo(
                id=UnsanitizedColumnId(item_str),
                prompt=prompt,
                value_type='multiselect',
                codes={ i.rDVal: i.rTx for i in item.responses },
            )
        case ChoiceItem(qTx=prompt):
            return UnsanitizedCodedColumnInfo(
                id=UnsanitizedColumnId(item_str),
                prompt=prompt,
                value_type='ordinal',
                codes={ i.rDVal: i.rTx for i in item.responses },
            )
        case TextItem(qTx=prompt):
            return UnsanitizedSimpleColumnInfo(
                id=UnsanitizedColumnId(item_str),
                prompt=prompt,
                is_safe=False,
            )

def parse_value(column: UnsanitizedColumnInfo | MultiWearitColumnHelper, value: str):
    if isinstance(column, MultiWearitColumnHelper):
        pairs = dict(tuple(pair.split(":")) for pair in value.split(",") if pair != "N/A")
        omitted_if_none_fn = value_if_none_fn(Omitted())
        return tuple(
            (c.id, Some(pairs.get(str(k))).bind(omitted_if_none_fn))
                for k, c in column.info_map.items()
        )

    if value == "N/A":
        return ((column.id, Omitted()),)

    match column:
        case UnsanitizedSimpleColumnInfo():
            return ((column.id, Some(value)),)
        case UnsanitizedCodedColumnInfo():
            match column.value_type:
                case 'ordinal':
                    return ((column.id, Some(value).bind(cast_fn(int))),)
                case 'multiselect':
                    return ((column.id, Some(value.split(",")).bind(cast_fn_seq(int))),)

def remove_helper(column: UnsanitizedColumnInfo | MultiWearitColumnHelper) -> t.Tuple[UnsanitizedColumnInfo, ...]:
    if isinstance(column, MultiWearitColumnHelper):
        return tuple(column.info_map.values())
    else:
        return (column,)

def load_unsanitizedtable_wearit(schema_json: str, data_csv: str) -> UnsanitizedTable:

    wearit_schema = WearitSchema.parse_raw(schema_json)

    item_lookup = dict(flatten_schema_items(wearit_schema.survey.surveyDataItems))

    reader = csv.reader(io.StringIO(data_csv, newline=''))

    header = ("submit_date", "complete_date", "pid") + tuple(next(reader))[3:]

    next(reader) # Throw out prompts

    columns = tuple(parse_column(item_lookup, i) for i in header)

    columns_flat = tuple(
        c
            for i in columns
                for c in remove_helper(i)
    )


    rows = tuple(
        UnsanitizedTableRowView(
            i 
                for c, v in zip(columns, row)
                    for i in parse_value(c, v)
        ) for row in reader
    )

    return UnsanitizedTable(
        schema=columns_flat,
        schema_checksum=hashlib.sha256(schema_json.encode()).hexdigest(),
        data_checksum=hashlib.sha256(data_csv.encode()).hexdigest(),
        source_name="wearit",
        source_title=wearit_schema.survey.surveyTitle,
        data=UnsanitizedTableData(
            column_ids=tuple(c.id for c in columns_flat),
            rows=rows
        )
    )
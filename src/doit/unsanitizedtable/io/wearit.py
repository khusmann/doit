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
    def inner(item: SurveyDataItem):
        if item.has_data is True:
            return (("Q_{}".format(item.sQuId), item), )
        if item.type == 'block':
            return flatten_schema_items(item.blockItems)
        return ()

    return tuple(
        i
            for item in schema_items
                for i in inner(item)
    )

class MultiWearitColumnHelper(t.NamedTuple):
    id: UnsanitizedColumnId
    wearit_item: MultisliderItem
    info_map: t.Mapping[int, UnsanitizedColumnInfo]

def parse_column(
    item_str: str,
    item_lookup: t.Mapping[str, SurveyQuestion],
    column_sort_order: t.Mapping[str, str],
) -> UnsanitizedColumnInfo | MultiWearitColumnHelper | None:
    if item_str in ["submit_date", "complete_date", "pid"]:
        return UnsanitizedSimpleColumnInfo(
            id=UnsanitizedColumnId(item_str),
            prompt=item_str,
            is_safe=True,
            sortkey=column_sort_order[item_str],
        )

    item = item_lookup.get(item_str)

    if not item:
        return None

    match item:
        case MultisliderItem(qTx=prompt):
            return MultiWearitColumnHelper(
                id=UnsanitizedColumnId(item_str),
                wearit_item=item,
                info_map={
                    i.assRId: UnsanitizedSimpleColumnInfo(
                        id=UnsanitizedColumnId("{}_{}".format(item_str, i.assRId)),
                        prompt="{} - {}".format(prompt, i.msd),
                        is_safe=True,
                        sortkey="{}_{}".format(column_sort_order[item_str], idx),
                    ) for idx, i in enumerate(item.responses)
                }
            )
        case SliderItem(qTx=prompt):
            return UnsanitizedSimpleColumnInfo(
                id=UnsanitizedColumnId(item_str),
                prompt=prompt,
                is_safe=True,
                sortkey=column_sort_order[item_str],
            )
        case MultiselectItem(qTx=prompt):
            return UnsanitizedCodedColumnInfo(
                id=UnsanitizedColumnId(item_str),
                prompt=prompt,
                value_type='multiselect',
                codes={ i.rDVal: i.rTx for i in item.responses },
                sortkey=column_sort_order[item_str],
            )
        case ChoiceItem(qTx=prompt):
            return UnsanitizedCodedColumnInfo(
                id=UnsanitizedColumnId(item_str),
                prompt=prompt,
                value_type='ordinal',
                codes={ i.rDVal: i.rTx for i in item.responses },
                sortkey=column_sort_order[item_str],
            )
        case TextItem(qTx=prompt):
            return UnsanitizedSimpleColumnInfo(
                id=UnsanitizedColumnId(item_str),
                prompt=prompt,
                is_safe=False,
                sortkey=column_sort_order[item_str],
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

FIRST_THREE_COLS = ("submit_date", "complete_date", "pid")

def load_unsanitizedtable_wearit(schema_json: str, data_csv: str) -> UnsanitizedTable:

    wearit_schema = WearitSchema.parse_raw(schema_json)

    item_lookup = dict(flatten_schema_items(wearit_schema.survey.surveyDataItems))
    column_sort_order = { wearit_id: str(i).zfill(6) for i, wearit_id in enumerate(FIRST_THREE_COLS + tuple(item_lookup)) }

    reader = csv.reader(io.StringIO(data_csv, newline=''))

    header = FIRST_THREE_COLS + tuple(next(reader))[3:]

    next(reader) # Throw out prompts

    columns = tuple(parse_column(i, item_lookup, column_sort_order) for i in header)

    rows = tuple(
        UnsanitizedTableRowView(
            i 
                for c, v in zip(columns, row)
                    if c is not None
                        for i in parse_value(c, v)
        ) for row in reader
    )


    columns_nonempty = tuple(c for c in columns if c is not None)

    columns_flat = tuple(
        c
            for i in sorted(columns_nonempty, key=lambda x: column_sort_order[x.id.unsafe_name])
                for c in remove_helper(i)
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
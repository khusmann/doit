from __future__ import annotations
import typing as t
from typing_extensions import Annotated
from pydantic import BaseModel, Field

# Question Types

class QuestionBoolean(BaseModel):
    type: t.Literal['boolean']
    prompt: str
    remote_id: str
    measure_id: t.Optional[str]
    true_value: str
    false_value: str

class QuestionText(BaseModel):
    type: t.Literal['text']
    prompt: str
    remote_id: str
    measure_id: t.Optional[str]

class QuestionNumber(BaseModel):
    type: t.Literal['number']
    prompt: str
    remote_id: str
    measure_id: t.Optional[str]

class QuestionChoiceItem(BaseModel):
    text: str
    level: t.Optional[str]

class QuestionChoice(BaseModel):
    type: t.Literal['choice']
    prompt: str
    remote_id: str
    measure_id: t.Optional[str]
    choices: t.List[t.Union[QuestionChoiceItem, str]]

class QuestionMultiselectItem(BaseModel):
    text: str
    measure_id: t.Optional[str]

class QuestionMultiselect(BaseModel):
    type: t.Literal['multiselect']
    prompt: str
    remote_id: str
    choices: t.List[QuestionMultiselectItem] # Verify measure_id in all or in none

Question = Annotated[t.Union[QuestionText, QuestionBoolean, QuestionNumber, QuestionChoice, QuestionMultiselect], Field(discriminator='type')]

class QuestionGroup(BaseModel):
    type: t.Literal['group']
    items: t.List[Question]

SectionItem = Annotated[t.Union[QuestionGroup, Question], Field(discriminator='type')]

# Section

class Section(BaseModel):
    section: str
    instructions: t.Optional[str]
    items: t.Optional[t.List[SectionItem]]

# ConstantItem

class ConstantItem(BaseModel):
    type: t.Literal['number', 'text', 'boolean']
    measure_id: str
    value: str

# HiddenItem

class HiddenItem(BaseModel):
    type: t.Literal['number', 'text', 'boolean']
    remote_id: str
    measure_id: str

# InstrumentConfig

class InstrumentConfig(BaseModel):
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]
    hidden: t.Optional[t.List[HiddenItem]]
    constant: t.Optional[t.List[ConstantItem]]
    content: t.Optional[t.List[Section]]
import typing as t
import pytest

from doit.study.sqlalchemy.impl import (
    SqlAlchemyRepo,
)

from doit.study.spec import StudySpec
from doit.study.io import load_studyspec_str

@pytest.fixture
def configspec():
    return {
        'title': "Test study",
        'description': "description of test study",
        'indices': {
            'year': {
                'title': 'Collection year',
                'values': [1, 2],
            },
            'wave': {
                'title': 'Collection wave',
                'values': [1, 2, 3],
            },
        }
    }

@pytest.fixture
def measurespec():
    return {
        'measure1': {
            'title': "Test measure 1",
            'description': "This is a test measure",
            'items': [
                {
                    'id': "measure_group",
                    'type': "group",
                    'items': [
                        {
                            'id': "01",
                            'prompt': "prompt question 1",
                            'type': "ordinal",
                            'codes': "test_likert",
                        },
                        {
                            'id': "02",
                            'prompt': "prompt question 2",
                            'type': "ordinal",
                            'codes': "test_likert",
                        },
                    ],
                },
            ],
            'codes': {
                'test_likert': [
                    { 'value': 1, 'tag': "NEVER", 'text': "Never" },
                    { 'value': 2, 'tag': "SELDOM", 'text': "Seldom" },
                    { 'value': 3, 'tag': "OFTEN", 'text': "Often" },
                    { 'value': 4, 'tag': "ALMOST_ALWAYS", 'text': "Almost Always" },
                ],
            },
        },
    }

@pytest.fixture
def instrumentspec():
    return {
        'instrument1': {
            'title': "Test instrument 1",
            'description': "test description",
            'prompt': "prompt",
            'items': [
                {
                    'type': "constant",
                    'id': "indices.wave",
                    'value': "1"
                },
                {
                    'type': "constant",
                    'id': "indices.year",
                    'value': "2"
                },
                {
                    'type': "group",
                    'items': [
                        {
                            'type': "question",
                            'prompt': "instrument measure1.01 prompt",
                            'id': "measure1.01"
                        },
                        {
                            'type': "question",
                            'prompt': "instrument measure1.02 prompt",
                            'id': "measure1.02"
                        },
                        {
                            'type': "question",
                            'prompt': "instrument measure1.03 prompt",
                            'id': "measure1.03"
                        },
                    ]
                }
            ]
        }
    }

@pytest.fixture
def studyspec(configspec: t.Any, measurespec: t.Any, instrumentspec: t.Any):
    return load_studyspec_str(configspec, measurespec, instrumentspec, lambda x: x)

def test_add_measure(studyspec: StudySpec):
    repo = SqlAlchemyRepo.new(studyspec)

    assert isinstance(repo, SqlAlchemyRepo)

    print(repo.query_measure("measure1"))

    assert 5==6

import pytest

from doit.study.sqlalchemy.impl import (
    SqlAlchemyRepo,
)

from doit.study.spec import (
    OrdinalMeasureItemSpec,
    RelativeCodeMapName,
    RelativeMeasureNodeName,
    StudySpec,
    MeasureSpec,
    StudyConfigSpec,
)

@pytest.fixture
def studyspec():
    return StudySpec(
        config=StudyConfigSpec(
            title="Study Title",
            description=None,
            indices={},
        ),
        measures={
            "measure": MeasureSpec(
                title="Measure Title",
                description=None,
                items=(OrdinalMeasureItemSpec(
                    id=RelativeMeasureNodeName("ordinalmeasureitem"),
                    prompt="ordinal measure node prompt",
                    type="ordinal",
                    codes=RelativeCodeMapName('codemap1'),
                ),),
                codes={},
            ),
        },
        instruments={},
    )

def test_add_measure(studyspec: StudySpec):
    repo = SqlAlchemyRepo.new(studyspec)

    assert isinstance(repo, SqlAlchemyRepo)

    print(repo.query_measure("measure"))

    assert 5==6






import pytest
from faker import Faker
from .fakespec import StudySpecProvider

from doit.study.sqlalchemy.impl import (
    SqlAlchemyRepo,
)

fake = Faker()
fake.add_provider(StudySpecProvider)

@pytest.mark.parametrize("seed", [0, 1, 2])
def test_add_measure(seed: int):
    Faker.seed(seed)

    studyspec = fake.study_spec()

    measure_names = tuple(studyspec.measures)

    repo = SqlAlchemyRepo.new(studyspec)

    assert isinstance(repo, SqlAlchemyRepo)

    print(repo.query_measure(measure_names[0]))

    #assert 5==6

import pytest
from faker import Faker
from .fakespec import StudySpecProvider

from doit.study.sqlalchemy.impl import SqlAlchemyRepo

from doit.study.spec import StudySpec

fake = Faker()
fake.add_provider(StudySpecProvider)

@pytest.mark.parametrize("seed", [0, 1, 2])
def test_add_measure(seed: int):
    Faker.seed(seed)

    studyspec: StudySpec = fake.study_spec()

    measure_names = tuple(studyspec.measures)

    repo = SqlAlchemyRepo.new(studyspec)

    assert isinstance(repo, SqlAlchemyRepo)

    print(repo.query_measure(measure_names[0]))

    #assert 5==6

@pytest.mark.parametrize("seed", [0, 1, 2])
def test_add_instrument(seed: int):
    Faker.seed(seed)

    studyspec: StudySpec = fake.study_spec()

    instrument_names = tuple(studyspec.instruments)

    repo = SqlAlchemyRepo.new(studyspec)

    assert isinstance(repo, SqlAlchemyRepo)

    print(repo.query_instrument(instrument_names[0]).json(indent=3))
    
    #assert 5==6

@pytest.mark.parametrize("seed", [0, 1, 2])
def test_studytable(seed: int):
    Faker.seed(seed)

    studyspec: StudySpec = fake.study_spec()

    instrument_names = tuple(studyspec.instruments)

    repo = SqlAlchemyRepo.new(studyspec)

    assert isinstance(repo, SqlAlchemyRepo)

    print(repo.query_studytable_by_instrument(instrument_names[1]).json(indent=3))
    
    #assert 5==6

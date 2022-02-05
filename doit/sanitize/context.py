from pyrsistent import pvector
from .values import *

def load_study_context():
    return Study(
        instruments = pvector([
            Instrument(
                name = "student_behavior",
                long_name = "Student Behavior Questionnaire",
                description = "Description of Student Behavior Questionnaire",
                versions = pvector([
                    InstrumentVersion(
                        name = "Y1W1",
                        uri = "qualtrics://SV_asdfsdf"
                    ),
                    InstrumentVersion(
                        name = "Y1W2",
                        uri = "qualtrics://SV_asdfsdf"
                    )
                ])
            ),
            Instrument(
                name = "teacher_wellbeing",
                long_name = "Teacher Wellbeing Questionnaire",
                description = "Description of Teacher Wellbeing Questionnaire"
            )
        ])
    )
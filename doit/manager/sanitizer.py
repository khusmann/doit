import typing as t
import csv
from itertools import zip_longest
from ..domain.value import *
from ..settings import ProjectSettings

class SanitizerManager(ImmutableBaseModel):
    settings = ProjectSettings()

    def load_sanitizers(self, instrument_name: InstrumentName) -> t.List[Sanitizer]:
        return [ 
            self.load_sanitizer(instrument_name, SanitizerName(sanitizer_name)) 
                for sanitizer_name in self.settings.get_sanitizer_names(instrument_name)
        ]


    def load_sanitizer(self, instrument_name: InstrumentName, sanitizer_name: SanitizerName):
        with open(self.settings.sanitizer_file(instrument_name, sanitizer_name), 'r') as f:
            csvreader = csv.reader(f)

            column_names = next(csvreader)

            column_data = list(zip_longest(*csvreader))

            return Sanitizer(
                name=sanitizer_name,
                instrument_name=instrument_name,
                columns={ name: data for name, data in zip_longest(column_names, column_data, fillvalue=()) }
            )


    def write_sanitizers(self, sanitizers: t.Sequence[SanitizerUpdate]) -> t.List[Path]:
        return [self.write_sanitizer(sanitizer) for sanitizer in sanitizers]

    def write_sanitizer(self, sanitizer: SanitizerUpdate):
        self.settings.sanitizer_workdir(sanitizer.instrument_name).mkdir(exist_ok=True, parents=True)
        filename = self.settings.sanitizer_file(sanitizer.instrument_name, sanitizer.name)

        append = filename.exists()


        with open(filename, "a") as f:
            writer = csv.writer(f)

            if not append:
                writer.writerow(sanitizer.columns.keys())

            for row in zip_longest(*sanitizer.columns.values()):
                writer.writerow(row)

        return filename
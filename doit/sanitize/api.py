# build/downloaded/SV_asdfsadfsdf.json
# build/downloaded/SV_asdfsadfsdf.schema.json

# instruments/instrument/instrument-version.yaml

# sanitizers/instrument/instrument-column.csv

# UnsafeInstrumentData
## get_variable(variable, sanitizer) -> pd.Series
## get_variable_unsafe(variable) -> pd.Series
## get_schema() -> UnsafeStudySchema
## sanitize(InstrumentConfig, Mapping[str, Sanitizers]) -> InstrumentData
##      for var in InstrumentConfig.import_vars:
##          if self.needs_sanitization(var.remote_name):
##              get_variable(var.remote_name, sanitizers[var.name])
##          else:
##              get_variable(var.remote_name)

# InstrumentDataRepo
## load(instrument)
## save(instrument, InstrumentData) 

# sanitize_instrument_data(UnsafeInstrumentData, InstrumentConfig) -> InstrumentData

# InstrumentConfig
## get_import_items() -> List[ImportItem(varname: str, sanitizer: Sanitizer | None)]
## update_variable(self, UnsafeInstrumentVariable) -> InstrumentConfig
## update_sanitizer(self, UnsafeInstrumentVariable) -> InstrumentConfig
## update(UnsafeInstrumentData) -> InstrumentConfig
##      UnsafeInstrumentData.variables.fold(self.update_variables)
##      UnsafeInstrumentData.variables.fold(self.update_sanitizer)
## needs_save() -> bool

# InstrumentConfigRepo
## ls() -> List[(str, str)]
## new("instrument", "version") -> InstrumentConfig
## load("instrument", "version") -> InstrumentConfig
## save(InstrumentConfig)

# InstrumentSchema
## title
## description
## variables -> InstrumentVariableSchema[]

# InstrumentVariableSchema
## name
## type
## description

# Sanitizer
## instrument
## variable
## get_data_map -> Mapping[Str, Str]
## update(StudyConfig, StudyData) -> Sanitizer
## needs_save() -> bool

# SanitizerRepo
## ls() -> List[(str, str)]
## load(InstrumentConfig) -> 
## save("instrument", "version", "variable")

# StudyData
## schema -> InstrumentSchema

# StudyDataRepo
## ls() -> List[(str, str)]
## load_data("uri")
## download_data("uri") (download data; download schema)

def qualtrics_add(instrument_name: str, instrument_version: str, uri: str):
    # Prompt create folder if instrument_name doesn't exist
    # try:
    # ic = get_study_config() throws ConfigError
    # schema = qualtrics.schema_from_uri(uri) throws typeof(QualtricsError)
    # ic = ic.add_instrument_version(instrument_name, instrument_version, schema) throws VersionAlreadyExistsError
    # click.secho("This action will have the following effects:")
    # click.secho(ic.effects)
    # click.prompt("Continue? [y/n]")
    # ic.save()
    pass

def qualtrics_list():
    # 
    pass

def download():
    pass

def update_instrument(instrument_name: str | None = None, instrument_version: str | None = None):
    ## Check downloaded schema for new fields; add any new fields to instrument def
    ## Prereq validation:
    ## - Does /instruments/instrument.yaml exist? Does it have instrument_version?
    ## - Do I have a downloaded schema for that instrument?
    ## - Does the schema have fields that the instrument doesn't have?

    ## If so, then add the new schema fields to the instrument
    ## Prompt with changes
    ## Execute changes
   pass

def update_sanitizer(instrument_name: str | None = None, var: str | None = None):
    ## Check instrument def for new fields that should be sanitized; add any new fields to instrument def
    ## Check data for new unique values; add any new values to sanitizer def
    ## Prereq validation:
    ## - Does /instruments/instrument.yaml exist? Does it have instrument_version?
    ## - Does the downloaded schema exist?
    ## - Does the instrument need updating based on the schema? -> Fail if not
    ## - Does the data have unique values to add to the sanitizer? -> If not, fail with "All updated"
    ## If so, then add new unique values to the sanitizer
    ## Prompt with changes
    ## Execute changes
    pass

def preview(instrument_name: str, instrument_version: str, column: str):
    ## Print out a before / after column, with edits highlighted
    pass

def sanitize():
    ## Create build/sanitized/study_sanitized.hd5
    pass
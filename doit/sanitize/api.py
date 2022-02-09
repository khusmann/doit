
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
    # ir = InstrumentRepo, sr = SchemaRepo
    pass

def update_instrument(instrument_id: str | None = None, version_id: str | None = None):
    # instruments = io.load_instrumentconfig(instrument_name, instrument_version)
    # schemas = map(io.load_schema, instruments)
    # new_instruments = starmap(domain.update_instrument, zip(instruments, schemas)
    # modified = filter(InstrumentConfig.was_modified, new_instruments)
    #
    # for i in modified_instruments:
    #     echo(i.history)
    #
    # if prompt("Save changes Y/N?"):
    #     for i in instruments:
    #          ir.save(i)

   pass

def update_sanitizer(instrument_name: str | None = None, var: str | None = None):
    # instruments = io.load_instrumentconfig(instrument_name, instrument_version)
    # sanitizers = map(io.load_sanitizer, instruments)
    # blobs = map(io.load_blob, instruments)
    # new_sanitizers = starmap(domain.update_sanitizer, zip(sanitizers, blobs)
    # modified_sanitizers = filter(InstrumentConfig.was_modified, new_sanitizers)
    #
    # for s in modified_sanitizers:
    #     echo(i.history)
    #
    # if prompt("Save changes Y/N?"):
    #     for i in instruments:
    #          ir.save(i)
    pass

def update(instrument_name: str | None = None, var: str | None = None):
    # instruments = io.load_instrumentconfig(instrument_name, instrument_version)
    # schemas = map(io.load_schema, instruments)
    #
    # new_instruments = starmap(update_instrument, zip(instruments, schemas)
    # modified_instruments = filter(InstrumentConfig.was_modified, new_instruments)
    #
    # sanitizers = map(io.load_sanitizer, instruments)
    # blobs = map(io.load_blob, schemas)
    #
    # new_sanitizers = starmap(domain.update_sanitizer, zip(sanitizers, blobs)
    # modified_sanitizers = filter(InstrumentConfig.was_modified, new_sanitizers)
    #
    # for i in modified_instruments:
    #     echo(i.history)
    #
    # for s in modified_sanitizers:
    #     echo(i.history)
    #
    # if prompt("Save changes Y/N?"):
    #     for i in instruments:
    #          ir.save(i)

    pass

def preview(instrument_name: str, instrument_version: str, column: str):
    # instrument = io.load_instrumentconfig(instrument_name, instrument_version)
    # sanitizer = io.load_sanitizers(instrument_name, instrument_version)[column]
    # blob = io.load_blob(instrument)
    #
    # original = blob.variables[column].data
    # sanitized = domain.sanitize()

    pass

def sanitize():
    ## Create build/sanitized/study_sanitized.hd5
    pass
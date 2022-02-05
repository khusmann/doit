# doit

## doit-src

Add a new source:

```
doit-src add instrument_name [version_name] [uri]
```

Re-download sources; set to most recent version

```
doit-src update instrument_name [version_name] [uri]
```

List source download versions; set env to version

```
doit-src version [versionId]
```

Generate / update sanitizers

```
doit-src sangen 
```

Run sanitizers; new bundle in build/sanitized. Only run if repo is clean

```
doit-src sanitize
```

## doit


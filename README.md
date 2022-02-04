# doit

## doit-san

Add a new source:

```
doit-san source add
```

Re-download sources; set to most recent version

```
doit-san source update [source-name]
```

List source download versions; set env to version

```
doit-san source version [versionId]
```

Generate sanitizers

```
doit-san sanitizer generate [source-name]
```

Run sanitizers; new bundle in build/sanitized. Only run if repo is clean

```
doit-san sanitizer run
```

## doit


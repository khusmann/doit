# Doit Architecture

(inspired by https://matklad.github.io/2021/02/06/ARCHITECTURE.md.html)

TODO: Fill this in more

# Module Design Rules / Principles

## src/doit

- cli.py:
  The UI. Makes calls to the app.py to load / save entites based on user input;
  makes calls to domain services to perform computations on those entites. Prints
  results / prompts as needed.

- app.py:
  Main function is to load / save versions of entities. Right now the "version control"
  is very rudimentary (simple renaming); in the future this will be more complete.

## src/doit/[module]

- model.py:
  Only type definitions. Prefer NamedTuple types, but @dataclasses are necessary for
  generics. Avoid pydantic.BaseModel, but ok if it will be serialized.

- io.py:
  Functions; this is the module's API for loading / saving types found in model.py. 
  Avoid implementation here. Instead, api entrypoints should figure out which
  implementation to use and invoke by loading the proper module in ./impl and
  relaying the proper info.

## src/doit/[module]/impl

- [impl-name].py:

  Implementations of loading / saving APIs; interface to outside world. Sandboxes
  a particular library (like SqlAlchemy). This code should only be called by
  functions in io.py. (With exceptions for writing tests)

## src/doit/service

- [service-name].py
  The main domain service. Core / critical functionality should be implemented here. These
  functions tie together types from all the modules. Functions should be 100% pure
  and ONLY import [module].model definitions. (No io!). A good indicator that you should
  make a service is that you're doing a computation that requires multiple types of
  models. If you're working in one module and find yourself importing a definition from
  another module, you probably should be doing what you're doing in a service instead.

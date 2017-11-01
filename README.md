# daglet: DAG tools for Python

[![Build status](https://travis-ci.org/kkroening/daglet.svg?branch=master)](https://travis-ci.org/kkroening/daglet)


## Overview

`daglet` standardizes common directed acyclic graph operations in a functional manner:
- Topological sorting
- Transformations
- Vertex/edge representation
- Graph visualization
- Graph diffing / change detection

The library is designed in such a way that these features can be utilized á la carte, and for the most part is a set of pure functions along with an immutable `Vertex` representation.

## Installation

The easiest way to acquire the latest version of `daglet` is through pip:

```
pip install daglet
```

Alternatively, it's possible to clone the source and put it on your python path (`$PYTHONPATH`, `sys.path`, etc.):
```
$ git clone git@github.com:kkroening/daglet.git
$ export PYTHONPATH=${PYTHONPATH}:daglet
$ python
>>> import daglet
```

## Running tests

```
$ virtualenv venv
$ . venv/bin/activate
(venv) $ pip install -r requirements.txt
(venv) $ pytest
```

## [API Reference](https://kkroening.github.io/daglet/)

API documentation is automatically generated from python docstrings and hosted on github pages: https://kkroening.github.io/daglet/

Alternatively, standard python help is available, such as at the python REPL prompt as follows:
```
>>> import daglet
>>> help(daglet)
```

## Contributing

Feel free to report any bugs or feature requests.  Pull requests are welcome as well.

## Additional Resources

- [API Reference](https://kkroening.github.io/daglet/)
- [Tests](https://github.com/kkroening/daglet/blob/master/daglet/test_daglet.py)

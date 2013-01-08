Revori Terminal and Client
==========================

This is a python-based client for use in systems integration testing, protocol documentation, and debugging.  Since the protocol itself is still in flux, this code should be considered in an early alpha state, and may break spontaneously as Revori changes.

Using `revterm`
--------------

Everything in `revterm` is designed to work with both python [2.7](http://www.python.org/download/releases/2.7/) and [python 3.3](http://www.python.org/download/releases/3.3.0/).  it is also usable either as a standalone application or as an import in other python applications. 

In order to use revterm as a command-line client:

```
$ python revterm
```

This will open a connection to revori at `localhost:8017` by default.   Using the `--help` option will show the command line arguments to change these settings.

In order to run the tests:

```
$ python setup.py test
```

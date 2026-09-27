"""Configuration, data access, shared types and utilities.

Modules
-------
assets
    Quote precision and minimum order size of tradable assets.
concurrency
    Determine how many consumer processes to run.
config
    Framework-wide configuration settings.
data_loader
    Market data containers that load bar data on demand.
database
    Database connection pool and SQL results as pandas DataFrames.
decorators
    Decorators that instrument function execution.
exceptions
    Exceptions raised by the framework.
hyperparameter
    Hyper-parameter definitions for the back-test optimizer.
list_utils
    Sequence utilities.
market_data
    Market data, industry membership and trading calendar lookups.
tlog
    Framework logging, optionally forwarded to Google Cloud Logging.
trading_data
    Global trading state shared within a process.
types
    Enumerations and broker-neutral data types shared by the framework.
"""

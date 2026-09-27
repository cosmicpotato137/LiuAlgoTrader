"""Database models for runs, trades, portfolios and accounts.

Modules
-------
accounts
    Persist and load accounts and their transactions.
algo_run
    Persist and load strategy run records and their batches.
gain_loss
    Persist and load the gain and trade analyses of strategy runs.
keystore
    Persist string values under unique keys in the database.
new_trades
    Persistence of the trade operations made by strategy runs.
optimizer
    Persist and load the batches run by optimizer sessions.
portfolio
    Persist and load trading portfolios.
ticker_data
    Persist and load ticker descriptions and daily price data.
ticker_snapshot
    Data type for the volume and daily change of a symbol.
tradeplan
    Load the trade plan entries that assign strategies to portfolios.
trades
    Deprecated persistence of round-trip trades.
trending_tickers
    Persist and load the trending symbols of a batch.
"""

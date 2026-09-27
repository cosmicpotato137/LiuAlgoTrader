## Structural changes

- [x] Add docstrings
- [x] Update python and package versions
- [ ] Restructure classes to follow the pattern: Filters, Indicators, Signals, Rules. These classes are composed as part of a Strategy
- [ ] Update the docker compose: create a docker folder to hold configs for local development
- [ ] Abstract out the broker and data apis and create a wrapper for alpaca
- [ ] Use an ORM to manage database. Might need to make some updates to include the OHLC timescale?
- [ ] Update database schema - not sure what's going on there

## Possible bugs

- Infinite loop: in common/market_data.py:146 (sp500_historical_constituents), the loop re-adds removed tickers only if they are already in the set. That re-add does nothing but still marks the pass as having changes, so the while True loop never ends when any removed ticker is present.
- Tradier always fails: trading/trader_factory.py:26-27 stores the Tradier trader under "TRADIER" but returns traders["TradierTrader"], so every call raises KeyError.
- Invalid SQL: models/algo_run.py's get_batch_list_by_date puts ORDER BY before WHERE, which Postgres rejects.
- Broken queries: analytics/analysis.py:94,149 has literal \x1f control characters instead of newlines in the SQL f-strings of load_trades and load_runs, so those queries are likely broken.
- Setting ignored: in miners/stock_cluster.py, the num_workers setter checks the value but never stores it.
- GeminiTrader.cancel_order returns a dict despite its -> bool annotation.
- enhanced_backtest.py checks the signature of buy_callback when handling sells.
- Several queue.Full handlers raise KeyError on event['sym'], because that key no longer exists by then.
- reprocess/portfolio.py is missing an await and calls a Portfolio.load_details method that doesn't exist.
- SQL is built with f-strings from arguments in a few places, which is an injection risk.
- Retries on 429 and 502 responses have no limit.

If you'd like, I can commit the docstrings on a branch, collect the full list of reported issues, or start fixing the confirmed ones.
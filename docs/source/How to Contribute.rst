Contributing
============
Would you like to help improve & evolve LiuAlgoTrader? 
Do you have a suggestion, comment, idea for improvement or 
a have a wish-list item? Please read our
Contribution Document_ or email me at amichay@sgeltd.com.

.. _Document: https://github.com/amor71/LiuAlgoTrader/blob/master/CONTRIBUTING.md

How to setup a development environment
--------------------------------------

LiuAlgoTrader manages its dependencies with PDM_. ``pyproject.toml`` declares
them, and ``pdm.lock`` pins the exact versions to develop and test with.

.. _PDM: https://pdm-project.org

Prerequisites:

- Python 3.12 or later,
- PDM, for example installed with ``pipx install pdm`` or ``brew install pdm``,
- Docker Engine and Docker Compose, for the local PostgreSQL database.

1. Clone LiuAlgoTrader:

   .. code-block:: bash

       git clone https://github.com/amor71/LiuAlgoTrader.git
       cd LiuAlgoTrader

   This checks out the ``master`` branch, which is the latest development
   version. It may not be the most stable version; the latest stable version
   is the latest tagged version.

2. Install LiuAlgoTrader and the development tools:

   .. code-block:: bash

       pdm install -G dev

   PDM creates a virtual environment in ``.venv``, installs the locked
   dependencies into it, and installs LiuAlgoTrader in editable mode, along
   with the ``liu``, ``trader``, ``backtester``, ``optimizer``,
   ``market_miner`` and ``portfolio`` commands. To choose the Python
   interpreter, run ``pdm use <path to python>`` before installing. Prefix
   commands with ``pdm run``, or activate the environment with
   ``source .venv/bin/activate``.

3. Install the git pre-commit hook:

   .. code-block:: bash

       pdm run pre-commit install

   On each commit, the hook runs mypy, bandit, black, isort and autoflake on
   the staged Python files. To run the hooks on other files, use
   ``pdm run pre-commit run --files <files>``.

4. If you have not yet set up a local database, run the quickstart wizard:

   .. code-block:: bash

       pdm run liu quickstart

   Follow these_ instructions, skipping `Step 1`. The wizard starts
   PostgreSQL in Docker and sets up the environment variables that
   LiuAlgoTrader reads. :doc:`Configuration` lists all of them.

   .. _these: https://liualgotrader-v2.readthedocs.io/en/latest/Quickstart.html

5. Run the tests:

   .. code-block:: bash

       pdm run pytest
       pdm run pytest tests/test_resample.py

   Most tests need the database (``DSN``). Tests of a broker or data provider
   also need its API keys, such as ``APCA_API_KEY_ID`` and
   ``APCA_API_SECRET_KEY`` for Alpaca.

To add or upgrade a dependency, run ``pdm add <package>`` or
``pdm update <package>``, and commit both ``pyproject.toml`` and ``pdm.lock``.
Development tools belong in the ``dev`` group: ``pdm add -G dev <package>``.


Contributors
------------

Special thanks to the below individuals for their comments, reviews and suggestions:

- Jonathan Morland-Barrett sigmantium_

.. _sigmantium: https://github.com/sigmantium

- Alex Lau riven314_

.. _riven314: https://github.com/riven314

- Rokas Gegevicius ksilo_

.. _ksilo: https://github.com/ksilo

- Shlomi Kushchi shlomikushchi_

.. _shlomikushchi: https://github.com/shlomikushchi

- Venkat Y vinmestmant_

.. _vinmestmant: https://github.com/vinmestmant

- Chris crowforc3_

.. _crowforc3: https://github.com/crawforc3

- TheSnoozer_

.. _TheSnoozer: https://github.com/TheSnoozer

- Aditya Gupta adi0x90_

.. _adi0x90: https://github.com/adi0x90




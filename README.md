# Fulcra Dynamics Demos

This is a public repository of notebooks demonstrating the use of Fulcra's Life API.

For Fulcra's main developer docs page, see [https://fulcradynamics.github.io/developer-docs/](https://fulcradynamics.github.io/developer-docs/).

These notebooks use the `fulcra-api` Python module.  See the detailed guide and API reference for this here: [https://fulcradynamics.github.io/fulcra-api-python/](https://fulcradynamics.github.io/fulcra-api-python/)

All code here is covered under the [Apache 2.0 license](LICENSE).

Repo layout:

- [notebooks/](notebooks/) contains the legacy Jupyter/Colab `.ipynb` notebooks.
- [marimo/](marimo/) contains local-first marimo notebook apps.
- [scripts/](scripts/) contains local CLI helpers.
- [fulcra_demos/](fulcra_demos/) contains shared Python logic used by scripts, tests, and notebooks.
- [tests/](tests/) contains executable checks for the shared logic.

## Running locally with marimo

This repo now supports a local-first notebook workflow with [marimo](https://marimo.io/).

From the repo root:

```bash
uv sync
uv run marimo edit marimo/00_hello_fulcra_marimo.py
```

That will create a local `.venv`, install the notebook dependencies, and open the marimo editor in your browser.

Starter marimo notebook:

- [marimo/00_hello_fulcra_marimo.py](marimo/00_hello_fulcra_marimo.py)

It follows the same basic flow as the existing "Hello Fulcra" notebook:

1. Create a `FulcraAPI` client.
2. Authorize in the browser.
3. Fetch grouped and single-metric time series data.
4. Inspect the returned pandas DataFrames locally.

Recommended notebook for location visits:

- [marimo/location_visits_walkthrough.py](marimo/location_visits_walkthrough.py)

This notebook uses the shared, tested visit-reconstruction logic from
[fulcra_demos/location_visits.py](fulcra_demos/location_visits.py) and is the
best current path for understanding or presenting the location-visit workflow.

## Running the Visits CLI

To print a tidy visit table at the command line:

```bash
uv run python scripts/location_visits.py visits-table \
  --start-time 2026-03-17T00:00:00-04:00 \
  --end-time 2026-03-18T00:00:00-04:00 \
  --sample-rate 60 \
  --window-size 14400 \
  --min-visit-minutes 5 \
  --bridge-gap-minutes 3 \
  --stationary-distance-threshold-m 150
```

If you need to authenticate first:

```bash
uv run python scripts/location_visits.py start-auth
uv run python scripts/location_visits.py poll-auth
```


To get started quickly, within Colab: click here: <a target="_blank" href="https://colab.research.google.com/github/fulcradynamics/demos/blob/main/notebooks/00_Hello_Fulcra.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>

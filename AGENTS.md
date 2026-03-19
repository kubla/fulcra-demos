# AGENTS.md

Instructions for creating or updating **Python notebooks that use Fulcra API** in this repository.

## Scope
These instructions apply to the entire repository tree rooted here.

## Goal
When building a notebook, optimize for:
1. Quick reproducibility in Colab/Jupyter.
2. Clear narrative flow (explain first, then code).
3. Safe use of authentication (never hardcode tokens/secrets).
4. Dataframe-first analysis patterns that are easy to inspect and chart.

## Notebook structure (recommended)
Use this flow unless the task explicitly requires something else:

1. **Title + short problem statement** (Markdown)
2. **Install cell**
   - ` %pip install -qU fulcra-api`
3. **Imports + client setup**
   - `from fulcra_api.core import FulcraAPI`
   - Initialize once: `fulcra = FulcraAPI()`
4. **Authorization section**
   - Explain device/browser login.
   - Use `fulcra.authorize()` in a dedicated cell.
5. **Timeframe / parameters section**
   - Centralize `start_date`, `end_date`, and key filter IDs in one place.
6. **Data retrieval section**
   - Fetch Fulcra data in clear, composable steps.
7. **Data shaping section**
   - Convert JSON/list payloads to Pandas DataFrames.
   - Handle nulls/types explicitly.
8. **Display + validation section**
   - Show head/sample/summary before plotting.
9. **Visualization / analysis section**
   - Prefer readable axes, legends, labels, and comments.
10. **Optional interpretation section**
   - Summarize findings and limitations.

## Fulcra API usage guidance
Based on `fulcra_api/core.py` and existing demo notebooks:

- Primary entrypoint is `FulcraAPI`.
- Typical notebook auth flow is `fulcra.authorize()` (device auth).
- Many data calls take `start_time` / `end_time`; standardize on ISO8601-compatible values.
- You can retrieve the current user id with `fulcra.get_fulcra_userid()` when needed.
- `fulcra.fulcra_v1_api(...)` exists for lower-level access if a convenience method is missing.

Prefer high-level convenience methods (for example notebook-specific methods like workout/event helpers) before dropping to raw endpoint calls.

## Coding conventions for notebook cells

- Keep cells small and single-purpose.
- Add a short Markdown heading before each major code block.
- Avoid duplicated imports; keep global imports near the top.
- Use descriptive variable names:
  - `df_workouts`, `df_calories`, `daily_summary`, etc.
- When using loops/parallelization, include a short comment explaining why.
- Avoid hidden state: define key parameters in explicit cells.

## Data handling expectations

- Convert timestamps to timezone-aware datetimes when comparing/merging time series.
- Aggregate to the intended grain explicitly (hour/day/week).
- When joining datasets, document join keys and join type.
- Guard against empty API responses and show a user-friendly message.

## Visualization expectations

- Title charts with metric + timeframe.
- Label axes with units.
- If using seaborn/matplotlib, set figure size intentionally.
- If correlation/regression is shown, include a caveat that correlation is not causation.

## Security and privacy rules

- Never commit access tokens, refresh tokens, API keys, or copied bearer headers.
- Never print entire JWTs in notebook output.
- Do not hardcode personal user IDs unless the notebook is explicitly about shared/public demo IDs.

## Minimal starter template

```python
%pip install -qU fulcra-api

from datetime import datetime
import pandas as pd
from fulcra_api.core import FulcraAPI

fulcra = FulcraAPI()
fulcra.authorize()

start_time = datetime(2024, 1, 1)
end_time = datetime(2024, 1, 31)

# Example: replace with the specific Fulcra method for your analysis
# records = fulcra.some_data_method(start_time=start_time, end_time=end_time)
# df = pd.DataFrame(records)
# display(df.head())
```

## Pre-commit checklist for notebook edits

- Notebook runs top-to-bottom on a clean runtime.
- Install/auth/data-fetch cells are present and ordered logically.
- Outputs are readable (no overwhelming raw dumps).
- No secrets or sensitive IDs in cells or outputs.
- Narrative markdown explains what each major section does.

# notebooks/

Experimentation and exploration. **Nothing here is imported by the pipeline**: the DAGs and `src/` run without these notebooks and without their extra dependencies.

| Notebook | What it covers |
|---|---|
| `01_eda.ipynb` | **Stage A: exploratory data analysis.** Three dated batches (2026-10-01/02/03) are inspected in detail, then all 46 files: feature types, missing values, duplicates, invalid values, category spellings, date and label checks, class balance and drift, feature patterns, engineered features, raw vs Silver, and which problems are fixed in Silver vs learned in the training pipeline. |

The notebook reuses the pipeline's own code (`clean_orders`, `add_features`, `NUMERIC_RULES` from `src/`), so its "cleaned" views use exactly the Silver rules.

## Running it

The notebook tooling is an optional uv group, so it's never installed in the Airflow image:

```bash
uv sync --group experiments
```

Open `01_eda.ipynb` in VS Code or Jupyter and pick the project's `.venv` as the kernel. To re-execute it headless and keep the outputs:

```bash
uv run --group experiments jupyter nbconvert --to notebook --execute --inplace notebooks/01_eda.ipynb
```

The notebook switches to the repository root itself, so it works from either folder.

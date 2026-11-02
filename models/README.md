# models/

| File | Content |
|---|---|
| `delivery_delay_model.joblib` | The evaluated scikit-learn `Pipeline` (preprocessing + `LogisticRegression`), written by `save_model`. It is always the latest model. |
| `versions/vYYYYMMDDTHHMMSSZ/model.joblib` | Immutable copy of every trained model, named by its UTC training time |
| `versions/vYYYYMMDDTHHMMSSZ/metrics.json` | The `ModelReport` of that version |

The pipeline includes every preprocessing step, so it accepts Gold columns directly:

```python
import joblib
import pandas as pd

from src.schemas import FEATURES

model = joblib.load("models/delivery_delay_model.joblib")
gold = pd.read_csv("data/gold/date=2026-10-20/orders.csv")
model.predict(gold[list(FEATURES)])
```

## Versioning (`feature/model-versioning`)

Every `save_model` run writes a new `versions/<version>/` folder and records the same `model_version` in `metrics/metrics.json`. To roll back, copy an older `versions/<version>/model.joblib` over `delivery_delay_model.joblib`. To compare runs, diff their `metrics.json` files.

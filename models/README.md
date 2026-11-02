# models/

| File | Content |
|---|---|
| `delivery_delay_model.joblib` | The evaluated scikit-learn `Pipeline` (preprocessing + `LogisticRegression`), written by `save_model` |

The pipeline includes every preprocessing step, so it accepts Gold columns directly:

```python
import joblib
import pandas as pd

from src.schemas import FEATURES

model = joblib.load("models/delivery_delay_model.joblib")
gold = pd.read_csv("data/gold/date=2026-10-20/orders.csv")
model.predict(gold[list(FEATURES)])
```

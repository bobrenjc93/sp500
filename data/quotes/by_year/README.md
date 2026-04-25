# Yearly Quote Files

Each year is stored as a single browser-friendly CSV:

- `YYYY.csv`

Use `../preview/YYYY.csv` when you only want the first 200 sorted rows for a quick scan.

Examples:

```bash
head data/quotes/by_year/2025.csv
python3 -c "import pandas as pd; print(pd.read_csv('data/quotes/by_year/2025.csv').head())"
```

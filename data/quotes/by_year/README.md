# Yearly Quote Files

Each year is stored twice in this folder:

- `YYYY.csv` is the full browser-friendly CSV.
- `YYYY.csv.gz` is the gzip-compressed copy of the same dataset.

Use `../preview/YYYY.csv` when you only want the first 200 sorted rows for a quick scan.

Examples:

```bash
head data/quotes/by_year/2025.csv
gzip -dc data/quotes/by_year/2025.csv.gz | head
python3 -c "import pandas as pd; print(pd.read_csv('data/quotes/by_year/2025.csv').head())"
```

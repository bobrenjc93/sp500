# Yearly Quote Archives

These files are the full yearly quote datasets stored as gzip-compressed CSV archives (`*.csv.gz`).

GitHub does not render compressed CSV files inline, so clicking one of these files in the web UI will usually show a binary/preview error instead of a table view.

Use one of these paths instead:

- Open `../preview/YYYY.csv` for a browser-friendly sample of the matching year.
- Download `YYYY.csv.gz` when you need the full file.

Examples:

```bash
gzip -dc data/quotes/by_year/2025.csv.gz | head
python3 -c "import pandas as pd; print(pd.read_csv('data/quotes/by_year/2025.csv.gz').head())"
```

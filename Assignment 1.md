```python
import pandas as pd
import numpy as np
```


```python
# ---- Configuration ----
from pathlib import Path
import pandas as pd

# Point this to the parent folder that contains the 24 month-folders (e.g., 2023-12, 2024-01, ... 2025-11)
BASE_DIR = Path(r"C:\Users\C29347E\OneDrive - EXPERIAN SERVICES CORP\Uni\Crime Data")

# File name expected in each month folder (adjust if your file names differ)
# e.g., Crime data exports are often like "2023-12-west-midlands-street.csv"
FILENAME_SUFFIX = "-west-midlands-street.csv"

# Output locations (optional but recommended)
INTERIM_DIR = BASE_DIR / "_interim"
PROCESSED_DIR = BASE_DIR / "_processed"
INTERIM_DIR.mkdir(exist_ok=True, parents=True)
PROCESSED_DIR.mkdir(exist_ok=True, parents=True)

# Expected months (2023-12 ... 2025-11) — 24 months
expected_months = pd.period_range("2023-12", "2025-11", freq="M").astype(str).tolist()

print("Looking for data under:", BASE_DIR)
print("Expecting month folders for:", expected_months[0], "to", expected_months[-1])
```

    Looking for data under: C:\Users\C29347E\OneDrive - EXPERIAN SERVICES CORP\Uni\Crime Data
    Expecting month folders for: 2023-12 to 2025-11
    


```python
def find_monthly_csvs(base_dir: Path, months: list[str], suffix: str):
    files = []
    missing = []
    for ym in months:
        month_dir = base_dir / ym
        csv_path = month_dir / f"{ym}{suffix}"
        if csv_path.exists():
            files.append(csv_path)
        else:
            # Fallback: if the exact file name differs, try globbing for any CSV in the folder
            candidates = list(month_dir.glob("*.csv"))
            if len(candidates) == 1:
                files.append(candidates[0])
            elif len(candidates) > 1:
                # If multiple CSVs, pick the one that includes 'street' by preference
                street = [c for c in candidates if "street" in c.name.lower()]
                if street:
                    files.append(street[0])
                else:
                    files.append(candidates[0])  # last resort
            else:
                missing.append(ym)
    return files, missing

files, missing = find_monthly_csvs(BASE_DIR, expected_months, FILENAME_SUFFIX)
print(f"Found {len(files)} CSV files.")
if missing:
    print("Missing folders or CSVs for months:", missing)

# Load & merge
frames = []
for f in files:
    # Infer source_month from parent folder name (YYYY-MM)
    source_month = f.parent.name
    # Use low-memory and encoding fallbacks for OneDrive CSVs
    try:
        df = pd.read_csv(f, low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(f, low_memory=False, encoding="latin-1")
    df["source_month"] = source_month
    frames.append(df)

if not frames:
    raise FileNotFoundError("No CSVs loaded. Please check BASE_DIR and file names.")

merged = pd.concat(frames, ignore_index=True)
print("Merged shape:", merged.shape)
merged.head()
```

    Found 24 CSV files.
    Merged shape: (668108, 13)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Crime ID</th>
      <th>Month</th>
      <th>Reported by</th>
      <th>Falls within</th>
      <th>Longitude</th>
      <th>Latitude</th>
      <th>Location</th>
      <th>LSOA code</th>
      <th>LSOA name</th>
      <th>Crime type</th>
      <th>Last outcome category</th>
      <th>Context</th>
      <th>source_month</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>fdbff4da55066884700d48f6ca48e03fbe8a75b6a143cc...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.849790</td>
      <td>52.590937</td>
      <td>On or near Walsall Road</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Public order</td>
      <td>Unable to prosecute suspect</td>
      <td>NaN</td>
      <td>2023-12</td>
    </tr>
    <tr>
      <th>1</th>
      <td>02affdeba7ab5d450fe62e72a2009d0cd796d5aa66f5a2...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.849790</td>
      <td>52.590937</td>
      <td>On or near Walsall Road</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Theft from the person</td>
      <td>Investigation complete; no suspect identified</td>
      <td>NaN</td>
      <td>2023-12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>9e26f25dc8749749da9e14400236bec7f30872cfe75f9a...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.847123</td>
      <td>52.593864</td>
      <td>On or near Bramble Way</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Vehicle crime</td>
      <td>Investigation complete; no suspect identified</td>
      <td>NaN</td>
      <td>2023-12</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4026edb36cf730ba9ac2b5d7e385d3d20c26fb5c07927c...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.849790</td>
      <td>52.590937</td>
      <td>On or near Walsall Road</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Violence and sexual offences</td>
      <td>Unable to prosecute suspect</td>
      <td>NaN</td>
      <td>2023-12</td>
    </tr>
    <tr>
      <th>4</th>
      <td>452f4613d8c6ca1ba1bdd38b592641bb8d1aafcbf19ea9...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.849790</td>
      <td>52.590937</td>
      <td>On or near Walsall Road</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Violence and sexual offences</td>
      <td>Investigation complete; no suspect identified</td>
      <td>NaN</td>
      <td>2023-12</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Check month coverage from provenance column
months_found = sorted(merged["source_month"].astype(str).unique().tolist())
print("Months present (from data):", months_found)
print("Expected #months:", len(expected_months), " | Found #months:", len(months_found))

missing_in_data = [m for m in expected_months if m not in months_found]
extra_in_data = [m for m in months_found if m not in expected_months]

if missing_in_data:
    print("WARNING – Missing months in merged data:", missing_in_data)
if extra_in_data:
    print("NOTE – Extra months present in data:", extra_in_data)

# Basic columns check (these are typical for data.police.uk "street" crime files)
expected_columns = {"Crime ID", "Month", "Reported by", "Falls within", "Longitude", "Latitude",
                    "Location", "LSOA code", "LSOA name", "Crime type", "Last outcome category"}

present = set(merged.columns)
print("Columns present:", sorted(list(present)))
missing_cols = expected_columns - present
if missing_cols:
    print("Columns not found (may be OK depending on export):", missing_cols)

# Dtypes & quick cleaning (optional, keeps this cell short)
for col in ("Longitude", "Latitude"):
    if col in merged.columns:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

# Quick dup check on Crime ID if present
if "Crime ID" in merged.columns:
    dup_count = merged.duplicated(subset=["Crime ID"]).sum()
    print(f"Duplicate Crime ID rows: {dup_count}")
else:
    print("Note: 'Crime ID' not available; duplicate detection will use a fallback later if needed.")

```

    Months present (from data): ['2023-12', '2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12', '2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11']
    Expected #months: 24  | Found #months: 24
    Columns present: ['Context', 'Crime ID', 'Crime type', 'Falls within', 'LSOA code', 'LSOA name', 'Last outcome category', 'Latitude', 'Location', 'Longitude', 'Month', 'Reported by', 'source_month']
    Duplicate Crime ID rows: 51666
    


```python
# Derive year_month, year, month for analysis
if "Month" in merged.columns:
    merged["year_month"] = pd.PeriodIndex(pd.to_datetime(merged["Month"]), freq="M").astype(str)
    merged["year"] = merged["year_month"].str[:4]
    merged["month"] = merged["year_month"].str[5:7]
else:
    # Fallback: use source_month if Month is missing
    merged["year_month"] = merged["source_month"]
    merged["year"] = merged["year_month"].str[:4]
    merged["month"] = merged["year_month"].str[5:7]

# Outcome bucketing (optional quick pass)
if "Last outcome category" in merged.columns:
    mapping = {
        "Investigation complete; no suspect identified": "No further action",
        "Status update unavailable": "Unknown/No outcome",
        "Court result unavailable": "Court/Legal",
    }
    merged["outcome_bucket"] = merged["Last outcome category"].map(mapping).fillna(merged["Last outcome category"])

print("Prepared shape:", merged.shape)
merged[["source_month", "year_month", "year", "month"]].head()
```

    Prepared shape: (668108, 17)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>source_month</th>
      <th>year_month</th>
      <th>year</th>
      <th>month</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Row counts by month
counts = (merged
          .groupby("year_month")
          .size()
          .rename("rows")
          .reset_index()
          .sort_values("year_month"))
display(counts)

# Crime type distribution
if "Crime type" in merged.columns:
    display(merged["Crime type"].value_counts().head(15))
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year_month</th>
      <th>rows</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2023-12</td>
      <td>28058</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2024-01</td>
      <td>27500</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2024-02</td>
      <td>26067</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2024-03</td>
      <td>28383</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2024-04</td>
      <td>28495</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2024-05</td>
      <td>31037</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2024-06</td>
      <td>29865</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2024-07</td>
      <td>31636</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2024-08</td>
      <td>29180</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2024-09</td>
      <td>28635</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2024-10</td>
      <td>29074</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2024-11</td>
      <td>26571</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2024-12</td>
      <td>25723</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2025-01</td>
      <td>25601</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2025-02</td>
      <td>24624</td>
    </tr>
    <tr>
      <th>15</th>
      <td>2025-03</td>
      <td>27967</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2025-04</td>
      <td>27296</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2025-05</td>
      <td>28292</td>
    </tr>
    <tr>
      <th>18</th>
      <td>2025-06</td>
      <td>28708</td>
    </tr>
    <tr>
      <th>19</th>
      <td>2025-07</td>
      <td>29191</td>
    </tr>
    <tr>
      <th>20</th>
      <td>2025-08</td>
      <td>27312</td>
    </tr>
    <tr>
      <th>21</th>
      <td>2025-09</td>
      <td>26066</td>
    </tr>
    <tr>
      <th>22</th>
      <td>2025-10</td>
      <td>27012</td>
    </tr>
    <tr>
      <th>23</th>
      <td>2025-11</td>
      <td>25815</td>
    </tr>
  </tbody>
</table>
</div>



    Crime type
    Violence and sexual offences    266582
    Shoplifting                      62492
    Vehicle crime                    59127
    Anti-social behaviour            51667
    Criminal damage and arson        50311
    Other theft                      41491
    Public order                     37459
    Burglary                         32222
    Drugs                            17005
    Robbery                          14162
    Other crime                      13209
    Possession of weapons            12946
    Theft from the person             5613
    Bicycle theft                     3822
    Name: count, dtype: int64



```python
merged
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Crime ID</th>
      <th>Month</th>
      <th>Reported by</th>
      <th>Falls within</th>
      <th>Longitude</th>
      <th>Latitude</th>
      <th>Location</th>
      <th>LSOA code</th>
      <th>LSOA name</th>
      <th>Crime type</th>
      <th>Last outcome category</th>
      <th>Context</th>
      <th>source_month</th>
      <th>year_month</th>
      <th>year</th>
      <th>month</th>
      <th>outcome_bucket</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>fdbff4da55066884700d48f6ca48e03fbe8a75b6a143cc...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.849790</td>
      <td>52.590937</td>
      <td>On or near Walsall Road</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Public order</td>
      <td>Unable to prosecute suspect</td>
      <td>NaN</td>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
      <td>Unable to prosecute suspect</td>
    </tr>
    <tr>
      <th>1</th>
      <td>02affdeba7ab5d450fe62e72a2009d0cd796d5aa66f5a2...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.849790</td>
      <td>52.590937</td>
      <td>On or near Walsall Road</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Theft from the person</td>
      <td>Investigation complete; no suspect identified</td>
      <td>NaN</td>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
      <td>No further action</td>
    </tr>
    <tr>
      <th>2</th>
      <td>9e26f25dc8749749da9e14400236bec7f30872cfe75f9a...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.847123</td>
      <td>52.593864</td>
      <td>On or near Bramble Way</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Vehicle crime</td>
      <td>Investigation complete; no suspect identified</td>
      <td>NaN</td>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
      <td>No further action</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4026edb36cf730ba9ac2b5d7e385d3d20c26fb5c07927c...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.849790</td>
      <td>52.590937</td>
      <td>On or near Walsall Road</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Violence and sexual offences</td>
      <td>Unable to prosecute suspect</td>
      <td>NaN</td>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
      <td>Unable to prosecute suspect</td>
    </tr>
    <tr>
      <th>4</th>
      <td>452f4613d8c6ca1ba1bdd38b592641bb8d1aafcbf19ea9...</td>
      <td>2023-12</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-1.849790</td>
      <td>52.590937</td>
      <td>On or near Walsall Road</td>
      <td>E01009417</td>
      <td>Birmingham 001A</td>
      <td>Violence and sexual offences</td>
      <td>Investigation complete; no suspect identified</td>
      <td>NaN</td>
      <td>2023-12</td>
      <td>2023-12</td>
      <td>2023</td>
      <td>12</td>
      <td>No further action</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>668103</th>
      <td>4fed62c6e6c3579bc01cb5f3623992bcf2825529c3fdcf...</td>
      <td>2025-11</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-2.113522</td>
      <td>52.576576</td>
      <td>On or near Furnace Drive</td>
      <td>E01034314</td>
      <td>Wolverhampton 035I</td>
      <td>Violence and sexual offences</td>
      <td>Unable to prosecute suspect</td>
      <td>NaN</td>
      <td>2025-11</td>
      <td>2025-11</td>
      <td>2025</td>
      <td>11</td>
      <td>Unable to prosecute suspect</td>
    </tr>
    <tr>
      <th>668104</th>
      <td>f5f74239f3347800f98c17a760801d6f7d060eaa1c3393...</td>
      <td>2025-11</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-2.113522</td>
      <td>52.576576</td>
      <td>On or near Furnace Drive</td>
      <td>E01034314</td>
      <td>Wolverhampton 035I</td>
      <td>Violence and sexual offences</td>
      <td>Under investigation</td>
      <td>NaN</td>
      <td>2025-11</td>
      <td>2025-11</td>
      <td>2025</td>
      <td>11</td>
      <td>Under investigation</td>
    </tr>
    <tr>
      <th>668105</th>
      <td>cb777ddb507042d87ceb53caa0d73f194116c0924fb4be...</td>
      <td>2025-11</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-2.119472</td>
      <td>52.572273</td>
      <td>On or near Silver Birch Road</td>
      <td>E01034315</td>
      <td>Wolverhampton 035J</td>
      <td>Criminal damage and arson</td>
      <td>Investigation complete; no suspect identified</td>
      <td>NaN</td>
      <td>2025-11</td>
      <td>2025-11</td>
      <td>2025</td>
      <td>11</td>
      <td>No further action</td>
    </tr>
    <tr>
      <th>668106</th>
      <td>a5e7d87b8efcc67da75e0e9809aeb5c7b727422c849eae...</td>
      <td>2025-11</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-2.117890</td>
      <td>52.570935</td>
      <td>On or near Rowan Tree Drive</td>
      <td>E01034315</td>
      <td>Wolverhampton 035J</td>
      <td>Vehicle crime</td>
      <td>Investigation complete; no suspect identified</td>
      <td>NaN</td>
      <td>2025-11</td>
      <td>2025-11</td>
      <td>2025</td>
      <td>11</td>
      <td>No further action</td>
    </tr>
    <tr>
      <th>668107</th>
      <td>b2c71bab3a0573777e1a4b42122206cd51f47a7db34631...</td>
      <td>2025-11</td>
      <td>West Midlands Police</td>
      <td>West Midlands Police</td>
      <td>-2.118325</td>
      <td>52.573820</td>
      <td>On or near Pond Grove</td>
      <td>E01034315</td>
      <td>Wolverhampton 035J</td>
      <td>Violence and sexual offences</td>
      <td>Under investigation</td>
      <td>NaN</td>
      <td>2025-11</td>
      <td>2025-11</td>
      <td>2025</td>
      <td>11</td>
      <td>Under investigation</td>
    </tr>
  </tbody>
</table>
<p>668108 rows × 17 columns</p>
</div>




```python

```

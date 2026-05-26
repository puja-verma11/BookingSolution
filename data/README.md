# Data Directory

This folder is the **local simulation of a cloud storage bucket** (e.g. AWS S3).

In production, data files arrive here from upstream systems (SFTP, APIs, S3).
Airflow polls this folder every 30 minutes and processes any new files automatically.

---

## Supported File Formats

| Format  | Example                        |
|---------|--------------------------------|
| JSON    | `booking_data_europe.json`     |
| Parquet | `booking_data_japan.parquet`   |

---

## Expected Schema

### JSON format
```json
{
  "bookings": [
    {
      "booking_id": 1,
      "type": "hotel",
      "location": "Paris",
      "metadata": {
        "name": "Hotel Name",
        "category": "luxury",
        "rating": 4.5,
        "price": 200
      }
    }
  ]
}
```

### Parquet format (flat schema)
| Column     | Type    | Example        |
|------------|---------|----------------|
| booking_id | integer | 1              |
| type       | string  | hotel          |
| location   | string  | Paris          |
| name       | string  | Hotel Name     |
| category   | string  | luxury         |
| rating     | float   | 4.5            |
| price      | float   | 200            |

---

## Valid booking types
- `hotel`
- `attraction`
- `restaurant`
- `flight`

---

## Data Quality Rules

| Issue             | Handled by  | How                              |
|-------------------|-------------|----------------------------------|
| Duplicate records | PySpark     | ROW_NUMBER() dedup before load   |
| Null booking_id   | PySpark     | Split to `raw.bookings_invalid`  |
| Null type         | dbt staging | COALESCE to `'unknown'`          |
| Invalid price     | dbt staging | TRY_CAST, negative → 0           |
| Invalid rating    | dbt test    | WARN if > 5 or < 0               |

---

## Note
Actual data files are excluded from git (see `.gitignore`).
Drop any `.json` or `.parquet` file here and Airflow will process it on the next run.

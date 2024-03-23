# cloudstorage2bq

This project loads a set of parquet files in Google Cloud Storage Bucket into BigQuery.


Every directory in the given bucket and prefix is assumed to be a dataset you want to create.
Every file in a directory

Input schema:
```
example_dataset_name/table_name1.parquet
                     table_name2.parquet
                     table_name3.parquet
                     table_name4.parquet
```

Would create a new dataset called `example_dataset_name` and create table_name1, table_name2, 
and so on in that dataset.

You can supply an optional table_prefix if you'd like.


# Installation
Requirements:
* Rye - https://rye-up.com/

```
git clone ..
rye sync
```

## Usage

```
usage: cloudstorage2bq [-h] --bucket_name BUCKET_NAME [--bucket_prefix BUCKET_PREFIX] [--table_name_prefix TABLE_NAME_PREFIX]
```


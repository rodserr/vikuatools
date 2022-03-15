# Changelog

## v0.1.3 (15/03/2022)
- fix bug in `parse_properties` arguments, now al columns_ are *None* by default. Also extra arguments to define boolean dictionary and dt unit
- function to `get_mkt_email_stats` from hubspot

## v0.1.2 (9/03/2022)

- Add fulcrum dependency
- Include instagram module
- `load_table_from_dataframe_safely` now can receive full bigquery.LoadJobConfig() optional argument instead of just a squema definition
- `drop_duplicates` tries to convert ids to string in case TypeError

## v0.1.1 (5/03/2022)

- Fix ImportError in odoo module: remove split_column() from imports

## v0.1.0 (17/02/2022)

- First release of `vikuatools`!
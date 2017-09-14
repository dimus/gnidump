# Conversion scripts for migrating data from gni MySQL database to gnindex Postgres database

dump
: Takes names from MySQL database and forms CSV files for `restore` script

restore
: Imports CSV files to Postgres database

canonicals
: Creates canonicals files for gnindex's `matcher`

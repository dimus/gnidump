# gnidump

Converts database form `gni` to `gnindex`

## Prerequisites

### Environment variables

To connect to gni database it needs the following environment variables:

`DB_USER`
: mysql user for gni database

`DB_PASSWORD`
: mysql user password

`DB_HOST`
: host where database is located

`DB_PORT`
: database port (usually 3306)

`DB_DATABASE`
: database name (usually gni)

`WORKERS_NUMBER`
: Number of workers running concurrently

`PARSER_URL`
: A URL to a gnparser http service
## Example

```
DB_USER=root
DB_PASSWORD=secret
DB_HOST=127.0.0.1
DB_PORT=3306
DB_DATABASE=gni
WORKERS_NUMBER=4
PARSER_URL="http://parser.globalnames.org/api"
```

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
For exporting data to gnindex postgres database you need the following env
variables:

`GNINDEX_HOST`
: Postgres host

`GNINDEX_PORT`
: Postgres port

`GNINDEX_USERNAME`
: Postgres user

`GNINDEX_PASSWORD`
: Postgres password

`CANONICAL_DIR`
: Directory where to put text files with canonical names. It should end with '/'

## Usage

* Compile app

```bash
go build
```
to compile with build time and git hash use

```
go build -ldflags "-X main.buildstamp=`date -u '+%Y-%m-%d_%I:%M:%S%p'` \
  -X main.githash=`git rev-parse HEAD | cut -c1-7`"
```

* Move binary file to `/usr/local/bin`

```bash
sudo mv gnidump /usr/local/bin
```

Go to scripts directory and run

```bash
./dump
./restore
```

To see version run `gnidump version`

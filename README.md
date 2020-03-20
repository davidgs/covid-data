# Covid-data

## COVID-19 Data 

Read all the .csv data files from [Johns Hopkins Corona Virus Tracking Data](https://github.com/CSSEGISandData/COVID-19) into InfluxDB 2

## Usage

The environment file `.env` is read, if present, for any variables listed below.

The following ENV variables are checked:

```
INFLUX_TOKEN
INFLUX_BUCKET
INFLUX_ORG
INFLUX_MEASURE
INFLUX_URL
DATA_DIR
```
If present, they are used. They are over-ridden by any command-line flags given at runtime.

Usage:

        -dir:           Path to where the .csv data files live. REQUIRED
        -url:           URL of your InfluxDB server, including port. REQUIRED
        -bucket:        Bucket name -- default: $INFLUX_BUCKET, REQUIRED
        -organization:  Organization name -- default: $INFLUX_ORG, REQUIRED
        -measurement:   Measurement name -- default: $INFLUX_MEASURE, REQUIRED
        -token:         InfluxDB Token -- default: $INFLUX_TOKEN, REQUIRED

`$ go build covid.go`

`$ ./covid -dir path/to/data -bucket bucket_name -organization org_name -measurement measure_name -url http://your.server.com:9999 -token yourToken`

## Output

```
Scanning Data Directory:  ../../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports
Processing File:  ../../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/01-22-2020.csv
Processing File:  ../../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/01-23-2020.csv
Processing File:  ../../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/01-24-2020.csv
Processing File:  ../../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/01-25-2020.csv
Processing File:  ../../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/01-26-2020.csv
...
```

Data is written from those files to your InfluxDB instance.

## Saved Configuration

The last datafile processed is saved into a file called `.last`. On subsequent runs, only datafiles added **after** this last-processed file will be read and processed into InfluxDB.

## Geopsatial Data

Starting sometime in February the dataset started including geospatial data (lat/lng) with all the data. This is now also written to the InfluxDB instance. 

In addition, since InfluxDB now also supports using [s2 GeoHashes](https://s2geometry.io/devguide/s2cell_hierarchy.html), the s2 GeoHash is also written to the database at the same time. If there is no lat/lng data available, lat/lng is written as `0.00` and `0.00` respectively, and an empty-string is entered as the s2 GeoHash.
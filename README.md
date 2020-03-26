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
MAPS_TOKEN
```
If present, they are used. They over-ride any command-line flags given at runtime.

Usage:

        -d --dir:         Path to where the .csv data files live.
        -u --url:         URL of your InfluxDB server, including port.
        -b --bucket:      Bucket name -- default: $INFLUX_BUCKET
        -g --org:         Organization name -- default: $INFLUX_ORG
        -m --measurement: Measurement name -- default: $INFLUX_MEASURE
        -t --token:       InfluxDB Token -- default: $INFLUX_TOKEN
        -a --apitoken:    Google Maps API Token -- default: $MAPS_TOKEN
        -o --out          Output line-protocol to stdout
        -f --file         file to output line-protocol to (must use -o as well)

`$ go build covid.go`

`$ ./covid dir path/to/data -b bucket_name -g org_name measurement measure_name -url http://your.server.com:9999 -token yourToken`

Notice that you can use flags with or without the `-` or a `--` if you choose.

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
Data is read from those files and written directly to your InfluxDB instance.

If you are using the `-o` without an output file (output to `stdoud`) all program output is redirected to `stderr`. If this is not what you want, supply an output file via the -file flag.



## Batch Processing

By default (now) data is sent to InfluxDB in batches of 500 points/write. You can change this by changing the value of `BatchSize`. A `BatchSize` of 0 will write each result as it is read.

## Saved Configuration

The last processing time is saved into a file called `.last`. On subsequent runs, only datafiles added **after** this time will be read and processed into InfluxDB. The time is saved as a Unix timestamp.

## Geopsatial Data

Starting sometime in February the dataset started including geospatial data (lat/lng) with all the data. This is now also written to the InfluxDB instance.

In addition, since InfluxDB now also supports using [s2 GeoHashes](https://s2geometry.io/devguide/s2cell_hierarchy.html), the s2 GeoHash is also written to the database at the same time as a tag called `s2_cell_id`. If there is no lat/lng data available, lat/lng is written as `0.00` and `0.00` respectively, and an empty-string is entered as the s2 GeoHash tag.

If you've provided a Google Maps API token, the `Country` and `Province` data from the record is used to reverse-encode the location and add a rough lat/lng before the `s2_cell_id` is calculated. The new data format includes
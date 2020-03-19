# Covid-data

## covid 

read all the .csv files from [Johns Hopkins Corona Virus Tracking Data](https://github.com/CSSEGISandData/COVID-19) into InfluxDB 2

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
        -nosave:        Don't save env variables to the .env file

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

## Saved Environement

If the `-nosave` flag is **not** used, all environment variables are saaved to a `.env` file in the working directory. 

Also saved is the last datafile processed. On subsequent runs, only datafiles added **after** this last-processed file will be read and processed into InfluxDB.

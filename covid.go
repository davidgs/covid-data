package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb-client-go"
	"github.com/smartystreets/scanners/csv"
)

// Case Struct to hold the data for each line
type Case struct {
	Province   string `csv:"Province/State"`
	Country    string `csv:"Country/Region"`
	LastUpdate string `csv:"Last Update"`
	Confirmed  string `csv:"Confirmed"`
	Deaths     string `csv:"Deaths"`
	Recovered  string `csv:"Recovered"`
	Latitude   string `csv:"Latitude"`
	Longitude  string `csv:"Longitude"`
}

// RFC3339FullDate Most common date format in the files
const RFC3339FullDate = "2006-01-02T15:04:05"

// RFC3339OldDate Another date format used
const RFC3339OldDate = "1/2/2006 15:04"

// RFC3339BadDate yet a 3rd format used.
const RFC3339BadDate = "1/2/06 15:04"

// streamlined error handling
func check(e error) {
	if e != nil {
		log.Panic(e)
	}
}

// usage
func usage(e string) {
	fmt.Println("\nUsage:\n")
	fmt.Println("\t-dir:\t Path to where the .csv data files live. Default is . (current Directory)")
	fmt.Println("\t-url:\tURL of your InfluxDB server, including port. (default: http://localhos:9999)")
	fmt.Println("\t-bucket:\tBucket name -- no default, REQUIRED")
	fmt.Println("\t-organization:\tOrganization name -- no default, REQUIRED")
	fmt.Println("\t-measurement:\tMeasurement name -- no default, REQUIRED")
	fmt.Println("\t-token:\tInfluxDB Token -- no default, REQUIRED\n")
	log.Fatal(errors.New(e))
}

func main() {
	dir := flag.String("dir", ".", "Directory where the .csv files are")
	bucket := flag.String("bucket", "", "Bucket to store data in *REQUIRED")
	org := flag.String("organization", "", "Organization to store data in *REQUIRED")
	meas := flag.String("measurement", "", "Measurement to send data to *REQUIRED")
	token := flag.String("token", "", "Database Token *REQUIRED")
	url := flag.String("url", "http://localhost:9999", "URL of your InfluxDB 2 Instance")

	flag.Parse()
	// check that all required flags are given. Error if not.
	if *token == "" {
		usage("ERROR: Token is REQUIRED! Must Provide a valid Token")
	}
	if *url == "" {
		usage("ERROR: Database URL is REQUIRED! Must Provide a valid URL")
	}
	if *org == "" {
		usage("ERROR: Organization is REQUIRED! Must Provide an Organization")
	}
	if *bucket == "" {
		usage("ERROR: Bucket is REQUIRED! Must Provide a Bucket")
	}
	if *meas == "" {
		usage("ERROR: Measurement is REQUIRED! Must Provide a Measurement")
	}

	// scan the data directory for all files, and order them by date.
	fmt.Println("Scanning Data Directory: ", *dir)
	dirname, err := os.Open(*dir)
	check(err)
	files, err := dirname.Readdir(0)
	check(err)
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime().Before(files[j].ModTime())
	})
	// new InfluxDB client.
	influx, err := influxdb.New(*url, *token)
	check(err)
	defer influx.Close()

	// go through each file in the list and process it.
	for _, fs := range files {
		if !fs.IsDir() {
			if strings.HasSuffix(fs.Name(), ".csv") { // only .csv files
				fmt.Println("Processing File: ", *dir+"/"+fs.Name())
				f := *dir + "/" + fs.Name()
				dataFile, err := os.OpenFile(f, os.O_RDWR, os.ModePerm)
				check(err)
				defer dataFile.Close()

				_, err = dataFile.Seek(0, 0)
				check(err)

				newReader := bufio.NewReader(dataFile)
				scanner, err := csv.NewStructScanner(newReader)
				check(err)

				for scanner.Scan() {
					var Case Case
					var confirmed int
					var dead int
					var recovered int
					var latitude float64
					var longitude float64

					err := scanner.Populate(&Case)
					check(err)
					var t time.Time
					// nested date processing because of format changes
					t, err = time.Parse(RFC3339FullDate, Case.LastUpdate)
					if err != nil {
						t, err = time.Parse(RFC3339OldDate, Case.LastUpdate)
						if err != nil {
							t, err = time.Parse(RFC3339BadDate, Case.LastUpdate)
							if err != nil {
								log.Panic(err)
							}
						}
					}
					// validate data a bit ...
					if Case.Confirmed != "" {
						confirmed, err = strconv.Atoi(Case.Confirmed)
						check(err)
					} else {
						confirmed = 0
					}
					if Case.Deaths != "" {
						dead, err = strconv.Atoi(Case.Deaths)
						check(err)
					} else {
						dead = 0
					}
					if Case.Recovered != "" {
						recovered, err = strconv.Atoi((Case.Recovered))
						check(err)
					} else {
						recovered = 0
					}
					if Case.Latitude != "" {
						latitude, err = strconv.ParseFloat(Case.Latitude, 64)
						check(err)
					} else {
						latitude = 0.00
					}
					if Case.Longitude != "" {
						longitude, err = strconv.ParseFloat(Case.Longitude, 64)
						check(err)
					} else {
						longitude = 0.00
					}
					Case.Province = strings.TrimRight(Case.Province, `"`)
					myMetrics := []influxdb.Metric{
						influxdb.NewRowMetric(

							map[string]interface{}{"confirmed": confirmed,
								"deaths":    dead,
								"recovered": recovered,
								"lat":       latitude,
								"lon":       longitude},
							*meas,
							map[string]string{"state_province": Case.Province, "country_region": Case.Country},
							t),
					}
					// write the data to the database.
					_, err = influx.Write(context.Background(), *bucket, *org, myMetrics...)
					check(err)

				}
				err = scanner.Error()
				check(err)
				dataFile.Close()
			}
		}
	}
	influx.Close()

}

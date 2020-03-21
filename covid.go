package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/golang/geo/s2"
	"github.com/influxdata/influxdb-client-go"
	"github.com/joho/godotenv"
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
		log.Fatal(e)
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
	fmt.Println("\t-nosave:\t Don't save ENV variables to .env file (default false)")
	check(errors.New(e))
}

func filterFiles(dir, suffix string, before int64) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := []string{}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), suffix) && f.ModTime().Unix() > before {
			res = append(res, filepath.Join(dir, f.Name()))
		}
	}
	return res, nil
}

func main() {

	// read environment file, if it exislastTime
	_, err := os.Stat("./.env")
	// check if not exislastTime
	if !os.IsNotExist(err) {
		check(godotenv.Load(".env"))
	}
	// read last run time, if it exislastTime
	_, err = os.Stat("./.last")
	// check if not exislastTime
	if !os.IsNotExist(err) {
		check(godotenv.Load(".last"))
	}

	dirPtr := flag.String("dir", "", "Directory where the .csv files are")
	bucketPtr := flag.String("bucket", "", "Bucket to store data in *REQUIRED")
	orgPtr := flag.String("organization", "", "Organization to store data in *REQUIRED")
	measPtr := flag.String("measurement", "", "Measurement to send data to *REQUIRED")
	tokenPtr := flag.String("token", "", "Database Token *REQUIRED")
	urlPtr := flag.String("url", "", "URL of your InfluxDB 2 Instance")

	dir := os.Getenv("DATA_DIR")
	bucket := os.Getenv("INFLUX_BUCKET")
	org := os.Getenv("INFLUX_ORG")
	meas := os.Getenv("INFLUX_MEASURE")
	token := os.Getenv("INFLUX_TOKEN")
	url := os.Getenv("INFLUX_URL")
	lastFile := os.Getenv("LAST_RUN")

	flag.Parse()
	// command-line flags over-ride ENV variables
	if token == "" {
		token = *tokenPtr
	}
	if bucket == "" {
		bucket = *bucketPtr
	}
	if org == "" {
		org = *orgPtr
	}
	if meas == "" {
		meas = *measPtr
	}
	if token == "" {
		token = *tokenPtr
	}
	if url == "" {
		url = *urlPtr
	}
	if dir == "" {
		dir = *dirPtr
	}
	// check that all required flags are given. Error if not.
	if token == "" {
		usage("ERROR: Token is REQUIRED! Must Provide a valid Token")
	}
	if url == "" {
		usage("ERROR: Database URL is REQUIRED! Must Provide a valid URL")
	}
	if org == "" {
		usage("ERROR: Organization is REQUIRED! Must Provide an Organization")
	}
	if bucket == "" {
		usage("ERROR: Bucket is REQUIRED! Must Provide a Bucket")
	}
	if meas == "" {
		usage("ERROR: Measurement is REQUIRED! Must Provide a Measurement")
	}
	if dir == "" {
		usage("ERROR: Data Directory is REQUIRED! Must Provide a Measurement")
	}

	fmt.Println("Using Values:\n")
	fmt.Println("\tOrganization: \t ", org)
	fmt.Println("\tBucket: \t ", bucket)
	fmt.Println("\tMeasurement: \t ", meas)
	fmt.Println("\tURL: \t\t ", url)
	fmt.Printf("\tData Directory:  %s\n\n", dir)

	// scan the data directory for all files, and order them by date.
	fmt.Println("Scanning Data Directory: ", dir)
	// dirname, err := os.Open(dir)
	// check(err)
	// files, err := dirname.Readdir(0)
	// check(err)

	// if we have never run it, set the date to
	// before the oldest data file. Otherwise,
	// set it to the date we last ran.
	lastTime, err := time.Parse(RFC3339FullDate, "2020-01-01T00:00:00")
	check(err)
	if lastFile != "" {
		lf, err := strconv.Atoi(lastFile)
		check(err)
		lastTime = time.Unix(int64(lf), 0)
	}
	files, err := filterFiles(dir, ".csv", lastTime.Unix())
	check(err)
	if len(files) == 1 {
	fmt.Printf("Processing %d data file.\n", len(files))
	} else if len(files) == 0 {
		fmt.Printf("No new data files to process.\n\n")
		os.Exit(0)
	} else {
		fmt.Printf("Processing %d data files.\n", len(files))
	}
	// new InfluxDB client.
	influx, err := influxdb.New(url, token)
	check(err)
	defer influx.Close()

	// go through each file in the list and process it.
	for _, fs := range files {


		fmt.Println("Processing File: ", fs)

		dataFile, err := os.OpenFile(fs, os.O_RDWR, os.ModePerm)
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
					check(err)
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
			var ll s2.LatLng
			var cellID s2.CellID
			if latitude != 0.00 && longitude != 0.00 {
				ll = s2.LatLngFromDegrees(latitude, longitude)
			}
			if ll.IsValid() {
				cellID = s2.CellIDFromLatLng(ll)
			}
			var cell = ""
			if cellID.IsValid() {
				cell = cellID.ToToken()
			}
			if cell == "1000000000000001" {
				cell = ""
			}
			Case.Province = strings.TrimRight(Case.Province, `"`)
			myMetrics := []influxdb.Metric{
				influxdb.NewRowMetric(

					map[string]interface{}{"confirmed": confirmed,
						"deaths":    dead,
						"recovered": recovered,
						"lat":       latitude,
						"lon":       longitude},
					meas,
					map[string]string{"state_province": Case.Province, "country_region": Case.Country, "s2_cell_id": cell},
					t),
			}
			// write the data to the database.
			_, err = influx.Write(context.Background(), bucket, org, myMetrics...)
			check(err)

		}
		err = scanner.Error()
		check(err)
		dataFile.Close()

	}
	influx.Close()
	_, err = os.Stat("./.last")

	// create file if not exislastTime
	if os.IsNotExist(err) {
		_, err = os.Create("./.last")
		check(err)

	}
	envFile, err := os.OpenFile("./.last", os.O_RDWR|os.O_TRUNC, os.ModePerm)
	check(err)
	defer envFile.Close()

	_, err = envFile.Seek(0, 0)
	check(err)
	newWriter := bufio.NewWriter(envFile)
	_, err = newWriter.WriteString("LAST_RUN=" + strconv.FormatInt(time.Now().Unix(), 10) + "\n")
	check(newWriter.Flush())
	check(envFile.Close())

}

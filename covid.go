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
	"googlemaps.github.io/maps"
)

// Case Struct to hold the data for each line
type Case struct {
	FIPS        string `csv:"FIPS"`
	Admin2      string `csv:"Admin2"`
	Province2   string `csv:"Province_State"`
	Province    string `csv:"Province/State"`
	Country     string `csv:"Country/Region"`
	Country2    string `csv:"Country_Region"`
	LastUpdate  string `csv:"Last Update"`
	LastUpdate2 string `csv:"Last_Update"`
	Confirmed   string `csv:"Confirmed"`
	Deaths      string `csv:"Deaths"`
	Recovered   string `csv:"Recovered"`
	Latitude    string `csv:"Latitude"`
	Lat         string `csv:"Lat"`
	Longitude   string `csv:"Longitude"`
	Long        string `csv:"Long_"`
	Combined    string `csv:"Combined_Key"`
}

// keep track of the total run time
var start time.Time

// RFC3339NewDate newest date format
const RFC3339NewDate = "2006-01-02 15:04:05"

// RFC3339FullDate Most common date format in the files
const RFC3339FullDate = "2006-01-02T15:04:05"

// RFC3339OldDate Another date format used
const RFC3339OldDate = "1/2/2006 15:04"

// RFC3339BadDate yet a 3rd format used.
const RFC3339BadDate = "1/2/06 15:04"

// RFC3339FileDate yet a 3rd format used.
const RFC3339FileDate = "01-02-2006"

// BatchSize size of the batches to write
const BatchSize = 500

// streamlined error handling
func check(e error) {
	if e != nil {
		runtime()
		log.Fatal(e)
	}
}

// usage
func usage(e string) {

	fmt.Println("\nUsage:")
	fmt.Println("\t-dir:\t Path to where the .csv data files live. Default is . (current Directory)")
	fmt.Println("\t-url:\tURL of your InfluxDB server, including port. (default: http://localhos:9999)")
	fmt.Println("\t-bucket:\tBucket name -- no default, REQUIRED")
	fmt.Println("\t-organization:\tOrganization name -- no default, REQUIRED")
	fmt.Println("\t-measurement:\tMeasurement name -- no default, REQUIRED")
	fmt.Println("\t-token:\tInfluxDB Token -- no default, REQUIRED")
	fmt.Println("\t-gtoken:\tGoogle Maps API Token if you want to reverse-encode missing location data")
	fmt.Println("")
	check(errors.New(e))
}

// filter the files to make sure we only get .csv files, and only after the 'last run' time
func filterFiles(dir, suffix string, before int64) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := []string{}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), suffix) {
			n := strings.TrimSuffix(f.Name(), ".csv")
			d, err := time.Parse(RFC3339FileDate, n)
			check(err)
			if f.ModTime().Unix() > before && d.Unix() > before {
				res = append(res, filepath.Join(dir, f.Name()))
			}
		}
	}
	return res, nil
}

// how long the process took
func runtime() {
	t1 := time.Now()
	// Get duration.
	d := t1.Sub(start)
	fmt.Print("Total Runtime ")
	if int64(d.Hours()) > 0 {
		fmt.Printf("%0.0f Hours, ", d.Hours())
		fmt.Printf("%0.0f Minutes, ", d.Minutes()/60)
		secs := d.Seconds() / 60.00
		fmt.Printf("%0.2f Seconds\n", secs)
		return
	}
	if int64(d.Minutes()) > 0.00 {
		fmt.Printf("%0.0f Minutes, ", d.Minutes())
		secs := d.Seconds() / 60.00
		fmt.Printf("%0.2f Seconds\n", secs)
		return
	}
	if int64(d.Seconds()) > 0 {
		fmt.Printf("%0.2f Seconds ", d.Seconds())
	} else {
		fmt.Printf("%d Milliseconds\n", d.Milliseconds())
	}
}

func main() {
	start = time.Now()
	// read environment file, if it exists
	_, err := os.Stat("./.env")
	// check if not exist
	if !os.IsNotExist(err) {
		check(godotenv.Load(".env"))
	}
	// read last run time, if it exists
	_, err = os.Stat("./.last")
	// check if not exists
	if !os.IsNotExist(err) {
		check(godotenv.Load(".last"))
	}

	dirPtr := flag.String("dir", "", "Directory where the .csv files are")
	bucketPtr := flag.String("bucket", "", "Bucket to store data in *REQUIRED")
	orgPtr := flag.String("organization", "", "Organization to store data in *REQUIRED")
	measPtr := flag.String("measurement", "", "Measurement to send data to *REQUIRED")
	tokenPtr := flag.String("token", "", "Database Token *REQUIRED")
	urlPtr := flag.String("url", "", "URL of your InfluxDB 2 Instance")
	gtokePtr := flag.String("gtoken", "", "Google Maps API Token, if desired")

	dir := os.Getenv("DATA_DIR")
	bucket := os.Getenv("INFLUX_BUCKET")
	org := os.Getenv("INFLUX_ORG")
	meas := os.Getenv("INFLUX_MEASURE")
	token := os.Getenv("INFLUX_TOKEN")
	url := os.Getenv("INFLUX_URL")
	gtoke := os.Getenv("MAPS_TOKEN")
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
	if gtoke == "" {
		gtoke = *gtokePtr
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
	fmt.Println("Using Values:")
	fmt.Println("\tOrganization: \t ", org)
	fmt.Println("\tBucket: \t ", bucket)
	fmt.Println("\tMeasurement: \t ", meas)
	fmt.Println("\tURL: \t\t ", url)
	if gtoke != "" {
		fmt.Println("\tGeoLocating:\t  Using Google Maps Geolocations")
	} else {
		fmt.Println("\tGeoLocating:  non-geo-tagged data will not be geolocated")
	}
	fmt.Println("\tLast run:\t  " + lastTime.Local().String())
	fmt.Printf("\tData Directory:   %s\n\n", dir)

	// scan the data directory for all files.
	fmt.Println("Scanning Data Directory: ", dir)
	files, err := filterFiles(dir, ".csv", lastTime.Unix())
	check(err)
	//files := []string{"../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/02-01-2020.csv"}
	if len(files) == 1 {
		fmt.Printf("Processing %d data file.\n", len(files))
	} else if len(files) == 0 {
		fmt.Printf("No new data files to process.\n\n")
		runtime()
		os.Exit(0)
	} else {
		fmt.Printf("Processing %d data files.\n", len(files))
	}

	var gClient *maps.Client
	if gtoke != "" {
		gClient, err = maps.NewClient(maps.WithAPIKey(gtoke))
		check(err)
	}
	myMetrics := []influxdb.Metric{}
	batchCount := 0
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
			var tTime string
			// nested date processing because of format changes
			if Case.LastUpdate != "" {
				tTime = Case.LastUpdate
			} else {
				tTime = Case.LastUpdate2
			}
			stringTime := decipherTime(tTime)

			var ll s2.LatLng
			if Case.FIPS == "" {
				// validate data a bit ...
				if Case.Province != "" {
					Case.Province = strings.ReplaceAll(Case.Province, `"`, ``)
				}
				if Case.Country == "Mainland China" {
					Case.Country = "China"
				}
				if Case.Country == "Viet Nam" {
					Case.Country = "Vietnam"
				}
				if Case.Country == "Korea, South" {
					Case.Country = "South Korea"
				}
				if Case.Country == "Hong Kong SAR" || Case.Country == "Hong Kong" {
					Case.Country = "Hong Kong"
					Case.Province = ""
				}
				if Case.Country == "Macau SAR" || Case.Country == "Macau" {
					Case.Country = "Macao"
					Case.Province = ""
				}
				if Case.Country == "Ivory Coast" {
					Case.Country = "Côte d'Ivoire"
				}
				if Case.Country == "North Ireland" {
					Case.Province = "Northern Ireland"
					Case.Country = "UK"
				}
				Case.Province = strings.ReplaceAll(Case.Province, `"`, ``)
				Case.Country = strings.ReplaceAll(Case.Country, `"`, ``)
				fail := strings.Contains(strings.ToLower(Case.Country), strings.ToLower("Diamond"))
				fail = strings.Contains(strings.ToLower(Case.Country), strings.ToLower("Cruise"))
				fail = strings.Contains(strings.ToLower(Case.Country), strings.ToLower("others"))
				//	fail = strings.Contains(strings.ToLower(Case.Province+" "+Case.Country), strings.ToLower("Other"))
				if Case.Latitude != "" {
					latitude, err = strconv.ParseFloat(Case.Latitude, 64)
					check(err)
					longitude, err = strconv.ParseFloat(Case.Longitude, 64)
					check(err)

				} else if gClient != nil && !fail {
					ll = geoCode(gClient, Case.Country, Case.Province, "")
					latitude = ll.Lat.Degrees()
					longitude = ll.Lng.Degrees()
				} else {
					latitude = 0.00
					longitude = 0.00
				}
			} else { // Case2
				if Case.Province2 != "" {
					Case.Province2 = strings.ReplaceAll(Case.Province2, `"`, ``)
				}
				if Case.Country2 == "Mainland China" {
					Case.Country2 = "China"
				}
				if Case.Country2 == "Viet Nam" {
					Case.Country2 = "Vietnam"
				}
				if Case.Country2 == "Korea, South" {
					Case.Country2 = "South Korea"
				}
				if Case.Country2 == "Hong Kong SAR" || Case.Country2 == "Hong Kong" {
					Case.Country2 = "Hong Kong"
					Case.Province = ""
				}
				if Case.Country2 == "Macau SAR" || Case.Country2 == "Macau" {
					Case.Country2 = "Macao"
					Case.Province2 = ""
				}
				if Case.Country2 == "Ivory Coast" {
					Case.Country2 = "Côte d'Ivoire"
				}
				if Case.Country2 == "North Ireland" {
					Case.Province2 = "Northern Ireland"
					Case.Country2 = "UK"
				}

				Case.Province2 = strings.ReplaceAll(Case.Province2, `"`, ``)
				Case.Country2 = strings.ReplaceAll(Case.Country2, `"`, ``)
				fail := strings.Contains(strings.ToLower(Case.Country2), strings.ToLower("Diamond"))
				fail = strings.Contains(strings.ToLower(Case.Country2), strings.ToLower("Cruise"))
				fail = strings.Contains(strings.ToLower(Case.Country2), strings.ToLower("others"))
				//	fail = strings.Contains(strings.ToLower(Case.Province2+" "+Case.Country2), strings.ToLower("Other"))

				if Case.Lat != "" {
					latitude, err = strconv.ParseFloat(Case.Lat, 64)
					check(err)
					longitude, err = strconv.ParseFloat(Case.Long, 64)
					check(err)
				} else if gClient != nil && !fail {
					ll = geoCode(gClient, Case.Country2, Case.Province2, Case.Admin2)
					latitude = ll.Lat.Degrees()
					longitude = ll.Lng.Degrees()
				} else {
					latitude = 0.00
					longitude = 0.00
				}

			} // end case2
			// fix any data issues ...
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
			cell := ""
			if ll.Lat != 0.00 && ll.Lng != 0.00 {
				cell = getS2Id(ll)
			}
			if Case.FIPS == "" {

				//myMetrics = []influxdb.Metric{
				row := influxdb.NewRowMetric(

					map[string]interface{}{"confirmed": confirmed,
						"deaths":    dead,
						"recovered": recovered,
						"lat":       latitude,
						"lon":       longitude},
					meas,
					map[string]string{
						"state_province": Case.Province,
						"country_region": Case.Country,
						"s2_cell_id":     cell,
						"last_update":    stringTime,
					},
					t)
				myMetrics = append(myMetrics, row)
				batchCount++
				//}
			} else {
				//myMetrics = []influxdb.Metric{
				row := influxdb.NewRowMetric(

					map[string]interface{}{
						"confirmed": confirmed,
						"deaths":    dead,
						"recovered": recovered,
						"lat":       latitude,
						"lon":       longitude},
					meas,
					map[string]string{
						"state_province": Case.Province2,
						"country_region": Case.Country2,
						"fips":           Case.FIPS,
						"last_update":    stringTime,
						"s2_cell_id":     cell,
						"combined_tag":   Case.Combined,
					},
					t)
				//}
				myMetrics = append(myMetrics, row)
				batchCount++
			}
			// write the data to the database.
			if batchCount > BatchSize {
				// new InfluxDB client.
				influx, err := influxdb.New(url, token)
				check(err)
				defer influx.Close()
				_, err = influx.Write(context.Background(), bucket, org, myMetrics...)
				check(err)
				batchCount = 0
				influx.Close()
			}

		}
		err = scanner.Error()
		check(err)
		dataFile.Close()

	}

	finish()

}

func getS2Id(latlng s2.LatLng) string {
	var cellID s2.CellID
	if latlng.IsValid() {
		cellID = s2.CellIDFromLatLng(latlng)
	}
	var cell = ""
	if cellID.IsValid() {
		cell = cellID.ToToken()
	}
	if cell == "1000000000000001" {
		fmt.Printf("S2 encoding failed for lat: %0.5f lng %0.5f\n", latlng.Lat, latlng.Lng)
		return ""
	}
	return cell
}

func parseLatLng(latlng string, r *maps.GeocodingRequest) {
	if latlng != "" {
		l := strings.Split(latlng, ",")
		lat, err := strconv.ParseFloat(l[0], 64)
		if err != nil {
			log.Fatalf("Couldn't parse latlng: %#v", err)
		}
		lng, err := strconv.ParseFloat(l[1], 64)
		if err != nil {
			log.Fatalf("Couldn't parse latlng: %#v", err)
		}
		r.LatLng = &maps.LatLng{
			Lat: lat,
			Lng: lng,
		}
	}
}

func finish() {
	_, err := os.Stat("./.last")

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
	runtime()
}
func geoCode(client *maps.Client, country string, province string, admin string) s2.LatLng {
	var address = make(map[maps.Component]string)

	if country != "" {
		address[maps.ComponentCountry] = country
	}

	var p = province
	if strings.Contains(strings.ToLower(p), strings.ToLower("diamond")) || strings.Contains(strings.ToLower(p), strings.ToLower("cruise")) {
		p = ""
	}
	if strings.Contains(strings.ToLower(p), "none") {
		p = ""
	}
	if admin != "" {
		address[maps.ComponentAdministrativeArea] = p + "|" + admin
	} else {
		address[maps.ComponentAdministrativeArea] = p
	}
	var r = &maps.GeocodingRequest{}
	// oh FFS, google maps only understands the country-code for Georgia.

	if country == "Georgia" {
		address[maps.ComponentCountry] = "GE"
		r = &maps.GeocodingRequest{
			Address:    "GE",
			Components: address,
			Language:   "english",
		}
	} else {
		r = &maps.GeocodingRequest{
			Components: address,
			Language:   "english",
		}
	}
	resp, err := client.Geocode(context.TODO(), r)
	check(err)
	if len(resp) < 1 {
		fmt.Printf("FAILED: Province:\t%s\t Country:\t%s\n", province, country)
		return s2.LatLngFromDegrees(0.00, 0.00)
	}
	return s2.LatLngFromDegrees(resp[0].Geometry.Location.Lat, resp[0].Geometry.Location.Lng)

}

func decipherTime(myTime string) string {
	t, err := time.Parse(RFC3339NewDate, myTime)
	if err != nil {
		t, err = time.Parse(RFC3339FullDate, myTime)
		if err != nil {
			t, err = time.Parse(RFC3339OldDate, myTime)
			if err != nil {
				t, err = time.Parse(RFC3339BadDate, myTime)
				check(err)
			}
		}
	}
	return t.String()
}

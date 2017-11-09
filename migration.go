package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/uttamgandhi24/whisper-go/whisper"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func usage() {
	log.Fatal(`
		migration.go -wspinfo -wspPath=whisper folder"

		OR

		migration.go -option=TSMW -wspPath=whisper folder
		-influxDataDir=influx data folder
		-from=<2015-11-01> -until=<2015-12-30> -dbname=migrated
		-retentionPolicy=default -tagconfig=config.json

		OR

		migration.go -option=ClientV2 -wspPath=whisper folder
		-from=<2015-11-01> -until=<2015-12-30> -dbname=migrated -host=http://localhost
		-port=8086, -retentionPolicy=default -tagconfig=config.json -username=<username>,
		-password=<password>`)
}

type ShardInfo struct {
	id    json.Number
	from  time.Time
	until time.Time
}

type MigrationData struct {
	option          string
	influxDataDir   string
	from            time.Time
	until           time.Time
	dbName          string
	wspFiles        []string
	shards          []ShardInfo
	tagConfigs      []TagConfig
	whisperFileSize int64
	tsmFileSize     int64
	retentionPolicy string
	host            string
	port            string
	username        string
	password        string
}

type TsmPoint struct {
	key    string
	values []tsm1.Value
}

type TagKeyValue struct {
	Tagkey   string `json:"tagkey"`
	Tagvalue string `json:"tagvalue"`
}

type TagConfig struct {
	Pattern     string        `json:"pattern"`
	Measurement string        `json:"measurement"`
	Tags        []TagKeyValue `json:"tags"`
	Field       string        `json:"field"`
}

type MTF struct {
	Measurement string
	Tags        []TagKeyValue
	Field       string
}

func main() {
	var (
		option          = flag.String("option", "NULL", "Use TSMWriter or ClientV2 for migration")
		wspPath         = flag.String("wspPath", "NULL", "Whisper files folder path")
		influxDataDir   = flag.String("influxDataDir", "NULL", "InfluxDB data directory")
		from            = flag.String("from", "NULL", "from date in YYYY-MM-DD format")
		until           = flag.String("until", "NULL", "until date in YYYY-MM-DD format")
		dbName          = flag.String("dbname", "migrated", "Database name (default: migrated")
		tagConfigFile   = flag.String("tagconfig", "NULL", "Configuration file for measurement and tags")
		retentionPolicy = flag.String("retentionPolicy", "default", "Retention Policy")
		host            = flag.String("host", "http://localhost", "Host name where influxdb is running")
		port            = flag.String("port", "8086", "Port on which influxdb is running")
		username        = flag.String("username", "NULL", "Username for influxdb auth")
		password        = flag.String("password", "NULL", "Password for influxdb auth")
		wspinfo         = flag.Bool("wspinfo", false, "Whisper file information")
	)
	flag.Parse()

	//Handle whisper information menu
	if *wspinfo == true {
		if *wspPath == "NULL" {
			usage()
		}
		migrationData := &MigrationData{}

		migrationData.FindWhisperFiles(*wspPath)
		if len(migrationData.wspFiles) == 0 {
			fmt.Println("No Whisper files found")
			return
		}
		if err := migrationData.GetWhisperInfo(); err != nil {
			fmt.Println("Error in Getting Whisper File information")
		}
		return
	}

	//Handle mandatory parameters
	if *option == "NULL" || *wspPath == "NULL" || *from == "NULL" ||
		*tagConfigFile == "NULL" {
		usage()
	}

	// InfluxDataDir is mandatory for TSMW option
	if *option == "TSMW" && *influxDataDir == "NULL" {
		usage()
	}

	migrationData := &MigrationData{
		option:          *option,
		dbName:          *dbName,
		influxDataDir:   *influxDataDir,
		retentionPolicy: *retentionPolicy,
		host:            *host,
		port:            *port,
		username:        *username,
		password:        *password,
	}

	var err error
	migrationData.from, err = time.Parse("2006-01-02", *from)

	if err != nil {
		log.Fatal("Error in parsing from ")
	}

	if *until != "NULL" {
		migrationData.until, err = time.Parse("2006-01-02", *until)
		if err != nil {
			log.Fatal("Error in parsing until ")
		}
	} else {
		migrationData.until = time.Now()
	}

	if err := migrationData.ReadTagConfig(*tagConfigFile); err != nil {
		fmt.Printf("Error in Parsing the Config file : %s\n", err)
		return
	}
	migrationData.FindWhisperFiles(*wspPath)
	if len(migrationData.wspFiles) == 0 {
		fmt.Println("No Whisper files found")
		return
	}
	migrationData.PreviewMTF()
	//Update the config file
	migrationData.WriteConfigFile(*tagConfigFile)
	//After the preview, confirm if the user wants to migrate data
	var userInput string
	fmt.Println("Do you want to continue the migration? Yes/No :")
	fmt.Scanf("%s", &userInput)
	if strings.ToUpper(userInput) != "YES" {
		return
	}
	timestart := time.Now()
	// Create shards for given time ranges
	if migrationData.option != "TSMW" {
		migrationData.WriteUsingV2()
	} else {
		err := migrationData.CreateShards()
		if err != nil {
			fmt.Println(err)
			return
		}
		//Map WSP to TSM
		err = migrationData.MapWSPToTSMByShard()
		if err != nil {
			fmt.Println("Mapping Whisper to TSM by Shard failed : %s\n", err)
			return
		}
	}
	timeend := time.Now()
	migrationData.PrintSummary(timeend.Sub(timestart).String())
}

// Read the config file and populate migrartionData.tagConfigs
func (migrationData *MigrationData) ReadTagConfig(filename string) error {
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	return json.Unmarshal(raw, &migrationData.tagConfigs)
}

//Write migrationData.tagConfigs to file
func (migrationData *MigrationData) WriteConfigFile(filename string) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("File Open Error")
		return
	}
	configStr, _ := json.MarshalIndent(migrationData.tagConfigs, "", "  ")
	_, err = f.WriteString(string(configStr))
	if err != nil {
		fmt.Println("Write Error")
		return
	}
	f.Close()
}

// Creates new config as per user's input
func NewConfig(wspFile string) *TagConfig {
	newTagConfig := &TagConfig{}
	fmt.Println("-------------------------------------------------------")
	fmt.Println("Tag config not found for", wspFile, `
		You will be prompted to enter Pattern, Measurement, Tags and Field`)
	fmt.Println("-------------------------------------------------------")
	fmt.Println(`Please enter pattern e.g. carbon.agents.#TEXT1.#TEXT2.#TEXT3
		Look at the migration_config.json for more examples->`)

	fmt.Scanf("%s", &newTagConfig.Pattern)
	if len(newTagConfig.Pattern) == 0 {
		return nil
	}
	fmt.Println(`Please enter measurement e.g. #TEXT3 ,\n#TEXT3 will be replaced
		with actual value->`)
	fmt.Scanf("%s", &newTagConfig.Measurement)
	if len(newTagConfig.Measurement) == 0 {
		return nil
	}

	fmt.Println(`Please enter tags e.g. host=#TEXT1 loc=#TEXT2
		\n host and loc are the tag keys and #TEXT1, #TEXT2 will be replaced
		actual tag values->`)

	var tagDataStr string
	fmt.Scanf("%s", &tagDataStr)

	if len(tagDataStr) != 0 {
		tagDataStrings := strings.Split(tagDataStr, " ")
		var tagKeyValue TagKeyValue
		newTagConfig.Tags = make([]TagKeyValue, len(tagDataStrings))
		for i := 0; i < len(tagDataStrings); i++ {
			tagKeyValueStr := strings.Split(tagDataStrings[i], "=")
			if len(tagKeyValueStr) < 2 {
				continue
			}
			tagKeyValue.Tagkey = strings.Trim(tagKeyValueStr[0], " ")
			tagKeyValue.Tagvalue = strings.Trim(tagKeyValueStr[1], " ")
			newTagConfig.Tags[i] = tagKeyValue
		}
	}

	fmt.Println(`Please enter Field e.g. value->`)
	fmt.Scanf("%s", &newTagConfig.Field)

	fmt.Println("You have entered->")
	fmt.Println("Pattern:", newTagConfig.Pattern)
	fmt.Println("Measurement:", newTagConfig.Measurement)
	fmt.Println("Tags:", newTagConfig.Tags)
	fmt.Println("Field:", newTagConfig.Field)
	fmt.Println("Do you want to add this pattern?(Yes/No):")
	var confirmPattern string
	fmt.Scanf("%s", &confirmPattern)
	if strings.ToUpper(confirmPattern) == "YES" {
		return newTagConfig
	} else {
		return nil
	}
}

// Find all whisper files from a given wspPath
func (migrationData *MigrationData) FindWhisperFiles(searchDir string) {

	fileList := []string{}
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if os.IsNotExist(err) { //search dir does not exist
			return nil
		}
		if strings.HasSuffix(f.Name(), "wsp") {
			fileList = append(fileList, path)
			migrationData.whisperFileSize = migrationData.whisperFileSize + f.Size()
		}
		return nil
	})
	if err != nil {
		fmt.Println("Err")
	}
	migrationData.wspFiles = fileList
}

// Gives a preview how the measurements, tags and fields look like for given
// whisper files and config file. Also will take input for new config if does
// not exist already for a given pattern
func (migrationData *MigrationData) PreviewMTF() {
	for _, wspFile := range migrationData.wspFiles {
		var tagConfig *TagConfig
		var mtf *MTF
		if mtf = migrationData.GetMTF(wspFile); mtf != nil {
		} else { //Create and add the pattern
			for {
				tagConfig = NewConfig(wspFile)
				if tagConfig != nil {
					break
				}
			}
			migrationData.tagConfigs = append(migrationData.tagConfigs, *tagConfig)

			mtf = &MTF{Measurement: tagConfig.Measurement, Tags: tagConfig.Tags,
				Field: tagConfig.Field}
		}
		key := CreateTSMKey(mtf)
		fmt.Println("\nWhisper File", wspFile, "\nTSM Key->", key)
	}
}

/*

 Create shards for given time range, shards should be created before the tsm
 data can be written
 The function writes point for each day and then drops the database.
 The shards remain even if the database is dropped
*/

func (migrationData *MigrationData) CreateShards() error {
	c, _ := client.NewHTTPClient(client.HTTPConfig{
		Addr: migrationData.host + ":" + migrationData.port,
	})
	defer c.Close()

	createDBString := fmt.Sprintf("Create Database %v", migrationData.dbName)
	createDBQuery := client.NewQuery(createDBString, "", "")
	_, err := c.Query(createDBQuery)
	if err != nil {
		return fmt.Errorf("Error while creating Database : %s\n", err)
	}

	// Create a new point batch
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  migrationData.dbName,
		Precision: "s",
	})

	// Create a point and add to batch
	tags := map[string]string{"tag1": "value1"}
	fields := map[string]interface{}{
		"value": 10.1,
	}
	//Create and parse
	for i := migrationData.from; i.Before(migrationData.until); i = i.Add(time.Duration(24) * time.Hour) {
		pt, _ := client.NewPoint("dummy", tags, fields, i)
		bp.AddPoint(pt)
	}
	// Write the batch
	c.Write(bp)

	query := client.NewQuery("Show Shard Groups", "", "")
	response, err := c.Query(query)
	if err != nil {
		return fmt.Errorf("Error in Querying : %s\n", err)
	}
	var index int = 0
	var colname string
	for index, colname = range response.Results[0].Series[0].Columns {
		if colname == "database" {
			break
		}
	}
	for _, values := range response.Results[0].Series[0].Values {
		if values[index] == migrationData.dbName {
			shard := &ShardInfo{}
			shard.id = values[0].(json.Number)
			shard.from, _ = time.Parse(time.RFC3339, values[3].(string))
			shard.until, _ = time.Parse(time.RFC3339, values[4].(string))
			migrationData.shards = append(migrationData.shards, *shard)
		}
	}

	//Once shards are created, this measurement is not required
	dropMeasurementQuery := client.NewQuery("Drop Database dummy", "", "")
	_, err = c.Query(dropMeasurementQuery)
	if err != nil {
		return fmt.Errorf("Error in Dropping Dummy DB : %s\n", err)
	}
	return nil
}

// For every shard, gets the whisper data which overlaps the time range of shard
// and  Writes to the respective TSM file
func (migrationData *MigrationData) MapWSPToTSMByShard() error {
	var from, until time.Time
	for _, shard := range migrationData.shards {
		from = shard.from
		if shard.from.Before(migrationData.from) && shard.until.After(migrationData.from) {
			from = migrationData.from
		}
		until = shard.until
		if shard.until.After(migrationData.until) {
			until = migrationData.until
		}
		ch := make(chan []TsmPoint)
		go func() {
			ch <- migrationData.MapWSPToTSMByWhisperFile(from, until)
		}()
		//Write the TSM data
		filename := migrationData.GetTSMFileName(shard)
		err := migrationData.WriteTSMPoints(filename, <-ch)
		if err != nil {
			return fmt.Errorf("Error in TSM Writing")
		}
	}
	return nil
}

//For every whisper file, maps whisper data points to TSM data points for
//given time range, this is just mapping points from one Data structure to other
// not writing to files
func (migrationData *MigrationData) MapWSPToTSMByWhisperFile(from time.Time, until time.Time) []TsmPoint {
	var tsmPoints []TsmPoint
	var tsmPoint TsmPoint

	for _, wspFile := range migrationData.wspFiles {
		w, err := whisper.Open(wspFile)
		fmt.Println("Migrating Data From ", wspFile, "For TimeRange ", from, until, "Size", w.Header.Archives[0].Size())
		if err != nil {
			log.Fatal(err)
		}

		//the first argument is interval, since it's not required for migration
		//using _
		_, wspPoints, err := w.FetchUntilTime(from, until)
		w.Close()
		if err != nil {
			log.Fatal(err)
		}

		if len(wspPoints) == 0 {
			continue
		}
		var tagConfig *TagConfig
		var mtf *MTF
		if mtf = migrationData.GetMTF(wspFile); mtf != nil {
		} else { //Create and add the pattern
			for {
				tagConfig = NewConfig(wspFile)
				if tagConfig != nil {
					break
				}
			}
			migrationData.tagConfigs = append(migrationData.tagConfigs, *tagConfig)

			mtf = &MTF{Measurement: tagConfig.Measurement, Tags: tagConfig.Tags,
				Field: tagConfig.Field}
		}

		tsmPoint.key = CreateTSMKey(mtf)
		tsmPoint.values = make([]tsm1.Value, len(wspPoints))
		for j, wspPoint := range wspPoints {
			tsmPoint.values[j] = tsm1.NewValue(int64(wspPoint.Timestamp), wspPoint.Value)
		}
		tsmPoints = append(tsmPoints, tsmPoint)
	}
	return tsmPoints
}

func (migrationData *MigrationData) GetTSMFileName(shard ShardInfo) string {
	retentionPolicy := "/default/" //TODO:...
	shardName := shard.id.String() + "/"
	filename := "000000001-000000002.tsm" // TODO:..
	filePath := migrationData.influxDataDir + "/" + migrationData.dbName +
		retentionPolicy + shardName + filename
	return filePath
}

// Write TSMPoints data to TSM files
func (migrationData *MigrationData) WriteTSMPoints(filename string,
	tsmPoints []TsmPoint) error {

	if len(tsmPoints) == 0 {
		return nil
	}
	// Open tsm file for writing
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer f.Close()

	//Create TSMWriter with filehandle
	tsmWriter, err := tsm1.NewTSMWriter(f)
	if err != nil {
		panic(fmt.Sprintf("create TSM writer: %v", err))
	}

	//Write the points in batch
	writes := 0
	for _, tsmPoint := range tsmPoints {
		//fmt.Println(i, tsmPoint.key)
		if len(tsmPoint.values) > 0 {
			if err := tsmWriter.Write([]byte(tsmPoint.key), tsmPoint.values); err != nil {
				panic(fmt.Sprintf("write TSM value: %v", err))
			}
			writes = writes + 1
		}
	}
	// Should not write index if there are no writes
	if writes == 0 {
		return nil
	}
	//Write index
	if err := tsmWriter.WriteIndex(); err != nil {
		panic(fmt.Sprintf("write TSM index: %v", err))
	}

	if err := tsmWriter.Close(); err != nil {
		panic(fmt.Sprintf("write TSM close: %v", err))
	}

	// Opening the file again, just to check size
	f1, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer f1.Close()

	fileinfo1, err := f1.Stat()
	if err != nil {
		fmt.Println("Could not read filestat", err)
		return err
	}
	migrationData.tsmFileSize = migrationData.tsmFileSize + fileinfo1.Size()
	fmt.Println("TSM File ", filename, "Size ", fileinfo1.Size())
	return err
}

//Create TSM Key from measurement, tags and field
func CreateTSMKey(mtf *MTF) string {
	key := mtf.Measurement
	if len(mtf.Tags) > 0 {
		for _, tagKeyValue := range mtf.Tags {
			key = key + ","
			key = key + tagKeyValue.Tagkey + "=" + tagKeyValue.Tagvalue
		}
	}
	return key + "#!~#" + mtf.Field
}

// Get measurement, tags and field by matching the whisper filename with a
// pattern in the config file
func (migrationData *MigrationData) GetMTF(wspFilename string) *MTF {

	wspFilename = strings.TrimSuffix(wspFilename, ".wsp")
	wspFilename = strings.Replace(wspFilename, "/", ".", -1)
	wspFilename = strings.Replace(wspFilename, ",", "_", -1)
	wspFilename = strings.Replace(wspFilename, " ", "_", -1)

	var patternStr []string
	var matches [][]int
	var tagConfig TagConfig
	filenameMatched := false
	for _, tagConfig = range migrationData.tagConfigs {
		patternStr = strings.Split(tagConfig.Pattern, "#")
		re := regexp.MustCompile(patternStr[0])
		//FindAllIndex returns array of start and end index of the match
		matches = re.FindAllIndex([]byte(wspFilename), -1)
		if matches != nil {
			filenameMatched = true
			break
		}
	}
	if filenameMatched == false {
		return nil
	}
	//extract the string starting at end of the matched pattern
	//e.g. carbon.relays.eud3-pr-mutgra1-a.whitelistRejects,
	// the remaining would be eud3-pr-mutgra1-a.whitelistRejects
	remaining := wspFilename[matches[0][1]:]

	//Split the remaining string on .
	//e.g. Now the remArr holds eud3-pr-mutgra1-a, whitelistRejects
	remArr := strings.Split(remaining, ".")

	//patternStr contains pattern split on #
	//e.g. patternStr[0]carbon.relays. , patternStr[1]TEXT1. , patternStr[2]TEXT2.
	var mtf MTF

	//start at i=1, that's #TEXT1 and iterate on all possible # strings in given
	// pattern
	mtf.Tags = make([]TagKeyValue, len(tagConfig.Tags))
	for i := 1; i < len(patternStr)-1; i++ {
		patternTagValue := strings.Trim(patternStr[i], ".")
		//For each # string, find a match in tag values
		for j, tagkeyvalue := range tagConfig.Tags {
			if strings.Trim(tagkeyvalue.Tagvalue, "#") == patternTagValue {
				mtf.Tags[j].Tagkey = tagkeyvalue.Tagkey
				//Tag #value is replaced with the actual value
				mtf.Tags[j].Tagvalue = remArr[i-1]
			}
		}
	}
	// Assign the last string as measurement
	mtf.Measurement = remArr[len(remArr)-1]
	mtf.Field = tagConfig.Field
	return &mtf
}

// Print Migration Summary
func (migrationData *MigrationData) PrintSummary(duration string) {
	fmt.Printf("|------------------------------------|\n")
	fmt.Printf("|------Migration Summary-------------|\n")
	fmt.Printf("|------------------------------------|\n")
	fmt.Printf("| No. of whisper files migrated %d|\n", len(migrationData.wspFiles))
	fmt.Printf("| TimeTaken %v |\n", duration)
	size, unit := formatSize(migrationData.whisperFileSize)
	fmt.Printf("| Total Whisper File Size %.2f %s |\n", size, unit)
	if migrationData.option == "TSMW" {
		size, unit := formatSize(migrationData.tsmFileSize)
		fmt.Printf("| Total TSM File Size     %.2f %s |\n", size, unit)
		var percentage float64
		percentage = float64(migrationData.whisperFileSize-migrationData.tsmFileSize) / float64(migrationData.whisperFileSize) * 100.0
		fmt.Printf("| Percentage of size reduction %.2f\n", percentage)
	}
	fmt.Printf("|------------------------------------|\n")
}

func (migrationData *MigrationData) WriteUsingV2() {
	from := migrationData.from
	until := migrationData.until
	c, _ := client.NewHTTPClient(client.HTTPConfig{
		Addr:     migrationData.host + ":" + migrationData.port,
		Username: migrationData.username,
		Password: migrationData.password,
	})
	defer c.Close()

	createDBString := fmt.Sprintf("Create Database %v", migrationData.dbName)
	createDBQuery := client.NewQuery(createDBString, "", "")
	_, err := c.Query(createDBQuery)
	if err != nil {
		fmt.Println(err)
		return
	}

	var bp client.BatchPoints
	bp, _ = client.NewBatchPoints(client.BatchPointsConfig{
		Database:  migrationData.dbName,
		Precision: "s",
	})

	for _, wspFile := range migrationData.wspFiles {
		w, err := whisper.Open(wspFile)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Migrating Data From ", wspFile, "For TimeRange ", from, until, "Size", w.Header.Archives[0].Size())
		//the first return argument is interval, since it's not required for migration
		//using _
		_, wspPoints, err := w.FetchUntilTime(from, until)
		w.Close()
		if err != nil {
			log.Fatal(err)
		}

		if len(wspPoints) == 0 {
			continue
		}
		var tagConfig *TagConfig
		var mtf *MTF
		if mtf = migrationData.GetMTF(wspFile); mtf != nil {
		} else { //Create and add the pattern
			for {
				tagConfig = NewConfig(wspFile)
				if tagConfig != nil {
					break
				}
			}
			migrationData.tagConfigs = append(migrationData.tagConfigs, *tagConfig)

			mtf = &MTF{Measurement: tagConfig.Measurement, Tags: tagConfig.Tags,
				Field: tagConfig.Field}
		}

		var tags map[string]string
		tags = make(map[string]string)
		for _, tagConfigTag := range mtf.Tags {
			tags[tagConfigTag.Tagkey] = tagConfigTag.Tagvalue
		}
		var fields map[string]interface{}
		fields = make(map[string]interface{})

		for _, wspPoint := range wspPoints {
			fields[mtf.Field] = wspPoint.Value
			pt, _ := client.NewPoint(mtf.Measurement, tags, fields,
				time.Unix(int64(wspPoint.Timestamp), 0))
			bp.AddPoint(pt)
		}
		c.Write(bp)
	}
	return
}

func formatSize(size int64) (float64, string) {
	if size < 1024 {
		return float64(size), "B"
	}
	if size < 1048576 {
		return float64(size) / float64(1024), "KB"
	}
	if size < 1073741824 {
		return float64(size) / float64(1048576), "MB"
	}
	return float64(size) / float64(1073741824), "GB"
}

func (migrationData *MigrationData) GetWhisperInfo() error {
	for _, wspFile := range migrationData.wspFiles {
		w, err := whisper.Open(wspFile)
		if err != nil {
			return err
		}
		t, err := w.GetOldest()
		if err != nil {
			return err
		}
		_, pts, err := w.Fetch(t)
		if err != nil {
			return err
		}
		fmt.Printf("Whisper File : %s\n", wspFile)
		fmt.Println("Oldest Data in File : ", time.Unix(int64(t), 0))
		fmt.Println("Number of whisper points : ", len(pts))
		fmt.Println("-----------------------------------------------------------------------")
		w.Close()
	}
	return nil
}

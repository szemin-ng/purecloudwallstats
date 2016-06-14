package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/szemin-ng/purecloud"
	"github.com/szemin-ng/purecloud/analytics"
	"github.com/szemin-ng/purecloud/routing"

	_ "github.com/alexbrainman/odbc"
)

// AppConfig stores the application's config data
type AppConfig struct {
	PureCloudRegion       string   `json:"pureCloudRegion"`
	PureCloudClientID     string   `json:"pureCloudClientId"`
	PureCloudClientSecret string   `json:"pureCloudClientSecret"`
	Granularity           string   `json:"granularity"`
	PollFrequencySeconds  float32  `json:"pollFrequencySeconds"`
	Queues                []string `json:"queues"`
	Agents                []string `json:"agents"`
	OdbcDsn               string   `json:"odbcDsn"`
}

const configFile string = ""

//const configFile string = `c:\users\sze min\documents\go projects\src\purecloudwallstats\config.json`
const queueStatsTable string = "QueueStats"
const timeFormat string = "2006-01-02T15:04:05-0700"

var appConfig AppConfig // global app config

// PureCloud doesn't return service levels if lower than 30 minutes
var supportedGranularity = map[string]time.Duration{"PT30M": time.Minute * 30, "PT60M": time.Hour * 1, "PT1H": time.Hour * 1}

var supportedMediaType = []string{"voice", "chat", "email"}

var pureCloudToken purecloud.AccessToken
var db *sql.DB
var statTicker *time.Ticker

func main() {
	var err error

	if err = loadAppConfig(configFile); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Connect to ODBC database
	if db, err = sql.Open("odbc", appConfig.OdbcDsn); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Login to PureCloud using Client Credentials login
	if err = loginToPureCloud(); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Prepare ODBC tables, needs a valid PureCloud access token first, so login before calling this func
	if err = prepareDbTables(); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Start polling queue stats from PureCloud
	startGrabbingPureCloudStats()

	fmt.Println("Press ENTER to STOP")
	fmt.Scanln()
	statTicker.Stop()
	fmt.Println("Done")
}

// getPureCloudQueues returns a map of queueIDs and its corresponding queue names. Up to 1,000 active and inactive queues are returned.
func getPureCloudQueues() (queues map[string]string, err error) {
	var p = routing.GetQueueParams{PageSize: 1000, PageNumber: 1, Active: false}
	var queueList routing.QueueEntityListing

	queues = make(map[string]string)

	fmt.Printf("Retrieving list of configured queues...\n")
	if queueList, err = routing.GetListOfQueues(pureCloudToken, p); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	for _, queue := range queueList.Entities {
		queues[queue.ID] = queue.Name
	}
	fmt.Printf("Mapped %d queues\n", len(queues))

	return
}

// loadAppConfig loads the config file for the app to run. If a configFile is passed in, e.g., C:\config.json, it uses that file. This is for testing purposes.
// In production, null string should be passed in so that it looks for the config file at os.Args[1]
func loadAppConfig(configFile string) (err error) {
	var f string

	// Config file supplied?
	if configFile == "" {
		if len(os.Args) < 2 {
			err = errors.New("Usage: %s configfile")
			return
		}
		f = os.Args[1]
	} else {
		f = configFile
	}

	// Read config file
	var b []byte
	if b, err = ioutil.ReadFile(f); err != nil {
		return
	}

	// Decode into AppConfig struct
	var d = json.NewDecoder(bytes.NewReader(b))
	if err = d.Decode(&appConfig); err != nil {
		return
	}

	// Validate granularity in config file
	if _, valid := supportedGranularity[appConfig.Granularity]; valid == false {
		err = errors.New("Invalid granularity. Use PT30M, PT60M or PT1H")
		return
	}

	// Validate pollFequencySeconds
	if int(appConfig.PollFrequencySeconds) <= 0 || int(appConfig.PollFrequencySeconds) > 60 {
		err = errors.New("Invalid frequency. Keep it within 60 seconds")
		return
	}

	return
}

// loginToPureCloud logs into PureCloud using client credentials login
func loginToPureCloud() (err error) {
	fmt.Printf("Logging into PureCloud...\r")
	if pureCloudToken, err = purecloud.LoginWithClientCredentials(appConfig.PureCloudRegion, appConfig.PureCloudClientID, appConfig.PureCloudClientSecret); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Successfully logged in.\n")
	return
}

// prepareDbTables creates the table to hold queue wallboard stats
func prepareDbTables() (err error) {
	var queueNames map[string]string
	if queueNames, err = getPureCloudQueues(); err != nil {
		return
	}

	// Drop table existing table, wallboard stats don't need historical data
	db.Exec("DROP TABLE " + queueStatsTable)

	fmt.Printf("Creating %s table\n", queueStatsTable)

	// Create table
	if _, err = db.Exec("CREATE TABLE " + queueStatsTable + " (QueueID VARCHAR(50), QueueName VARCHAR(100), MediaType VARCHAR(10), " +
		"oServiceTarget float, oServiceLevel float, oInteracting int, oWaiting int, " +
		"nError int, nOffered int, nOutboundAbandoned int, nOutboundAttempted int, " +
		"nOutboundConnected int, nTransferred int, nOverSla int, " +
		"tAbandon float, mtAbandon float, nAbandon int, " +
		"tAcd float, mtAcd float, nAcd int, " +
		"tAcw float, mtAcw float, nAcw int, " +
		"tAgentResponseTime float, mtAgentResponseTime float, nAgentResponseTime int, " +
		"tAnswered float, mtAnswered float, nAnswered int, " +
		"tHandle float, mtHandle float, nHandle int, " +
		"tHeld float, mtHeld float, nHeld int, " +
		"tHeldComplete float, mtHeldComplete float, nHeldComplete int, " +
		"tIvr float, mtIvr float, nIvr int, " +
		"tTalk float, mtTalk float, nTalk int, " +
		"tTalkComplete float, mtTalkComplete float, nTalkComplete int, " +
		"tWait float, mtWait float, nWait int, " +
		"tUserResponseTime float, mtUserResponseTime float, nUserResponseTime int)"); err != nil {
		return
	}

	fmt.Println("Prepopulating table data")

	// Prepopulate queue data, perhaps add an INDEX too?
	for _, queueID := range appConfig.Queues {
		for _, mediaType := range supportedMediaType {
			if _, err = db.Exec("INSERT INTO "+queueStatsTable+" (QueueID, QueueName, MediaType) VALUES (?, ?, ?)", queueID, queueNames[queueID], mediaType); err != nil {
				return
			}
		}
	}

	return
}

// queryAndWriteQueueStatsToDb queries PureCloud for stats, parse through it and writes it into the database table
func queryAndWriteQueueStatsToDb(startInterval time.Time, endInterval time.Time) (err error) {
	fmt.Printf("Querying queue stats for interval %s...\n", startInterval.Format(timeFormat))

	// Query PureCloud stats
	var aggResp purecloud.AggregateQueryResponse
	var obsResp purecloud.ObservationQueryResponse
	if aggResp, obsResp, err = queryQueueIntervalStats(startInterval, endInterval); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Loop through the monitored queues and update the database table
	for _, queueID := range appConfig.Queues {
		// Loop through all supported media types
		for _, mediaType := range supportedMediaType {
			// Declare variables here so that it gets initialized to zero for every loop
			var oInteracting, oWaiting, nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred, nOverSLA int
			var nAbandon, nAcd, nAcw, nAgentResponseTime, nAnswered, nHandle, nHeld, nHeldComplete, nIvr, nTalk, nTalkComplete, nUserResponseTime, nWait int
			var tAbandon, mtAbandon, tAcd, mtAcd, tAcw, mtAcw, tAgentResponseTime, mtAgentResponseTime, tAnswered, mtAnswered, tHandle, mtHandle float64
			var tHeld, mtHeld, tHeldComplete, mtHeldComplete, tIvr, mtIvr, tTalk, mtTalk, tTalkComplete, mtTalkComplete, tUserResponseTime, mtUserResponseTime float64
			var tWait, mtWait float64
			var oServiceLevel, oServiceTarget float64

			// Find result from queue aggregate result set returned from PureCloud
			for _, result := range aggResp.Results {
				if result.Group.QueueID == queueID && result.Group.MediaType == mediaType {
					// If found, grab all the metrics
					for _, data := range result.Data {
						for _, metric := range data.Metrics {
							switch {
							case metric.Metric == "nError":
								nError = int(metric.Stats.Count)
							case metric.Metric == "nOffered":
								nOffered = int(metric.Stats.Count)
							case metric.Metric == "nOutboundAbandoned":
								nOutboundAbandoned = int(metric.Stats.Count)
							case metric.Metric == "nOutboundAttempted":
								nOutboundAttempted = int(metric.Stats.Count)
							case metric.Metric == "nOutboundConnected":
								nOutboundConnected = int(metric.Stats.Count)
							case metric.Metric == "nTransferred":
								nTransferred = int(metric.Stats.Count)
							case metric.Metric == "nOverSla":
								nOverSLA = int(metric.Stats.Count)
							case metric.Metric == "oInteracting": // ignore this metric
							case metric.Metric == "oServiceLevel":
								oServiceLevel = metric.Stats.Ratio
							case metric.Metric == "oServiceTarget":
								oServiceTarget = metric.Stats.Current
							case metric.Metric == "oWaiting": // ignore this metric
							case metric.Metric == "tAbandon":
								tAbandon = metric.Stats.Sum
								mtAbandon = metric.Stats.Max
								nAbandon = int(metric.Stats.Count)
							case metric.Metric == "tAcd":
								tAcd = metric.Stats.Sum
								mtAcd = metric.Stats.Max
								nAcd = int(metric.Stats.Count)
							case metric.Metric == "tAcw":
								tAcw = metric.Stats.Sum
								mtAcw = metric.Stats.Max
								nAcw = int(metric.Stats.Count)
							case metric.Metric == "tAgentResponseTime":
								tAgentResponseTime = metric.Stats.Sum
								mtAgentResponseTime = metric.Stats.Max
								nAgentResponseTime = int(metric.Stats.Count)
							case metric.Metric == "tAnswered":
								tAnswered = metric.Stats.Sum
								mtAnswered = metric.Stats.Max
								nAnswered = int(metric.Stats.Count)
							case metric.Metric == "tHandle":
								tHandle = metric.Stats.Sum
								mtHandle = metric.Stats.Max
								nHandle = int(metric.Stats.Count)
							case metric.Metric == "tHeld":
								tHeld = metric.Stats.Sum
								mtHeld = metric.Stats.Max
								nHeld = int(metric.Stats.Count)
							case metric.Metric == "tHeldComplete":
								tHeldComplete = metric.Stats.Sum
								mtHeldComplete = metric.Stats.Max
								nHeldComplete = int(metric.Stats.Count)
							case metric.Metric == "tIvr":
								tIvr = metric.Stats.Sum
								mtIvr = metric.Stats.Max
								nIvr = int(metric.Stats.Count)
							case metric.Metric == "tTalk":
								tTalk = metric.Stats.Sum
								mtTalk = metric.Stats.Max
								nTalk = int(metric.Stats.Count)
							case metric.Metric == "tTalkComplete":
								tTalkComplete = metric.Stats.Sum
								mtTalkComplete = metric.Stats.Max
								nTalkComplete = int(metric.Stats.Count)
							case metric.Metric == "tUserResponseTime":
								tUserResponseTime = metric.Stats.Sum
								mtUserResponseTime = metric.Stats.Max
								nUserResponseTime = int(metric.Stats.Count)
							case metric.Metric == "tWait":
								tWait = metric.Stats.Sum
								mtWait = metric.Stats.Max
								nWait = int(metric.Stats.Count)
							default:
								panic(fmt.Sprintf("Unrecognized metric %s", metric.Metric)) // panic if we don't recognize the metric, need to fix code
							}
						}
					}
					// Found it, break out of loop
					break
				}
			}

			// Find result from queue observation result set returned from PureCloud
			for _, result := range obsResp.Results {
				if result.Group.QueueID == queueID && result.Group.MediaType == mediaType {
					// If found, grab all the metrics
					for _, metric := range result.Data {
						switch {
						case metric.Metric == "oInteracting":
							oInteracting = int(metric.Stats.Count)
						case metric.Metric == "oWaiting":
							oWaiting = int(metric.Stats.Count)
						default:
							panic(fmt.Sprintf("Unrecognized metric %s", metric.Metric)) // panic if we don't recognize the metric, need to fix code
						}
					}
					// Found it, break out of loop
					break
				}
			}

			fmt.Printf("Que: %.8s..., Med: %5s, Int: %3d, Wai: %3d, Off: %3d, Ans: %3d, Aba: %3d, Svc: %1.2f\n", queueID, mediaType, oInteracting, oWaiting, nOffered, nAnswered, nAbandon, oServiceLevel)

			if _, err = db.Exec(fmt.Sprintf("UPDATE %s SET "+
				"nError = %d, oServiceLevel = %f, oServiceTarget = %f, oInteracting = %d, oWaiting = %d, nOffered = %d, nOutboundAbandoned = %d, nOutboundAttempted = %d, nOutboundConnected = %d, nTransferred = %d, nOverSla = %d, "+
				"tAbandon = %f, mtAbandon = %f, nAbandon = %d, tAcd = %f, mtAcd = %f, nAcd = %d, tAcw = %f, mtAcw = %f, nAcw = %d, tAgentResponseTime = %f, mtAgentResponseTime = %f, nAgentResponseTime = %d, "+
				"tAnswered = %f, mtAnswered = %f, nAnswered = %d, tHandle = %f, mtHandle = %f, nHandle = %d, tHeld = %f, mtHeld = %f, nHeld = %d, tHeldComplete = %f, mtHeldComplete = %f, nHeldComplete = %d, "+
				"tIvr = %f, mtIvr = %f, nIvr = %d, tTalk = %f, mtTalk = %f, nTalk = %d, tTalkComplete = %f, mtTalkComplete = %f, nTalkComplete = %d, tUserResponseTime = %f, mtUserResponseTime = %f, nUserResponseTime = %d, "+
				"tWait = %f, mtWait = %f, nWait = %d "+
				"WHERE QueueID = '%s' AND MediaType = '%s'",
				queueStatsTable,
				nError, oServiceLevel, oServiceTarget, oInteracting, oWaiting, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred, nOverSLA,
				tAbandon, mtAbandon, nAbandon, tAcd, mtAcd, nAcd, tAcw, mtAcw, nAcw, tAgentResponseTime, mtAgentResponseTime, nAgentResponseTime,
				tAnswered, mtAnswered, nAnswered, tHandle, mtHandle, nHandle, tHeld, mtHeld, nHeld, tHeldComplete, mtHeldComplete, nHeldComplete,
				tIvr, mtIvr, nIvr, tTalk, mtTalk, nTalk, tTalkComplete, mtTalkComplete, nTalkComplete, tUserResponseTime, mtUserResponseTime, nUserResponseTime,
				tWait, mtWait, nWait,
				queueID, mediaType)); err != nil {
				fmt.Printf("Error: %s\n", err)
				return
			}
		}
	}
	return
}

// queryQueueIntervalStats prepares the PureCloud query, sends it to PureCloud and returns a response
func queryQueueIntervalStats(startInterval time.Time, endInterval time.Time) (aggResp purecloud.AggregateQueryResponse, obsResp purecloud.ObservationQueryResponse, err error) {
	// Create the following query to use in API call
	/*
		{
		   "interval": "2016-06-08T00:00:00+08:00/2016-06-09T00:00:00+08:00",
		   "granularity": "P1D",
		   "groupBy": [ "queueId" ],
		   "filter": {
		       "type": "and",
		       "clauses": [
		           {
		              "type": "or",
		              "predicates": [
		                  { "dimension": "mediaType", "value": "chat" },
		                  { "dimension": "mediaType", "value": "..." },
		              ]
		           },
		           {
		              "type": "or",
		              "predicates": [
		                  { "dimension": "queueId", "value": "c2788c7e-c8c5-40ac-97d9-51c3b364479b" }
		                  { "dimension": "queueId", "value": "..." }
		              ]
		           }
		       ]
		   }
		}*/

	// Query to send out
	var aggQuery = purecloud.AggregationQuery{
		Interval:    startInterval.Format(timeFormat) + "/" + endInterval.Format(timeFormat),
		Granularity: appConfig.Granularity,
		Filter: &purecloud.AnalyticsQueryFilter{
			Type: "and",
		},
		GroupBy: []string{"queueId"},
	}

	// Add media type clause into the query
	var mediaTypeClause = purecloud.AnalyticsQueryClause{Type: "or"}
	for _, mediaType := range supportedMediaType {
		mediaTypeClause.Predicates = append(mediaTypeClause.Predicates, purecloud.AnalyticsQueryPredicate{Dimension: "mediaType", Value: mediaType})
	}

	// Add queue ID clause into the query
	var queueIDClause = purecloud.AnalyticsQueryClause{Type: "or"}
	for _, queueID := range appConfig.Queues {
		queueIDClause.Predicates = append(queueIDClause.Predicates, purecloud.AnalyticsQueryPredicate{Dimension: "queueId", Value: queueID})
	}

	// Append the clauses to the query. We do it last because Go's append returns a new copy of the slice
	aggQuery.Filter.Clauses = append(aggQuery.Filter.Clauses, mediaTypeClause)
	aggQuery.Filter.Clauses = append(aggQuery.Filter.Clauses, queueIDClause)

	// Send query to PureCloud
	if aggResp, err = analytics.QueryConversationAggregates(pureCloudToken, aggQuery); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Reuse same filter from aggregate query for observation query
	var obsQuery purecloud.ObservationQuery
	obsQuery.Filter = aggQuery.Filter

	// Send query to PureCloud
	if obsResp, err = analytics.QueryQueueObservations(pureCloudToken, obsQuery); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	return
}

// startGrabbingPureCloudStats starts a goroutine to query PureCloud stats every poll interval
func startGrabbingPureCloudStats() (err error) {
	var tick time.Duration
	tick = time.Duration(int64(time.Second) * int64(appConfig.PollFrequencySeconds))
	fmt.Printf("Setting ticker to %s\n", tick)
	statTicker = time.NewTicker(tick)

	// goroutine to query PureCloud for stats based on frequency
	go func() {
		for t := range statTicker.C {
			var err error
			var startInterval, endInterval time.Time

			// Calculate current interval
			startInterval = time.Now().Truncate(supportedGranularity[appConfig.Granularity])
			endInterval = startInterval.Add(supportedGranularity[appConfig.Granularity])

			if err = queryAndWriteQueueStatsToDb(startInterval, endInterval); err != nil {
				fmt.Printf("%s: Error: %s\n", t.Format(timeFormat), err)
			}
		}
	}()

	return
}

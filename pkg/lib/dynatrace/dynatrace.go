package dynatrace

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	keptnevents "github.com/keptn/go-utils/pkg/lib"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/keptn-contrib/dynatrace-sli-service/pkg/common"

	// keptnevents "github.com/keptn/go-utils/pkg/events"
	// keptnutils "github.com/keptn/go-utils/pkg/utils"

	keptn "github.com/keptn/go-utils/pkg/lib/keptn"
)

const Throughput = "throughput"
const ErrorRate = "error_rate"
const ResponseTimeP50 = "response_time_p50"
const ResponseTimeP90 = "response_time_p90"
const ResponseTimeP95 = "response_time_p95"

// store url to the metrics api format migration document
const MetricsAPIOldFormatNewFormatDoc = "https://github.com/keptn-contrib/dynatrace-sli-service/blob/master/docs/CustomQueryFormatMigration.md"

type resultNumbers struct {
	Dimensions []string  `json:"dimensions"`
	Timestamps []int64   `json:"timestamps"`
	Values     []float64 `json:"values"`
}

type resultValues struct {
	MetricID string          `json:"metricId"`
	Data     []resultNumbers `json:"data"`
}

// DTUSQLResult struct
type DTUSQLResult struct {
	ExtrapolationLevel int             `json:"extrapolationLevel"`
	ColumnNames        []string        `json:"columnNames"`
	Values             [][]interface{} `json:"values"`
}

// SLI struct for SLI.yaml
type SLI struct {
	SpecVersion string            `yaml:"spec_version"`
	Indicators  map[string]string `yaml:"indicators"`
}

// DynatraceDashboards is struct for /dashboards endpoint
type DynatraceDashboards struct {
	Dashboards []struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Owner string `json:"owner"`
	} `json:"dashboards"`
}

// DynatraceDashboard is struct for /dashboards/<dashboardID> endpoint
type DynatraceDashboard struct {
	Metadata struct {
		ConfigurationVersions []int  `json:"configurationVersions"`
		ClusterVersion        string `json:"clusterVersion"`
	} `json:"metadata"`
	ID                string `json:"id"`
	DashboardMetadata struct {
		Name           string `json:"name"`
		Shared         bool   `json:"shared"`
		Owner          string `json:"owner"`
		SharingDetails struct {
			LinkShared bool `json:"linkShared"`
			Published  bool `json:"published"`
		} `json:"sharingDetails"`
		DashboardFilter *struct {
			Timeframe      string `json:"timeframe"`
			ManagementZone *struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"managementZone,omitempty"`
		} `json:"dashboardFilter,omitempty"`
		Tags []string `json:"tags"`
	} `json:"dashboardMetadata"`
	Tiles []struct {
		Name       string `json:"name"`
		TileType   string `json:"tileType"`
		Configured bool   `json:"configured"`
		Query      string `json:"query"`
		Type       string `json:"type"`
		CustomName string `json:"customName`
		Markdown   string `json:"markdown`
		Bounds     struct {
			Top    int `json:"top"`
			Left   int `json:"left"`
			Width  int `json:"width"`
			Height int `json:"height"`
		} `json:"bounds"`
		TileFilter struct {
			Timeframe      string `json:"timeframe"`
			ManagementZone *struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"managementZone,omitempty"`
		} `json:"tileFilter"`
		AssignedEntities []string `json:"assignedEntities"`
		FilterConfig     struct {
			Type        string `json:"type"`
			CustomName  string `json:"customName"`
			DefaultName string `json:"defaultName"`
			ChartConfig struct {
				LegendShown bool   `json:"legendShown"`
				Type        string `json:"type"`
				Series      []struct {
					Metric      string      `json:"metric"`
					Aggregation string      `json:"aggregation"`
					Percentile  interface{} `json:"percentile"`
					Type        string      `json:"type"`
					EntityType  string      `json:"entityType"`
					Dimensions  []struct {
						ID              string   `json:"id"`
						Name            string   `json:"name"`
						Values          []string `json:"values"`
						EntityDimension bool     `json:"entitiyDimension"`
					} `json:"dimensions"`
					SortAscending   bool   `json:"sortAscending"`
					SortColumn      bool   `json:"sortColumn"`
					AggregationRate string `json:"aggregationRate"`
				} `json:"series"`
				ResultMetadata struct {
				} `json:"resultMetadata"`
			} `json:"chartConfig"`
			FiltersPerEntityType map[string]map[string][]string `json:"filtersPerEntityType"`
			/* FiltersPerEntityType struct {
				HOST struct {
					SPECIFIC_ENTITIES    []string `json:"SPECIFIC_ENTITIES"`
					HOST_DATACENTERS     []string `json:"HOST_DATACENTERS"`
					AUTO_TAGS            []string `json:"AUTO_TAGS"`
					HOST_SOFTWARE_TECH   []string `json:"HOST_SOFTWARE_TECH"`
					HOST_VIRTUALIZATION  []string `json:"HOST_VIRTUALIZATION"`
					HOST_MONITORING_MODE []string `json:"HOST_MONITORING_MODE"`
					HOST_STATE           []string `json:"HOST_STATE"`
					HOST_HOST_GROUPS     []string `json:"HOST_HOST_GROUPS"`
				} `json:"HOST"`
				PROCESS_GROUP struct {
					SPECIFIC_ENTITIES     []string `json:"SPECIFIC_ENTITIES"`
					HOST_TAG_OF_PROCESS   []string `json:"HOST_TAG_OF_PROCESS"`
					AUTO_TAGS             []string `json:"AUTO_TAGS"`
					PROCESS_SOFTWARE_TECH []string `json:"PROCESS_SOFTWARE_TECH"`
				} `json:"PROCESS_GROUP"`
				PROCESS_GROUP_INSTANCE struct {
					SPECIFIC_ENTITIES     []string `json:"SPECIFIC_ENTITIES"`
					HOST_TAG_OF_PROCESS   []string `json:"HOST_TAG_OF_PROCESS"`
					AUTO_TAGS             []string `json:"AUTO_TAGS"`
					PROCESS_SOFTWARE_TECH []string `json:"PROCESS_SOFTWARE_TECH"`
				} `json:"PROCESS_GROUP_INSTANCE"`
				SERVICE struct {
					SPECIFIC_ENTITIES     []string `json:"SPECIFIC_ENTITIES"`
					SERVICE_SOFTWARE_TECH []string `json:"SERVICE_SOFTWARE_TECH"`
					AUTO_TAGS             []string `json:"AUTO_TAGS"`
					SERVICE_TYPE          []string `json:"SERVICE_TYPE"`
					SERVICE_TO_PG         []string `json:"SERVICE_TO_PG"`
				} `json:"SERVICE"`
				APPLICATION struct {
					SPECIFIC_ENTITIES          []string `json:"SPECIFIC_ENTITIES"`
					APPLICATION_TYPE           []string `json:"APPLICATION_TYPE"`
					AUTO_TAGS                  []string `json:"AUTO_TAGS"`
					APPLICATION_INJECTION_TYPE []string `json:"PROCESS_SOFTWARE_TECH"`
					APPLICATION_STATUS         []string `json:"APPLICATION_STATUS"`
				} `json:"APPLICATION"`
				APPLICATION_METHOD struct {
					SPECIFIC_ENTITIES []string `json:"SPECIFIC_ENTITIES"`
				} `json:"APPLICATION_METHOD"`
			} `json:"filtersPerEntityType"`*/
		} `json:"filterConfig"`
	} `json:"tiles"`
}

// MetricDefinition defines the output of /metrics/<metricID>
type MetricDefinition struct {
	MetricID           string   `json:"metricId"`
	DisplayName        string   `json:"displayName"`
	Description        string   `json:"description"`
	Unit               string   `json:"unit"`
	AggregationTypes   []string `json:"aggregationTypes"`
	Transformations    []string `json:"transformations"`
	DefaultAggregation struct {
		Type string `json:"type"`
	} `json:"defaultAggregation"`
	DimensionDefinitions []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"dimensionDefinitions"`
	EntityType []string `json:"entityType"`
}

type DtMetricsAPIError struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

/**
{
    "totalCount": 8,
    "nextPageKey": null,
    "result": [
        {
            "metricId": "builtin:service.response.time:percentile(50):merge(0)",
            "data": [
                {
                    "dimensions": [],
                    "timestamps": [
                        1579097520000
                    ],
                    "values": [
                        65005.48481639812
                    ]
                }
            ]
        }
    ]
}
*/

// DynatraceResult is struct for /metrics/query
type DynatraceResult struct {
	TotalCount  int            `json:"totalCount"`
	NextPageKey string         `json:"nextPageKey"`
	Result      []resultValues `json:"result"`
}

// Handler interacts with a dynatrace API endpoint
type Handler struct {
	ApiURL        string
	Username      string
	Password      string
	KeptnEvent    *common.BaseKeptnEvent
	HTTPClient    *http.Client
	Headers       map[string]string
	CustomQueries map[string]string
	CustomFilters []*keptnevents.SLIFilter
	Logger        *keptn.Logger
}

// NewDynatraceHandler returns a new dynatrace handler that interacts with the Dynatrace REST API
func NewDynatraceHandler(apiURL string, keptnEvent *common.BaseKeptnEvent, headers map[string]string, customFilters []*keptnevents.SLIFilter, keptnContext string, eventID string) *Handler {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !IsHttpSSLVerificationEnabled()},
	}
	ph := &Handler{
		ApiURL:        apiURL,
		KeptnEvent:    keptnEvent,
		HTTPClient:    &http.Client{Transport: tr},
		Headers:       headers,
		CustomFilters: customFilters,
		Logger:        keptn.NewLogger(keptnContext, eventID, "dynatrace-sli-service"),
	}

	return ph
}

/**
 * exeucteDynatraceREST
 * Executes a call to the Dynatrace REST API Endpoint - taking care of setting all required headers
 * addHeaders allows you to pass additional HTTP Headers
 * Returns the Response Object, the body byte array, error
 */
func (ph *Handler) executeDynatraceREST(httpMethod string, requestUrl string, addHeaders map[string]string) (*http.Response, []byte, error) {

	// new request to our URL
	req, err := http.NewRequest(httpMethod, requestUrl, nil)

	// add our default headers, e.g: authentication
	for headerName, headerValue := range ph.Headers {
		req.Header.Set(headerName, headerValue)
	}

	// add any additionally passed headers
	if addHeaders != nil {
		for addHeaderName, addHeaderValue := range addHeaders {
			req.Header.Set(addHeaderName, addHeaderValue)
		}
	}

	// perform the request
	resp, err := ph.HTTPClient.Do(req)
	if err != nil {
		return resp, nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	return resp, body, nil
}

/**
 * Helper function to validate whether string is a valid UUID
 */
func IsValidUUID(uuid string) bool {
	r := regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[8|9|aA|bB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$")
	return r.MatchString(uuid)
}

/**
 * findDynatraceDashboard
 * Queries all Dynatrace Dashboards and returns the dashboard ID that matches the following name patter: KQG;project=%project%;service=%service%;stage=%stage;xxx
 *
 * Returns the UUID of the dashboard that was found. If no dashboard was found it returns ""
 */
func (ph *Handler) findDynatraceDashboard(keptnEvent *common.BaseKeptnEvent) (string, error) {
	// Lets query the list of all Dashboards and find the one that matches project, stage, service based on the title (in the future - we can do it via tags)
	// create dashboard query URL and set additional headers
	// ph.Logger.Debug(fmt.Sprintf("Query all dashboards\n"))

	dashboardAPIUrl := ph.ApiURL + fmt.Sprintf("/api/config/v1/dashboards")
	resp, body, err := ph.executeDynatraceREST("GET", dashboardAPIUrl, nil)

	if resp == nil || resp.StatusCode != 200 {
		return "", err
	}

	// parse json
	dashboardsJSON := &DynatraceDashboards{}
	err = json.Unmarshal(body, &dashboardsJSON)

	if err != nil {
		return "", err
	}

	// now - lets iterate through the list and find one that matches our project, stage, service ...
	findValues := []string{strings.ToLower(fmt.Sprintf("project=%s", keptnEvent.Project)), strings.ToLower(fmt.Sprintf("service=%s", keptnEvent.Service)), strings.ToLower(fmt.Sprintf("stage=%s", keptnEvent.Stage))}
	for _, dashboard := range dashboardsJSON.Dashboards {

		// lets see if the dashboard matches our name
		if strings.HasPrefix(strings.ToLower(dashboard.Name), "kqg;") {
			nameSplits := strings.Split(dashboard.Name, ";")

			// now lets see if we can find all our name/value pairs for project, service & stage
			dashboardMatch := true
			for _, findValue := range findValues {
				foundValue := false
				for _, nameSplitValue := range nameSplits {
					if strings.Compare(findValue, strings.ToLower(nameSplitValue)) == 0 {
						foundValue = true
					}
				}
				if foundValue == false {
					dashboardMatch = false
					continue
				}
			}

			if dashboardMatch {
				return dashboard.ID, nil
			}
		}
	}

	return "", nil
}

/**
 * loadDynatraceDashboard:
 * Depending on the dashboard parameter which is pulled from dynatrace.conf.yaml:dashboard this method either
 * -- query: queries all dashboards on the Dynatrace Tenant and returns the one that matches project/service/stage
 * -- dashboard-ID: if this is a valid dashboard ID it will query the dashboard with this ID, e.g: ddb6a571-4bda-4e8b-a9c0-4a3e02c2e14a
 * -- <empty>: will not query any dashboard

 * Returns: parsed Dynatrace Dashboard and actual dashboard ID in case we queried a dashboard
 */
func (ph *Handler) loadDynatraceDashboard(keptnEvent *common.BaseKeptnEvent, dashboard string) (*DynatraceDashboard, string, error) {

	// Option 1: Query dashboards
	if dashboard == common.DynatraceConfigDashboardQUERY {
		dashboard, _ = ph.findDynatraceDashboard(keptnEvent)
		if dashboard == "" {
			ph.Logger.Debug(fmt.Sprintf("dashboard option query but couldnt find KQG dashboard for %s.%s.%s", keptnEvent.Project, keptnEvent.Stage, keptnEvent.Service))
		} else {
			ph.Logger.Debug(fmt.Sprintf("dashboard option query for %s.%s.%s found dashboard=%s", keptnEvent.Project, keptnEvent.Stage, keptnEvent.Service, dashboard))
		}
	}

	// Option 2: there is no dashboard we should query
	if dashboard == "" {
		return nil, dashboard, nil
	}

	// Lets validate if we have a valid UUID - either because it was passed or because queried
	// If not - we are going down the dashboard route!
	if !IsValidUUID(dashboard) {
		return nil, dashboard, fmt.Errorf("Dashboard ID %s not a valid UUID", dashboard)
	}

	// We have a valid Dashboard UUID - now lets query it!
	ph.Logger.Debug(fmt.Sprintf("Query dashboard with ID: %s", dashboard))
	dashboardAPIUrl := ph.ApiURL + fmt.Sprintf("/api/config/v1/dashboards/%s", dashboard)
	resp, body, err := ph.executeDynatraceREST("GET", dashboardAPIUrl, nil)

	if err != nil {
		return nil, dashboard, err
	}

	if resp == nil || resp.StatusCode != 200 {
		return nil, dashboard, fmt.Errorf("No valid response came back")
	}

	// parse json
	dashboardJSON := &DynatraceDashboard{}
	err = json.Unmarshal(body, &dashboardJSON)

	if err != nil {
		return nil, dashboard, fmt.Errorf("could not decode response payload: %v", err)
	}

	return dashboardJSON, dashboard, nil
}

/**
 * ExecuteMetricAPIDescribe
 * Calls the /metrics/<metricID> API call to retrieve Metric Definition Details
 */
func (ph *Handler) ExecuteMetricAPIDescribe(metricID string) (*MetricDefinition, error) {
	targetURL := ph.ApiURL + fmt.Sprintf("/api/v2/metrics/%s", metricID)
	resp, body, err := ph.executeDynatraceREST("GET", targetURL, nil)

	if err != nil {
		return nil, err
	}
	if resp == nil || resp.StatusCode != 200 {
		return nil, fmt.Errorf("No valid response from metrics api!")
	}

	// parse response json
	var result MetricDefinition
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	// make sure the status code from the API is 200
	if resp.StatusCode != 200 {
		dtMetricsErr := &DtMetricsAPIError{}
		err := json.Unmarshal(body, dtMetricsErr)
		if err == nil {
			return nil, fmt.Errorf("Dynatrace API returned status code %d: %s", dtMetricsErr.Error.Code, dtMetricsErr.Error.Message)
		}
		return nil, fmt.Errorf("Dynatrace API returned status code %d - Metric could not be received.", resp.StatusCode)
	}

	return &result, nil
}

// ExecuteMetricsAPIQuery executes the passed Metrics API Call, validates that the call returns data and returns the data set
func (ph *Handler) ExecuteMetricsAPIQuery(metricsQuery string) (*DynatraceResult, error) {
	// now we execute the query against the Dynatrace API
	resp, body, err := ph.executeDynatraceREST("GET", metricsQuery, map[string]string{"Content-Type": "application/json"})

	if err != nil {
		return nil, err
	}

	if resp == nil || resp.StatusCode != 200 {
		return nil, fmt.Errorf("No valid response from metrics api!")
	}

	// parse response json
	var result DynatraceResult
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	// make sure the status code from the API is 200
	if resp.StatusCode != 200 {
		dtMetricsErr := &DtMetricsAPIError{}
		err := json.Unmarshal(body, dtMetricsErr)
		if err == nil {
			return nil, fmt.Errorf("Dynatrace API returned status code %d: %s", dtMetricsErr.Error.Code, dtMetricsErr.Error.Message)
		}
		return nil, fmt.Errorf("Dynatrace API returned status code %d - Metric could not be received.", resp.StatusCode)
	}

	if len(result.Result) == 0 {
		// datapoints is empty - try again?
		return nil, errors.New("Dynatrace Metrics API returned no DataPoints")
	}

	return &result, nil
}

// ExecuteUSQLQuery executes the passed Metrics API Call, validates that the call returns data and returns the data set
func (ph *Handler) ExecuteUSQLQuery(usql string) (*DTUSQLResult, error) {
	// now we execute the query against the Dynatrace API
	resp, body, err := ph.executeDynatraceREST("GET", usql, map[string]string{"Content-Type": "application/json"})

	if resp == nil || err != nil || resp.StatusCode != 200 {
		return nil, err
	}

	// parse response json
	var result DTUSQLResult
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	// make sure the status code from the API is 200
	if resp.StatusCode != 200 {
		dtMetricsErr := &DtMetricsAPIError{}
		err := json.Unmarshal(body, dtMetricsErr)
		if err == nil {
			return nil, fmt.Errorf("Dynatrace API returned status code %d: %s", dtMetricsErr.Error.Code, dtMetricsErr.Error.Message)
		}
		return nil, fmt.Errorf("Dynatrace API returned status code %d - Metric could not be received.", resp.StatusCode)
	}

	// if no data comes back
	if len(result.Values) == 0 {
		// datapoints is empty - try again?
		return nil, errors.New("Dynatrace USQL Query didnt return any DataPoints")
	}

	return &result, nil
}

// BuildDynatraceUSQLQuery builds a USQL query based on the incoming values
func (ph *Handler) BuildDynatraceUSQLQuery(query string, startUnix time.Time, endUnix time.Time) string {
	ph.Logger.Debug(fmt.Sprintf("Finalize USQL query for %s\n", query))

	// replace query params (e.g., $PROJECT, $STAGE, $SERVICE ...)
	usql := ph.replaceQueryParameters(query)

	// default query params that are required: resolution, from and to
	queryParams := map[string]string{
		"query":             usql,
		"explain":           "false",
		"addDeepLinkFields": "false",
		"startTimestamp":    common.TimestampToString(startUnix),
		"endTimestamp":      common.TimestampToString(endUnix),
	}

	targetURL := fmt.Sprintf("%s/api/v1/userSessionQueryLanguage/table", ph.ApiURL)

	// append queryParams to targetURL
	u, _ := url.Parse(targetURL)
	q, _ := url.ParseQuery(u.RawQuery)

	for param, value := range queryParams {
		q.Add(param, value)
	}

	u.RawQuery = q.Encode()
	ph.Logger.Debug(fmt.Sprintf("Final USQL Query=%s", u.String()))

	return u.String()
}

// BuildDynatraceMetricsQuery builds the complete query string based on start, end and filters
// metricQuery should contain metricSelector and entitySelector
// Returns:
//  #1: Finalized Dynatrace API Query
//  #2: MetricID that this query will return, e.g: builtin:host.cpu
//  #3: error
func (ph *Handler) BuildDynatraceMetricsQuery(metricquery string, startUnix time.Time, endUnix time.Time) (string, string) {
	// replace query params (e.g., $PROJECT, $STAGE, $SERVICE ...)
	metricquery = ph.replaceQueryParameters(metricquery)

	if strings.HasPrefix(metricquery, "?metricSelector=") {
		ph.Logger.Debug(fmt.Sprintf("COMPATIBILITY WARNING: Provided query string %s is not compatible. Auto-removing the ? in front (see %s for details).\n", metricquery, MetricsAPIOldFormatNewFormatDoc))
		metricquery = strings.Replace(metricquery, "?metricSelector=", "metricSelector=", 1)
	}

	// split query string by first occurrence of "?"
	querySplit := strings.Split(metricquery, "?")
	metricSelector := ""
	metricQueryParams := ""

	// support the old format with "metricSelector:someFilters()?scope=..." as well as the new format with
	// "?metricSelector=metricSelector&entitySelector=...&scope=..."
	if len(querySplit) == 1 {
		// new format without "?" -> everything within the query string are query parameters
		metricQueryParams = querySplit[0]
	} else {
		ph.Logger.Debug(fmt.Sprintf("COMPATIBILITY WARNING: Your query %s still uses the old format (see %s for details).\n", metricQueryParams, MetricsAPIOldFormatNewFormatDoc))
		// old format with "?" - everything left of the ? is the identifier, everything right are query params
		metricSelector = querySplit[0]

		// build the new query
		metricQueryParams = fmt.Sprintf("metricSelector=%s&%s", querySplit[0], querySplit[1])
	}

	targetURL := ph.ApiURL + fmt.Sprintf("/api/v2/metrics/query/?%s", metricQueryParams)

	// default query params that are required: resolution, from and to
	queryParams := map[string]string{
		"resolution": "Inf", // resolution=Inf means that we only get 1 datapoint (per service)
		"from":       common.TimestampToString(startUnix),
		"to":         common.TimestampToString(endUnix),
	}
	// append queryParams to targetURL
	u, _ := url.Parse(targetURL)
	q, _ := url.ParseQuery(u.RawQuery)

	for param, value := range queryParams {
		q.Add(param, value)
	}

	// check if q contains "scope"
	scopeData := q.Get("scope")

	// compatibility with old scope=... custom queries
	if scopeData != "" {
		ph.Logger.Debug(fmt.Sprintf("COMPATIBILITY WARNING: You are still using scope=... - querying the new metrics API requires use of entitySelector=... instead (see %s for details).", MetricsAPIOldFormatNewFormatDoc))
		// scope is no longer supported in the new API, it needs to be called "entitySelector" and contain type(SERVICE)
		if !strings.Contains(scopeData, "type(SERVICE)") {
			ph.Logger.Debug(fmt.Sprintf("COMPATIBILITY WARNING: Automatically adding type(SERVICE) to entitySelector=... for compatibility with the new Metrics API (see %s for details).", MetricsAPIOldFormatNewFormatDoc))
			scopeData = fmt.Sprintf("%s,type(SERVICE)", scopeData)
		}
		// add scope as entitySelector
		q.Add("entitySelector", scopeData)
	}

	// check metricSelector
	if metricSelector == "" {
		metricSelector = q.Get("metricSelector")
	}

	u.RawQuery = q.Encode()
	ph.Logger.Debug(fmt.Sprintf("Final Query=%s", u.String()))

	return u.String(), metricSelector
}

// ParsePassAndWarningFromString takes a value such as
// Example 1: Some description;sli=teststep_rt;pass=<500ms,<+10%;warning=<1000ms,<+20%;weight=1;key=true
// Example 2: Response time (P95);sli=svc_rt_p95;pass=<+10%,<600
// Example 3: Host Disk Queue Length (max);sli=host_disk_queue;pass=<=0;warning=<1;key=false
// can also take a value like "KQG;project=myproject;pass=90%;warning=75%;"
// This will return
// #1: teststep_rt
// #2: []SLOCriteria { Criteria{"<500ms","<+10%"}}
// #3: []SLOCriteria { ["<1000ms","<+20%" }}
// #4: 1
// #5: true
func ParsePassAndWarningFromString(customName string, defaultPass []string, defaultWarning []string) (string, []*keptnevents.SLOCriteria, []*keptnevents.SLOCriteria, int, bool) {
	nameValueSplits := strings.Split(customName, ";")

	// lets initialize it
	sliName := ""
	weight := 1
	keySli := false
	passCriteria := []*keptnevents.SLOCriteria{}
	warnCriteria := []*keptnevents.SLOCriteria{}

	// lets iterate through all name-value pairs which are seprated through ";" to extract keys such as warning, pass, weight, key, sli
	for i := 0; i < len(nameValueSplits); i++ {

		nameValueDividerIndex := strings.Index(nameValueSplits[i], "=")
		if nameValueDividerIndex < 0 {
			continue
		}

		// for each name=value pair we get the name as first part of the string until the first =
		// the value is the after that =
		nameString := nameValueSplits[i][:nameValueDividerIndex]
		valueString := nameValueSplits[i][nameValueDividerIndex+1:]
		switch nameString /*nameValueSplit[0]*/ {
		case "sli":
			sliName = valueString
		case "pass":
			passCriteria = append(passCriteria, &keptnevents.SLOCriteria{
				Criteria: strings.Split(valueString, ","),
			})
		case "warning":
			warnCriteria = append(warnCriteria, &keptnevents.SLOCriteria{
				Criteria: strings.Split(valueString, ","),
			})
		case "key":
			keySli, _ = strconv.ParseBool(valueString)
		case "weight":
			weight, _ = strconv.Atoi(valueString)
		}
	}

	// use the defaults if nothing was specified
	if (len(passCriteria) == 0) && (len(defaultPass) > 0) {
		passCriteria = append(passCriteria, &keptnevents.SLOCriteria{
			Criteria: defaultPass,
		})
	}

	if (len(warnCriteria) == 0) && (len(defaultWarning) > 0) {
		warnCriteria = append(warnCriteria, &keptnevents.SLOCriteria{
			Criteria: defaultWarning,
		})
	}

	// if we have no criteria for warn or pass we just return nil
	if len(passCriteria) == 0 {
		passCriteria = nil
	}
	if len(warnCriteria) == 0 {
		warnCriteria = nil
	}

	return sliName, passCriteria, warnCriteria, weight, keySli
}

// ParseMarkdownConfiguration parses a text that can be used in a Markdown tile to specify global SLO properties
func ParseMarkdownConfiguration(markdown string, slo *keptnevents.ServiceLevelObjectives) {
	markdownSplits := strings.Split(markdown, ";")

	for _, markdownSplitValue := range markdownSplits {
		configValueSplits := strings.Split(markdownSplitValue, "=")
		if len(configValueSplits) != 2 {
			continue
		}

		// lets get configname and value
		configName := strings.ToLower(configValueSplits[0])
		configValue := configValueSplits[1]

		switch configName {
		case "kqg.total.pass":
			slo.TotalScore.Pass = configValue
		case "kqg.total.warning":
			slo.TotalScore.Warning = configValue
		case "kqg.compare.withscore":
			slo.Comparison.IncludeResultWithScore = configValue
			if (configValue == "pass") || (configValue == "pass_or_warn") || (configValue == "all") {
				slo.Comparison.IncludeResultWithScore = configValue
			} else {
				slo.Comparison.IncludeResultWithScore = "pass"
			}
		case "kqg.compare.results":
			noresults, err := strconv.Atoi(configValue)
			if err != nil {
				slo.Comparison.NumberOfComparisonResults = 1
			} else {
				slo.Comparison.NumberOfComparisonResults = noresults
			}
			if slo.Comparison.NumberOfComparisonResults > 1 {
				slo.Comparison.CompareWith = "several_results"
			} else {
				slo.Comparison.CompareWith = "single_result"
			}
		case "kqg.compare.function":
			if (configValue == "avg") || (configValue == "p50") || (configValue == "p90") || (configValue == "p95") {
				slo.Comparison.AggregateFunction = configValue
			} else {
				slo.Comparison.AggregateFunction = "avg"
			}
		}
	}
}

// cleanIndicatorName makes sure we have a valid indicator name by getting rid of special characters
func cleanIndicatorName(indicatorName string) string {
	// TODO: check more than just blanks
	indicatorName = strings.ReplaceAll(indicatorName, " ", "_")
	indicatorName = strings.ReplaceAll(indicatorName, "/", "_")
	indicatorName = strings.ReplaceAll(indicatorName, "%", "_")

	return indicatorName
}

/**
 * When passing a query to dynatrace using filter expressions - the dimension names in a filter will be escaped with specifal characters, e.g: filter(dt.entity.browser,IE) becomes filter(dt~entity~browser,ie)
 * This function here tries to come up with a better matching algorithm
 * WHILE NOT PERFECT - HERE IS THE FIRST IMPLEMENTATION
 */
func (ph *Handler) isMatchingMetricID(singleResultMetricID string, queryMetricID string) bool {
	if strings.Compare(singleResultMetricID, queryMetricID) == 0 {
		return true
	}

	// lets do some basic fuzzy matching
	if strings.Contains(singleResultMetricID, "~") {
		ph.Logger.Debug(fmt.Sprintf("Need Fuzzy Matching between %s and %s\n", singleResultMetricID, queryMetricID))

		//
		// lets just see whether everything until the first : matches
		if strings.Contains(singleResultMetricID, ":") && strings.Contains(singleResultMetricID, ":") {
			ph.Logger.Debug(fmt.Sprintf("Just compare before first :\n"))

			fuzzyResultMetricID := strings.Split(singleResultMetricID, ":")[0]
			fuzzyQueryMetricID := strings.Split(queryMetricID, ":")[0]
			if strings.Compare(fuzzyResultMetricID, fuzzyQueryMetricID) == 0 {
				ph.Logger.Debug(fmt.Sprintf("FUZZY MATCH!!\n"))
				return true
			}
		}

		// TODO - more fuzzy checks
	}

	return false
}

/**
 * This function will validate if the current dashboard.json stored in the configuration repo is the same as the one passed as parameter
 */
func (ph *Handler) HasDashboardChanged(keptnEvent *common.BaseKeptnEvent, dashboardJSON *DynatraceDashboard, existingDashboardContent string) bool {

	jsonAsByteArray, _ := json.MarshalIndent(dashboardJSON, "", "  ")
	newDashboardContent := string(jsonAsByteArray)

	// If ParseOnChange is not specified we consider this as a dashboard with a change
	if strings.Index(newDashboardContent, "KQG.QueryBehavior=ParseOnChange") == -1 {
		return true
	}

	// now lets compare the dashboard from the config repo and the one passed to this function
	if strings.Compare(newDashboardContent, existingDashboardContent) == 0 {
		return false
	}

	return true
}

/**
 * Parses the filtersPerEntityType dashboard definition and returns the entitySelector query filter - the return value always starts with a , (comma)
 * return example: ,entityId("ABAD-222121321321")
 */
func (ph *Handler) GetEntitySelectorFromEntityFilter(filtersPerEntityType map[string]map[string][]string, entityType string) string {
	entityTileFilter := ""
	if filtersPerEntityType, containsEntityType := filtersPerEntityType[entityType]; containsEntityType {
		// Check for SPECIFIC_ENTITIES - if we have an array then we filter for each entity
		if entityArray, containsSpecificEntities := filtersPerEntityType["SPECIFIC_ENTITIES"]; containsSpecificEntities {
			for _, entityId := range entityArray {
				entityTileFilter = entityTileFilter + ","
				entityTileFilter = entityTileFilter + fmt.Sprintf("entityId(\"%s\")", entityId)
			}
		}
		// Check for SPECIFIC_ENTITIES - if we have an array then we filter for each entity
		if tagArray, containsAutoTags := filtersPerEntityType["AUTO_TAGS"]; containsAutoTags {
			for _, tag := range tagArray {
				entityTileFilter = entityTileFilter + ","
				entityTileFilter = entityTileFilter + fmt.Sprintf("tag(\"%s\")", tag)
			}
		}
	}
	return entityTileFilter
}

// QueryDynatraceDashboardForSLIs implements - https://github.com/keptn-contrib/dynatrace-sli-service/issues/60
// Queries Dynatrace for the existance of a dashboard tagged with keptn_project:project, keptn_stage:stage, keptn_service:service, SLI
// if this dashboard exists it will be parsed and a custom SLI_dashboard.yaml and an SLO_dashboard.yaml will be created
// Returns:
//  #1: Link to Dashboard
//  #2: SLI
//  #3: ServiceLevelObjectives
//  #4: SLIResult
//  #5: Error
func (ph *Handler) QueryDynatraceDashboardForSLIs(keptnEvent *common.BaseKeptnEvent, dashboard string, startUnix time.Time, endUnix time.Time) (string, *DynatraceDashboard, *SLI, *keptnevents.ServiceLevelObjectives, []*keptnevents.SLIResult, error) {

	// Lets see if there is a dashboard.json already in the configuration repo - if so its an indicator that we should query the dashboard
	// This check is espcially important for backward compatibilty as the new dynatrace.conf.yaml:dashboard property is changing the default behavior
	// If a dashboard.json exists and dashboard property is empty we default to QUERY - which is the old default behavior
	existingDashboardContent, err := common.GetKeptnResource(keptnEvent, common.DynatraceDashboardFilename, ph.Logger)
	if err == nil && existingDashboardContent != "" && dashboard == "" {
		ph.Logger.Debug("Set dashboard=query for backward compatibility as dashboard.json was present!")
		dashboard = common.DynatraceConfigDashboardQUERY
	}

	// lets load the dashboard if needed
	dashboardJSON, dashboard, err := ph.loadDynatraceDashboard(keptnEvent, dashboard)
	if err != nil {
		return "", nil, nil, nil, nil, fmt.Errorf("Error while processing dashboard config '%s' - %v", dashboard, err)
	}

	if dashboardJSON == nil {
		return "", nil, nil, nil, nil, nil
	}

	// generate our own SLIResult array based on the dashboard configuration
	var sliResults []*keptnevents.SLIResult
	dashboardSLI := &SLI{}
	dashboardSLI.SpecVersion = "0.1.4"
	dashboardSLI.Indicators = make(map[string]string)
	dashboardSLO := &keptnevents.ServiceLevelObjectives{
		Objectives: []*keptnevents.SLO{},
		TotalScore: &keptnevents.SLOScore{Pass: "90%", Warning: "75%"},
		Comparison: &keptnevents.SLOComparison{CompareWith: "single_result", IncludeResultWithScore: "pass", NumberOfComparisonResults: 1, AggregateFunction: "avg"},
	}

	// if there is a dashboard management zone filter get them for both the queries as well as for the dashboard link
	dashboardManagementZoneFilter := ""
	mgmtZone := ""
	if dashboardJSON.DashboardMetadata.DashboardFilter != nil && dashboardJSON.DashboardMetadata.DashboardFilter.ManagementZone != nil {
		dashboardManagementZoneFilter = fmt.Sprintf(",mzId(%s)", dashboardJSON.DashboardMetadata.DashboardFilter.ManagementZone.ID)
		mgmtZone = ";gf=" + dashboardJSON.DashboardMetadata.DashboardFilter.ManagementZone.ID
	}

	// lets also generate the dashboard link for that timeframe (gtf=c_START_END) as well as management zone (gf=MZID) to pass back as label to Keptn
	dashboardLinkAsLabel := fmt.Sprintf("%s#dashboard;id=%s;gtf=c_%s_%s%s", ph.ApiURL, dashboardJSON.ID, common.TimestampToString(startUnix), common.TimestampToString(endUnix), mgmtZone)

	// Lets validate if we really need to process this dashboard as it might be the same (without change) from the previous runs
	// see https://github.com/keptn-contrib/dynatrace-sli-service/issues/92 for more details
	if !ph.HasDashboardChanged(keptnEvent, dashboardJSON, existingDashboardContent) {
		ph.Logger.Debug("Dashboard hasn't changed: skipping parsing of dashboard!")
		return dashboardLinkAsLabel, nil, nil, nil, nil, nil
	}

	ph.Logger.Debug("Dashboard has changed: reparsing it!")

	//
	// now lets iterate through the dashboard to find our SLIs
	for _, tile := range dashboardJSON.Tiles {
		if tile.TileType == "SYNTHETIC_TESTS" {
			// we dont do markdowns or synthetic tests
			continue
		}

		if tile.TileType == "MARKDOWN" {
			// we allow the user to use a markdown to specify SLI/SLO properties, e.g: KQG.Total.Pass
			// if we find KQG. we process the markdown
			if strings.Contains(tile.Markdown, "KQG.") {
				ParseMarkdownConfiguration(tile.Markdown, dashboardSLO)
			}

			continue
		}

		// custom chart and usql have different ways to define their tile names - so - lets figure it out by looking at the potential values
		tileTitle := tile.FilterConfig.CustomName // this is for all custom charts
		if tileTitle == "" {
			tileTitle = tile.CustomName
		}

		// first - lets figure out if this tile should be included in SLI validation or not - we parse the title and look for "sli=sliname"
		baseIndicatorName, passSLOs, warningSLOs, weight, keySli := ParsePassAndWarningFromString(tileTitle, []string{}, []string{})
		if baseIndicatorName == "" {
			ph.Logger.Debug(fmt.Sprintf("Chart Tile %s - NOT included as name doesnt include sli=SLINAME\n", tileTitle))
			continue
		}

		// only interested in custom charts
		if tile.TileType == "CUSTOM_CHARTING" {
			ph.Logger.Debug(fmt.Sprintf("Processing custom chart tile %s, sli=%s", tileTitle, baseIndicatorName))

			// we can potentially have multiple series on that chart
			for _, series := range tile.FilterConfig.ChartConfig.Series {

				// Lets query the metric definition as we need to know how many dimension the metric has
				metricDefinition, err := ph.ExecuteMetricAPIDescribe(series.Metric)
				if err != nil {
					ph.Logger.Debug(fmt.Sprintf("Error retrieving Metric Description for %s: %s\n", series.Metric, err.Error()))
					continue
				}

				// building the merge aggregator string, e.g: merge(1):merge(0) - or merge(0)
				metricDimensionCount := len(metricDefinition.DimensionDefinitions)
				metricAggregation := metricDefinition.DefaultAggregation.Type
				mergeAggregator := ""
				filterAggregator := ""
				filterSLIDefinitionAggregator := ""
				entitySelectorSLIDefinition := ""

				// now we need to merge all the dimensions that are not part of the series.dimensions, e.g: if the metric has two dimensions but only one dimension is used in the chart we need to merge the others
				// as multiple-merges are possible but as they are executed in sequence we have to use the right index
				for metricDimIx := metricDimensionCount - 1; metricDimIx >= 0; metricDimIx-- {
					doMergeDimension := true
					metricDimIxAsString := strconv.Itoa(metricDimIx)
					// lets check if this dimension is in the chart
					for _, seriesDim := range series.Dimensions {
						ph.Logger.Debug(fmt.Sprintf("seriesDim.id: %s; metricDimIx: %s\n", seriesDim.ID, metricDimIxAsString))
						if strings.Compare(seriesDim.ID, metricDimIxAsString) == 0 {
							// this is a dimension we want to keep and not merge
							ph.Logger.Debug(fmt.Sprintf("not merging dimension %s\n", metricDefinition.DimensionDefinitions[metricDimIx].Name))
							doMergeDimension = false

							// lets check if we need to apply a dimension filter
							// TODO: support multiple filters - right now we only support 1
							if len(seriesDim.Values) > 0 {
								filterAggregator = fmt.Sprintf(":filter(eq(%s,%s))", seriesDim.Name, seriesDim.Values[0])
							} else {
								// we need this for the generation of the SLI for each individual dimension value
								// if the dimension is a dt.entity we have to add an addiotnal entityId to the entitySelector - otherwise we add a filter for the dimension
								if strings.HasPrefix(seriesDim.Name, "dt.entity.") {
									entitySelectorSLIDefinition = fmt.Sprintf(",entityId(FILTERDIMENSIONVALUE)")
								} else {
									filterSLIDefinitionAggregator = fmt.Sprintf(":filter(eq(%s,FILTERDIMENSIONVALUE))", seriesDim.Name)
								}
							}
						}
					}

					if doMergeDimension {
						// this is a dimension we want to merge as it is not split by in the chart
						ph.Logger.Debug(fmt.Sprintf("merging dimension %s\n", metricDefinition.DimensionDefinitions[metricDimIx].Name))
						mergeAggregator = mergeAggregator + fmt.Sprintf(":merge(%d)", metricDimIx)
					}
				}

				// handle aggregation. If "NONE" is specified we go to the defaultAggregration
				if series.Aggregation != "NONE" {
					metricAggregation = series.Aggregation
				}
				// for percentile we need to specify the percentile itself
				if metricAggregation == "PERCENTILE" {
					metricAggregation = fmt.Sprintf("%s(%f)", metricAggregation, series.Percentile)
				}
				// for rate measures such as failure rate we take average if it is "OF_INTEREST_RATIO"
				if metricAggregation == "OF_INTEREST_RATIO" {
					metricAggregation = "avg"
				}
				// for rate measures charting also provides the "OTHER_RATIO" option which is the inverse
				// TODO: not supported via API - so we default to avg
				if metricAggregation == "OTHER_RATIO" {
					metricAggregation = "avg"
				}

				// TODO - handle aggregation rates -> probably doesnt make sense as we always evalute a short timeframe
				// if series.AggregationRate

				// lets get the true entity type as the one in the dashboard might not be accurate, e.g: IOT might be used instead of CUSTOM_DEVICE
				// so - if the metric definition has EntityTypes defined we take the first one
				entityType := series.EntityType
				if len(metricDefinition.EntityType) > 0 {
					entityType = metricDefinition.EntityType[0]
				}

				// Need to implement chart filters per entity type, e.g: its possible that a chart has a filter on entites or tags
				// lets see if we have a FiltersPerEntityType for the tiles EntityType
				entityTileFilter := ph.GetEntitySelectorFromEntityFilter(tile.FilterConfig.FiltersPerEntityType, entityType)

				// Check for tile management zone filter - this would overwrite the dashboardManagementZoneFilter
				tileManagementZoneFilter := dashboardManagementZoneFilter
				if tile.TileFilter.ManagementZone != nil {
					tileManagementZoneFilter = fmt.Sprintf(",mzId(%s)", tile.TileFilter.ManagementZone.ID)
				}

				// lets create the metricSelector and entitySelector
				// ATTENTION: adding :names so we also get the names of the dimensions and not just the entities. This means we get two values for each dimension
				metricQuery := fmt.Sprintf("metricSelector=%s%s%s:%s:names&entitySelector=type(%s)%s%s",
					series.Metric, mergeAggregator, filterAggregator, strings.ToLower(metricAggregation),
					entityType, entityTileFilter, tileManagementZoneFilter)

				// lets build the Dynatrace API Metric query for the proposed timeframe and additonal filters!
				fullMetricQuery, metricID := ph.BuildDynatraceMetricsQuery(metricQuery, startUnix, endUnix)

				// Lets run the Query and iterate through all data per dimension. Each Dimension will become its own indicator
				queryResult, err := ph.ExecuteMetricsAPIQuery(fullMetricQuery)
				if err != nil {
					ph.Logger.Debug(fmt.Sprintf("No result for query: %v", err))

					// ERROR-CASE: Metric API return no values or an error
					// we couldnt query data - so - we return the error back as part of our SLIResults
					sliResults = append(sliResults, &keptnevents.SLIResult{
						Metric:  baseIndicatorName,
						Value:   0,
						Success: false, // Mark as failure
						Message: err.Error(),
					})

					// add this to our SLI Indicator JSON in case we need to generate an SLI.yaml
					dashboardSLI.Indicators[baseIndicatorName] = metricQuery
				} else {
					// SUCCESS-CASE: we retrieved values - now we interate through the results and create an indicator result for every dimension
					for _, singleResult := range queryResult.Result {
						ph.Logger.Debug(fmt.Sprintf("Processing result for %s\n", singleResult.MetricID))
						if ph.isMatchingMetricID(singleResult.MetricID, metricID) {
							dataResultCount := len(singleResult.Data)
							if dataResultCount == 0 {
								ph.Logger.Debug(fmt.Sprintf("No data for this metric!\n"))
							}
							for _, singleDataEntry := range singleResult.Data {
								//
								// we need to generate the indicator name based on the base name + all dimensions, e.g: teststep_MYTESTSTEP, teststep_MYOTHERTESTSTEP
								// EXCEPTION: If there is only ONE data value then we skip this and just use the base SLI name
								indicatorName := baseIndicatorName

								metricQueryForSLI := metricQuery

								// we need this one to "fake" the MetricQuery for the SLi.yaml to include the dynamic dimension name for each value
								// we initialize it with ":names" as this is the part of the metric query string we will replace
								filterSLIDefinitionAggregatorValue := ":names"

								if dataResultCount > 1 {
									// because we use the ":names" transformation we always get two dimension names. First is the NAme, then the ID
									// lets first validate that we really received Dimension Names
									dimensionCount := len(singleDataEntry.Dimensions)
									dimensionIncrement := 2
									if dimensionCount != (len(series.Dimensions) * 2) {
										ph.Logger.Debug(fmt.Sprintf("DIDNT RECEIVE ID and Names. Lets assume we just received the dimension IDs"))
										dimensionIncrement = 1
									}

									// lets iterate through the list and get all names
									for dimIx := 0; dimIx < len(singleDataEntry.Dimensions); dimIx = dimIx + dimensionIncrement {
										dimensionName := singleDataEntry.Dimensions[dimIx]
										indicatorName = indicatorName + "_" + dimensionName

										filterSLIDefinitionAggregatorValue = ":names" + strings.Replace(filterSLIDefinitionAggregator, "FILTERDIMENSIONVALUE", dimensionName, 1)

										if entitySelectorSLIDefinition != "" && dimensionIncrement == 2 {
											dimensionEntityID := singleDataEntry.Dimensions[dimIx+1]
											metricQueryForSLI = metricQueryForSLI + strings.Replace(entitySelectorSLIDefinition, "FILTERDIMENSIONVALUE", dimensionEntityID, 1)
										}
									}
								}

								// make sure we have a valid indicator name by getting rid of special characters
								indicatorName = cleanIndicatorName(indicatorName)

								// calculating the value
								value := 0.0
								for _, singleValue := range singleDataEntry.Values {
									value = value + singleValue
								}
								value = value / float64(len(singleDataEntry.Values))

								// lets scale the metric
								value = scaleData(metricDefinition.MetricID, metricDefinition.Unit, value)

								// we got our metric, slos and the value

								ph.Logger.Debug(fmt.Sprintf("%s: %0.2f\n", indicatorName, value))

								// lets add the value to our SLIResult array
								sliResults = append(sliResults, &keptnevents.SLIResult{
									Metric:  indicatorName,
									Value:   value,
									Success: true,
								})

								// add this to our SLI Indicator JSON in case we need to generate an SLI.yaml
								// we use ":names" to find the right spot to add our custom dimension filter
								// we also "pre-pend" the metricDefinition.Unit - which allows us later on to do the scaling right
								dashboardSLI.Indicators[indicatorName] = fmt.Sprintf("MV2;%s;%s", metricDefinition.Unit, strings.Replace(metricQueryForSLI, ":names", filterSLIDefinitionAggregatorValue, 1))

								// lets add the SLO definitin in case we need to generate an SLO.yaml
								sloDefinition := &keptnevents.SLO{
									SLI:     indicatorName,
									Weight:  weight,
									KeySLI:  keySli,
									Pass:    passSLOs,
									Warning: warningSLOs,
								}
								dashboardSLO.Objectives = append(dashboardSLO.Objectives, sloDefinition)
							}
						} else {
							ph.Logger.Debug(fmt.Sprintf("Retrieving unintened metric %s while expecting %s\n", singleResult.MetricID, metricID))
						}
					}
				}
			}
		}

		// Dynatrace Query Language
		if tile.TileType == "DTAQL" {

			// for Dynatrace Query Language we currently support the following
			// SINGLE_VALUE: we just take the one value that comes back
			// PIE_CHART, COLUMN_CHART: we assume the first column is the dimension and the second column is the value column
			// TABLE: we assume the first column is the dimension and the last is the value

			usql := ph.BuildDynatraceUSQLQuery(tile.Query, startUnix, endUnix)
			usqlResult, err := ph.ExecuteUSQLQuery(usql)

			if err != nil {

			} else {

				for _, rowValue := range usqlResult.Values {
					dimensionName := ""
					dimensionValue := 0.0

					if tile.Type == "SINGLE_VALUE" {
						dimensionValue = rowValue[0].(float64)
					} else if tile.Type == "PIE_CHART" {
						dimensionName = rowValue[0].(string)
						dimensionValue = rowValue[1].(float64)
					} else if tile.Type == "COLUMN_CHART" {
						dimensionName = rowValue[0].(string)
						dimensionValue = rowValue[1].(float64)
					} else if tile.Type == "TABLE" {
						dimensionName = rowValue[0].(string)
						dimensionValue = rowValue[len(rowValue)-1].(float64)
					} else {
						ph.Logger.Debug(fmt.Sprintf("USQL Tile Type %s currently not supported!", tile.Type))
						continue
					}

					// lets scale the metric
					// value = scaleData(metricDefinition.MetricID, metricDefinition.Unit, value)

					// we got our metric, slos and the value
					indicatorName := baseIndicatorName
					if dimensionName != "" {
						indicatorName = indicatorName + "_" + dimensionName
					}

					ph.Logger.Debug(fmt.Sprintf("%s: %0.2f\n", indicatorName, dimensionValue))

					// lets add the value to our SLIResult array
					sliResults = append(sliResults, &keptnevents.SLIResult{
						Metric:  indicatorName,
						Value:   dimensionValue,
						Success: true,
					})

					// add this to our SLI Indicator JSON in case we need to generate an SLI.yaml
					// in that case we also need to mask it with USQL, TITLE_TYPE, DIMENSIONNAME
					dashboardSLI.Indicators[indicatorName] = fmt.Sprintf("USQL;%s;%s;%s", tile.Type, dimensionName, tile.Query)

					// lets add the SLO definitin in case we need to generate an SLO.yaml
					sloDefinition := &keptnevents.SLO{
						SLI:     indicatorName,
						Weight:  weight,
						KeySLI:  keySli,
						Pass:    passSLOs,
						Warning: warningSLOs,
					}
					dashboardSLO.Objectives = append(dashboardSLO.Objectives, sloDefinition)
				}
			}
		}
	}

	return dashboardLinkAsLabel, dashboardJSON, dashboardSLI, dashboardSLO, sliResults, nil
}

/**
 * GetSLIValue queries a single metric value from Dynatrace API
 * Can handle both Metric Queries as well as USQL
 */
func (ph *Handler) GetSLIValue(metric string, startUnix time.Time, endUnix time.Time) (float64, error) {

	// first we get the query from the SLI configuration based on its logical name
	metricsQuery, err := ph.getTimeseriesConfig(metric)
	if err != nil {
		return 0, fmt.Errorf("Error when fetching SLI config for %s %s\n", metric, err.Error())
	}
	ph.Logger.Debug(fmt.Sprintf("Retrieved SLI config for %s: %s", metric, metricsQuery))

	var (
		metricIDExists    = false
		actualMetricValue = 0.0
	)

	//
	// USQL: lets check whether this is USQL or regular Metric Query
	if strings.HasPrefix(metricsQuery, "USQL;") {
		// In this case we need to parse USQL;TILE_TYPE;DIMENSION;QUERY
		querySplits := strings.Split(metricsQuery, ";")
		if len(querySplits) != 4 {
			return 0, fmt.Errorf("USQL Query incorrect format: %s", metricsQuery)
		}

		tileName := querySplits[1]
		requestedDimensionName := querySplits[2]
		usqlRawQuery := querySplits[3]

		usql := ph.BuildDynatraceUSQLQuery(usqlRawQuery, startUnix, endUnix)
		usqlResult, err := ph.ExecuteUSQLQuery(usql)

		if err != nil {
			return 0, fmt.Errorf("Error executing USQL Query %v", err)
		}

		for _, rowValue := range usqlResult.Values {
			dimensionName := ""
			dimensionValue := 0.0

			if tileName == "SINGLE_VALUE" {
				dimensionValue = rowValue[0].(float64)
			} else if tileName == "PIE_CHART" {
				dimensionName = rowValue[0].(string)
				dimensionValue = rowValue[1].(float64)
			} else if tileName == "COLUMN_CHART" {
				dimensionName = rowValue[0].(string)
				dimensionValue = rowValue[1].(float64)
			} else if tileName == "TABLE" {
				dimensionName = rowValue[0].(string)
				dimensionValue = rowValue[len(rowValue)-1].(float64)
			} else {
				ph.Logger.Debug(fmt.Sprintf("USQL Tile Type %s currently not supported!", tileName))
				continue
			}

			// did we find the value we were looking for?
			if strings.Compare(dimensionName, requestedDimensionName) == 0 {
				metricIDExists = true
				actualMetricValue = dimensionValue
			}
		}
	} else {
		metricUnit := ""

		//
		// lets first start to query for the MV2 prefix, e.g: MV2;byte;actualQuery
		// if it starts with MV2 we extract metric unit and the actual query
		if strings.HasPrefix(metricsQuery, "MV2;") {
			metricsQuery = metricsQuery[4:]
			queryStartIndex := strings.Index(metricsQuery, ";")
			metricUnit = metricsQuery[:queryStartIndex]
			metricsQuery = metricsQuery[queryStartIndex+1:]
		}

		//
		// In this case we are querying regular MEtrics
		// now we are enriching it with all the additonal parameters, e.g: time, filters ...
		metricsQuery, metricID := ph.BuildDynatraceMetricsQuery(metricsQuery, startUnix, endUnix)
		result, err := ph.ExecuteMetricsAPIQuery(metricsQuery)

		if err != nil {
			return 0, fmt.Errorf("error from Execute Metrics API Query: %s\n", err.Error())
		}

		if result != nil {
			for _, i := range result.Result {

				if ph.isMatchingMetricID(i.MetricID, metricID) {
					metricIDExists = true

					if len(i.Data) != 1 {
						jsonString, _ := json.Marshal(i)
						return 0, fmt.Errorf("Dynatrace Metrics API returned %d result values, expected 1. Please ensure the response contains exactly one value (e.g., by using :merge(0):avg for the metric). Here is the output for troubleshooting: %s", len(i.Data), string(jsonString))
					}

					actualMetricValue = i.Data[0].Values[0]
					break
				}
			}
		}

		actualMetricValue = scaleData(metricID, metricUnit, actualMetricValue)
	}

	if !metricIDExists {
		return 0, fmt.Errorf("Not able to query identifier %s from Dynatrace", metric)
	}

	return actualMetricValue, nil
}

// scaleData
// scales data based on the timeseries identifier (e.g., service.responsetime needs to be scaled from microseconds to milliseocnds)
// Right now this method scales microseconds to milliseconds and bytes to Kilobytes
// At a later stage we should extend this with more conversions and even think of allowing custom scale targets, e.g: Byte to MegaByte
func scaleData(metricID string, unit string, value float64) float64 {
	if (strings.Compare(unit, "MicroSecond") == 0) || strings.Contains(metricID, "builtin:service.response.time") {
		// scale from microseconds to milliseconds
		return value / 1000.0
	}

	// convert Bytes to Kilobyte
	if strings.Compare(unit, "Byte") == 0 {
		return value / 1024
	}

	/*
		if strings.Compare(unit, "NanoSecond") {

		}
	*/

	return value
}

func (ph *Handler) replaceQueryParameters(query string) string {
	// apply customfilters
	for _, filter := range ph.CustomFilters {
		filter.Value = strings.Replace(filter.Value, "'", "", -1)
		filter.Value = strings.Replace(filter.Value, "\"", "", -1)

		// replace the key in both variants, "normal" and uppercased
		query = strings.Replace(query, "$"+filter.Key, filter.Value, -1)
		query = strings.Replace(query, "$"+strings.ToUpper(filter.Key), filter.Value, -1)
	}

	// apply default values
	/* query = strings.Replace(query, "$PROJECT", ph.Project, -1)
	query = strings.Replace(query, "$STAGE", ph.Stage, -1)
	query = strings.Replace(query, "$SERVICE", ph.Service, -1)
	query = strings.Replace(query, "$DEPLOYMENT", ph.Deployment, -1)*/

	query = common.ReplaceKeptnPlaceholders(query, ph.KeptnEvent)

	return query
}

// based on the requested metric a dynatrace timeseries with its aggregation type is returned
func (ph *Handler) getTimeseriesConfig(metric string) (string, error) {
	if val, ok := ph.CustomQueries[metric]; ok {
		return val, nil
	}

	// default SLI configs
	// Switched to new metric v2 query langugae as discussed here: https://github.com/keptn-contrib/dynatrace-sli-service/issues/91
	switch metric {
	case Throughput:
		return "metricSelector=builtin:service.requestCount.total:merge(0):sum&entitySelector=type(SERVICE),tag(keptn_project:$PROJECT),tag(keptn_stage:$STAGE),tag(keptn_service:$SERVICE),tag(keptn_deployment:$DEPLOYMENT)", nil
	case ErrorRate:
		return "metricSelector=builtin:service.errors.total.count:merge(0):avg&entitySelector=type(SERVICE),tag(keptn_project:$PROJECT),tag(keptn_stage:$STAGE),tag(keptn_service:$SERVICE),tag(keptn_deployment:$DEPLOYMENT)", nil
	case ResponseTimeP50:
		return "metricSelector=builtin:service.response.time:merge(0):percentile(50)&entitySelector=type(SERVICE),tag(keptn_project:$PROJECT),tag(keptn_stage:$STAGE),tag(keptn_service:$SERVICE),tag(keptn_deployment:$DEPLOYMENT)", nil
	case ResponseTimeP90:
		return "metricSelector=builtin:service.response.time:merge(0):percentile(90)&entitySelector=type(SERVICE),tag(keptn_project:$PROJECT),tag(keptn_stage:$STAGE),tag(keptn_service:$SERVICE),tag(keptn_deployment:$DEPLOYMENT)", nil
	case ResponseTimeP95:
		return "metricSelector=builtin:service.response.time:merge(0):percentile(95)&entitySelector=type(SERVICE),tag(keptn_project:$PROJECT),tag(keptn_stage:$STAGE),tag(keptn_service:$SERVICE),tag(keptn_deployment:$DEPLOYMENT)", nil
	default:
		return "", fmt.Errorf("unsupported SLI metric %s", metric)
	}
}

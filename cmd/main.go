package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/keptn-contrib/dynatrace-sli-service/pkg/common"
	"github.com/keptn-contrib/dynatrace-sli-service/pkg/lib/dynatrace"

	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/client"
	cloudeventshttp "github.com/cloudevents/sdk-go/pkg/cloudevents/transport/http"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/types"

	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"

	"gopkg.in/yaml.v2"

	configutils "github.com/keptn/go-utils/pkg/configuration-service/utils"
	keptnevents "github.com/keptn/go-utils/pkg/events"
	keptnutils "github.com/keptn/go-utils/pkg/utils"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

const eventbroker = "EVENTBROKER"
const configservice = "CONFIGURATION_SERVICE"
const sliResourceURI = "dynatrace/sli.yaml"

type envConfig struct {
	// Port on which to listen for cloudevents
	Port int    `envconfig:"RCV_PORT" default:"8080"`
	Path string `envconfig:"RCV_PATH" default:"/"`
}

func main() {
	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		log.Fatalf("Failed to process env var: %s", err)
	}

	if common.RunLocal || common.RunLocalTest {
		log.Println("env=runlocal: Running with local filesystem to fetch resources")
	}

	os.Exit(_main(os.Args[1:], env))
}

func _main(args []string, env envConfig) int {

	ctx := context.Background()

	t, err := cloudeventshttp.New(
		cloudeventshttp.WithPort(env.Port),
		cloudeventshttp.WithPath(env.Path),
	)

	if err != nil {
		log.Fatalf("failed to create transport, %v", err)
	}
	c, err := client.New(t)
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	log.Fatalf("failed to start receiver: %s", c.StartReceiver(ctx, gotEvent))

	return 0
}

func gotEvent(ctx context.Context, event cloudevents.Event) error {

	switch event.Type() {
	case keptnevents.InternalGetSLIEventType:
		return retrieveMetrics(event)
	default:
		return errors.New("received unknown event type")
	}
}

func retrieveMetrics(event cloudevents.Event) error {
	var shkeptncontext string
	event.Context.ExtensionAs("shkeptncontext", &shkeptncontext)
	eventData := &keptnevents.InternalGetSLIEventData{}
	err := event.DataAs(eventData)

	if err != nil {
		return err
	}

	// do not continue if SLIProvider is not dynatrace
	if eventData.SLIProvider != "dynatrace" {
		return nil
	}

	stdLogger := keptnutils.NewLogger(shkeptncontext, event.Context.GetID(), "dynatrace-sli-service")
	stdLogger.Info("Retrieving Dynatrace timeseries metrics")

	kubeClient, err := keptnutils.GetKubeAPI(true)
	if err != nil && !(common.RunLocal || common.RunLocalTest) {
		stdLogger.Error("could not create Kubernetes client")
		return errors.New("could not create Kubernetes client")
	}

	//
	// see if there is a dynatrace.conf.yaml
	keptnEvent := &common.BaseKeptnEvent{}
	keptnEvent.Project = eventData.Project
	keptnEvent.Stage = eventData.Stage
	keptnEvent.Service = eventData.Service
	keptnEvent.TestStrategy = eventData.TestStrategy
	keptnEvent.Labels = eventData.Labels
	keptnEvent.Context = shkeptncontext
	dynatraceConfigFile, _ := common.GetDynatraceConfig(keptnEvent, stdLogger)

	dtCreds := ""
	if dynatraceConfigFile != nil {
		dtCreds = dynatraceConfigFile.DtCreds
		stdLogger.Debug("Found dynatrace.conf.yaml with DTCreds: " + dtCreds)
	}
	dtCredentials, err := getDynatraceCredentials(dtCreds, eventData.Project, kubeClient, stdLogger)

	if err != nil {
		stdLogger.Debug(err.Error())
		stdLogger.Debug("Failed to fetch Dynatrace credentials, exiting.")
		return err
	}

	stdLogger.Info("Dynatrace credentials (Tenant, Token) received. Getting global custom queries ...")

	// get custom metrics for project
	projectCustomQueries, err := getCustomQueries(eventData.Project, eventData.Stage, eventData.Service, kubeClient, stdLogger)
	if err != nil {
		stdLogger.Error("Failed to get custom queries for project " + eventData.Project)
		stdLogger.Error(err.Error())
		return err
	}

	dynatraceHandler := dynatrace.NewDynatraceHandler(dtCredentials.Tenant,
		keptnEvent,
		map[string]string{
			"Authorization": "Api-Token " + dtCredentials.ApiToken,
		},
		eventData.CustomFilters,
	)

	if projectCustomQueries != nil {
		dynatraceHandler.CustomQueries = projectCustomQueries
	}

	if err != nil {
		return err
	}

	if dynatraceHandler == nil {
		stdLogger.Error("failed to get Dynatrace handler")
		return nil
	}

	// create a new CloudEvent to store SLI Results in
	var sliResults []*keptnevents.SLIResult

	// query all indicators
	for _, indicator := range eventData.Indicators {
		stdLogger.Info("Fetching indicator: " + indicator)
		sliValue, err := dynatraceHandler.GetSLIValue(indicator, eventData.Start, eventData.End, eventData.CustomFilters)
		if err != nil {
			stdLogger.Error(err.Error())
			// failed to fetch metric
			sliResults = append(sliResults, &keptnevents.SLIResult{
				Metric:  indicator,
				Value:   0,
				Success: false, // Mark as failure
				Message: err.Error(),
			})
		} else {
			// successfully fetched metric
			sliResults = append(sliResults, &keptnevents.SLIResult{
				Metric:  indicator,
				Value:   sliValue,
				Success: true, // mark as success
			})
		}
	}

	log.Println("Finished fetching metrics; Sending event now ...")

	if common.RunLocal || common.RunLocalTest {
		log.Println("(RunLocal Output) Here are the results:")
		for _, v := range sliResults {
			log.Println(fmt.Sprintf("%s:%.2f - Success: %t - Error: %s", v.Metric, v.Value, v.Success, v.Message))
		}
		return nil
	}

	return sendInternalGetSLIDoneEvent(shkeptncontext, eventData.Project, eventData.Service, eventData.Stage,
		sliResults, eventData.Start, eventData.End, eventData.TestStrategy, eventData.DeploymentStrategy,
		eventData.Deployment, eventData.Labels)
}

/**
 * Loads SLIs from a local file and adds it to the SLI map
 */
func addResourceContentToSLIMap(SLIs map[string]string, sliFilePath string, logger *keptnutils.Logger) (map[string]string, error) {
	localFileContent, err := ioutil.ReadFile(sliFilePath)
	if err != nil {
		logMessage := fmt.Sprintf("Couldn't load file content from %s", sliFilePath)
		logger.Info(logMessage)
		return nil, nil
	}
	logger.Info("Loaded LOCAL file " + sliFilePath)
	fileContent := string(localFileContent)

	if fileContent != "" {
		sliConfig := configutils.SLIConfig{}
		err := yaml.Unmarshal([]byte(fileContent), &sliConfig)
		if err != nil {
			return nil, err
		}

		for key, value := range sliConfig.Indicators {
			SLIs[key] = value
		}
	}
	return SLIs, nil
}

// getCustomQueries returns custom queries as stored in configuration store
func getCustomQueries(project string, stage string, service string, kubeClient v1.CoreV1Interface, logger *keptnutils.Logger) (map[string]string, error) {
	logger.Info("Checking for custom SLI queries")

	if common.RunLocal || common.RunLocalTest {
		var sliMap = map[string]string{}
		sliMap, _ = addResourceContentToSLIMap(sliMap, "dynatrace/sli.yaml", logger)
		return sliMap, nil
	}

	endPoint, err := getServiceEndpoint(configservice)
	if err != nil {
		return nil, errors.New("Failed to retrieve endpoint of configuration-service. %s" + err.Error())
	}

	resourceHandler := configutils.NewResourceHandler(endPoint.String())
	customQueries, err := resourceHandler.GetSLIConfiguration(project, stage, service, sliResourceURI)
	if err != nil {
		return nil, err
	}

	return customQueries, nil
}

/**
 * returns the DTCredentials
 * First looks at the passed secretName. If null validates if there is a dynatrace-credentials-%PROJECT% - if not - defaults to "dynatrace" global secret
 */
func getDynatraceCredentials(secretName string, project string, kubeClient v1.CoreV1Interface, logger *keptnutils.Logger) (*common.DTCredentials, error) {

	secretNames := []string{secretName, fmt.Sprintf("dynatrace-credentials-%s", project), "dynatrace-credentials", "dynatrace"}

	for _, secret := range secretNames {
		logger.Info(fmt.Sprintf("Trying to fetch secret containing Dynatrace credentials with name '%s'", secret))
		dtCredentials, err := common.GetDTCredentials(secret)

		// write in log if fetching Dynatrace Credentials failed
		if err != nil {
			logger.Error(fmt.Sprintf("Error fetching secret containing Dynatrace credentials with name '%s': %s", secret, err.Error()))
		}

		if dtCredentials != nil {
			logger.Info(" -> credentials found, returning...")
			return dtCredentials, nil
		}
	}

	logger.Error("No Dynatrace credentials found in namespace keptn")

	return nil, errors.New("Couldn't find any dynatrace specific secrets in namespace keptn")
}

func sendInternalGetSLIDoneEvent(shkeptncontext string, project string,
	service string, stage string, indicatorValues []*keptnevents.SLIResult, start string, end string,
	teststrategy string, deploymentStrategy string, deployment string, labels map[string]string) error {

	source, _ := url.Parse("dynatrace-sli-service")
	contentType := "application/json"

	getSLIEvent := keptnevents.InternalGetSLIDoneEventData{
		Project:            project,
		Service:            service,
		Stage:              stage,
		IndicatorValues:    indicatorValues,
		Start:              start,
		End:                end,
		TestStrategy:       teststrategy,
		DeploymentStrategy: deploymentStrategy,
		Deployment:         deployment,
		Labels:             labels,
	}
	event := cloudevents.Event{
		Context: cloudevents.EventContextV02{
			ID:          uuid.New().String(),
			Time:        &types.Timestamp{Time: time.Now()},
			Type:        keptnevents.InternalGetSLIDoneEventType,
			Source:      types.URLRef{URL: *source},
			ContentType: &contentType,
			Extensions:  map[string]interface{}{"shkeptncontext": shkeptncontext},
		}.AsV02(),
		Data: getSLIEvent,
	}

	return sendEvent(event)
}

func sendEvent(event cloudevents.Event) error {
	endPoint, err := getServiceEndpoint(eventbroker)
	if err != nil {
		return errors.New("Failed to retrieve endpoint of eventbroker. %s" + err.Error())
	}

	if endPoint.Host == "" {
		return errors.New("Host of eventbroker not set")
	}

	transport, err := cloudeventshttp.New(
		cloudeventshttp.WithTarget(endPoint.String()),
		cloudeventshttp.WithEncoding(cloudeventshttp.StructuredV02),
	)
	if err != nil {
		return errors.New("Failed to create transport:" + err.Error())
	}

	c, err := client.New(transport)
	if err != nil {
		return errors.New("Failed to create HTTP client:" + err.Error())
	}

	if _, err := c.Send(context.Background(), event); err != nil {
		return errors.New("Failed to send cloudevent:, " + err.Error())
	}
	return nil
}

// getServiceEndpoint gets an endpoint stored in an environment variable and sets http as default scheme
func getServiceEndpoint(service string) (url.URL, error) {
	url, err := url.Parse(os.Getenv(service))
	if err != nil {
		return *url, fmt.Errorf("Failed to retrieve value from ENVIRONMENT_VARIABLE: %s", service)
	}

	if url.Scheme == "" {
		url.Scheme = "http"
	}

	return *url, nil
}

package common

import (
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"	
)

var RunLocal = (os.Getenv("env") == "runlocal")
var RunLocalTest = (os.Getenv("env") == "runlocaltest")

/**
 * Defines the Dynatrace Configuration File structure!
 */
const DynatraceConfigFilename = "dynatrace/dynatrace.conf.yaml"
const DynatraceConfigFilenameLOCAL = "dynatrace/_dynatrace.conf.yaml"
type DynatraceConfigFile struct {
	SpecVersion string `json:"spec_version" yaml:"spec_version"`
	DtCreds     string `json:"dtCreds",omitempty yaml:"dtCreds",omitempty`
}

type DTCredentials struct {
	Tenant    string `json:"DT_TENANT" yaml:"DT_TENANT"`
	ApiToken  string `json:"DT_API_TOKEN" yaml:"DT_API_TOKEN"`
	PaaSToken string `json:"DT_PAAS_TOKEN" yaml:"DT_PAAS_TOKEN"`
}

type baseKeptnEvent struct {
	context string
	source  string
	event   string

	project            string
	stage              string
	service            string
	deployment         string
	testStrategy       string
	deploymentStrategy string

	image string
	tag   string

	labels map[string]string
}

func GetKubernetesClient() (*kubernetes.Clientset, error) {
	if RunLocal || RunLocalTest {
		return nil, nil
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

/**
 * Returns the Keptn Domain stored in the keptn-domainconfigmap
 */
func GetKeptnDomain() (string, error) {
	kubeAPI, err := GetKubernetesClient()
	if kubeAPI == nil || err != nil {
		return "", err
	}

	keptnDomainCM, errCM := kubeAPI.CoreV1().ConfigMaps("keptn").Get("keptn-domain", metav1.GetOptions{})
	if errCM != nil {
		return "", errors.New("Could not retrieve keptn-domain ConfigMap: " + errCM.Error())
	}

	keptnDomain := keptnDomainCM.Data["app_domain"]
	return keptnDomain, nil
}


//
// replaces $ placeholders with actual values
// $CONTEXT, $EVENT, $SOURCE
// $PROJECT, $STAGE, $SERVICE, $DEPLOYMENT
// $TESTSTRATEGY
// $LABEL.XXXX  -> will replace that with a label called XXXX
// $ENV.XXXX    -> will replace that with an env variable called XXXX
// $SECRET.YYYY -> will replace that with the k8s secret called YYYY
//
func replaceKeptnPlaceholders(input string, keptnEvent *baseKeptnEvent) string {
	result := input

	// first we do the regular keptn values
	result = strings.Replace(result, "$CONTEXT", keptnEvent.context, -1)
	result = strings.Replace(result, "$EVENT", keptnEvent.event, -1)
	result = strings.Replace(result, "$SOURCE", keptnEvent.source, -1)
	result = strings.Replace(result, "$PROJECT", keptnEvent.project, -1)
	result = strings.Replace(result, "$STAGE", keptnEvent.stage, -1)
	result = strings.Replace(result, "$SERVICE", keptnEvent.service, -1)
	result = strings.Replace(result, "$DEPLOYMENT", keptnEvent.deployment, -1)
	result = strings.Replace(result, "$TESTSTRATEGY", keptnEvent.testStrategy, -1)

	// now we do the labels
	for key, value := range keptnEvent.labels {
		result = strings.Replace(result, "$LABEL."+key, value, -1)
	}

	// now we do all environment variables

//
// Loads dynatrace.conf for the current service
//
func getDynatraceConfig(keptnEvent *baseKeptnEvent, logger *keptn.Logger) (*DynatraceConfigFile, error) {

	logger.Info("Loading dynatrace.conf.yaml")
	// if we run in a runlocal mode we are just getting the file from the local disk
	var fileContent string
	if common.RunLocal {
		localFileContent, err := ioutil.ReadFile(DynatraceConfigFilenameLOCAL)
		if err != nil {
			logMessage := fmt.Sprintf("No %s file found LOCALLY for service %s in stage %s in project %s", DynatraceConfigFilenameLOCAL, keptnEvent.service, keptnEvent.stage, keptnEvent.project)
			logger.Info(logMessage)
			return nil, nil
		}
		logger.Info("Loaded LOCAL file " + DynatraceConfigFilenameLOCAL)
		fileContent = string(localFileContent)
	} else {
		resourceHandler := utils.NewResourceHandler("configuration-service:8080")

		// Lets search on SERVICE-LEVEL
		keptnResourceContent, err := resourceHandler.GetServiceResource(keptnEvent.project, keptnEvent.stage, keptnEvent.service, DynatraceConfigFilename)
		if err != nil || keptnResourceContent == nil || keptnResourceContent.ResourceContent == "" {
			// Lets search on STAGE-LEVEL
			keptnResourceContent, err = resourceHandler.GetStageResource(keptnEvent.project, keptnEvent.stage, DynatraceConfigFilename)
			if err != nil || keptnResourceContent == nil || keptnResourceContent.ResourceContent == "" {
				// Lets search on PROJECT-LEVEL
				keptnResourceContent, err = resourceHandler.GetProjectResource(keptnEvent.project, DynatraceConfigFilename)
				if err != nil || keptnResourceContent == nil || keptnResourceContent.ResourceContent == "" {
					logger.Debug(fmt.Sprintf("No Keptn Resource found: %s/%s/%s/%s - %s", keptnEvent.project, keptnEvent.stage, keptnEvent.service, DynatraceConfigFilename, err))
					return nil, err
				}

				logger.Debug("Found " + DynatraceConfigFilename + " on project level")
			} else {
				logger.Debug("Found " + DynatraceConfigFilename + " on stage level")
			}
		} else {
			logger.Debug("Found " + DynatraceConfigFilename + " on service level")
		}
		fileContent = keptnResourceContent.ResourceContent
	}

	// unmarshal the file
	dynatraceConfFile, err := parseDynatraceConfigFile([]byte(fileContent))

	if err != nil {
		logMessage := fmt.Sprintf("Couldn't parse %s file found for service %s in stage %s in project %s. Error: %s", DynatraceConfigFilename, keptnEvent.service, keptnEvent.stage, keptnEvent.project, err.Error())
		logger.Error(logMessage)
		return nil, errors.New(logMessage)
	}

	logMessage := fmt.Sprintf("Loaded Config from dynatrace.conf.yaml:  %s", dynatraceConfFile)
	logger.Info(logMessage)

	return dynatraceConfFile, nil
}

func parseDynatraceConfigFile(input []byte) (*DynatraceConfigFile, error) {
	dynatraceConfFile := &DynatraceConfigFile{}
	err := yaml.Unmarshal([]byte(input), &dynatraceConfFile)

	if err != nil {
		return nil, err
	}

	return dynatraceConfFile, nil
}

/**
 * Pulls the Dynatrace Credentials from the passed secret
 */
 func (dt *DynatraceHelper) GetDTCredentials(dynatraceSecretName string) (*DTCredentials, error) {
	if dynatraceSecretName == "" {
		return nil, nil
	}

	dtCreds := &DTCredentials{}
	if common.RunLocal || common.RunLocalTest {
		dtCreds.Tenant = os.Getenv("DT_TENANT")
		dtCreds.ApiToken = os.Getenv("DT_API_TOKEN")
		dtCreds.PaaSToken = os.Getenv("DT_PAAS_TOKEN")
	} else {
		kubeAPI, err := common.GetKubernetesClient()
		if err != nil {
			return nil, err
		}
		secret, err := kubeAPI.CoreV1().Secrets("keptn").Get(dynatraceSecretName, metav1.GetOptions{})
	
		if err != nil {
			return nil, err
		}
	
		if string(secret.Data["DT_TENANT"]) == "" || string(secret.Data["DT_API_TOKEN"]) == "" || string(secret.Data["DT_PAAS_TOKEN"]) == "" {
			return nil, errors.New("invalid or no Dynatrace credentials found")
		}
	
		dtCreds.Tenant = string(secret.Data["DT_TENANT"])
		dtCreds.ApiToken = string(secret.Data["DT_API_TOKEN"])
		dtCreds.PaaSToken = string(secret.Data["DT_PAAS_TOKEN"])	
	}

	// ensure URL always has http or https in front
	if strings.HasPrefix(dtCreds.Tenant, "https://") || strings.HasPrefix(dtCreds.Tenant, "http://") {
		dtCreds.Tenant = dtCreds.Tenant
	} else {
		dtCreds.Tenant = "https://" + dtCreds.Tenant
	}	

	return dtCreds, nil
}

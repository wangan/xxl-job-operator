package xxlclient

import "encoding/json"

const (
	// SuccessCode is XXL-JOB's standard HTTP/ReturnT success code.
	SuccessCode = 200
	// FailCode is XXL-JOB's standard failure code.
	FailCode = 500
	// TimeoutCode is XXL-JOB 2.4.1's standard timeout callback code (IJobHandler.FAIL_TIMEOUT = 502).
	TimeoutCode = 502
	// AccessTokenHeader is the header key used by XXL-JOB OpenAPI.
	AccessTokenHeader = "XXL-JOB-ACCESS-TOKEN"
	// RegistryGroupExecutor is the default registryGroup value for executors.
	RegistryGroupExecutor = "EXECUTOR"
	// DefaultUnavailableSentinelAddress is written to xxl_job_group.address_list when addressType=1
	// has zero ready pods, preventing XXL-JOB 2.4 from rejecting empty manual addressList or falling
	// back to xxl_job_registry where terminating pods may still have unexpired heartbeats.
	DefaultUnavailableSentinelAddress = "http://xxl-job-operator-proxy.xxl-job-system.svc.cluster.local:9999/proxy/_unavailable/"
)

// ReturnT represents XXL-JOB's generic API response wrapper.
type ReturnT struct {
	Code    int             `json:"code"`
	Msg     string          `json:"msg"`
	Content json.RawMessage `json:"content,omitempty"`
}

// IsSuccess returns true if Code == 200.
func (r ReturnT) IsSuccess() bool {
	return r.Code == SuccessCode
}

// RegistryParam is the payload for /api/registry and /api/registryRemove.
type RegistryParam struct {
	RegistryGroup string `json:"registryGroup"`
	RegistryKey   string `json:"registryKey"`
	RegistryValue string `json:"registryValue"`
}

// HandleCallbackParam is an item in the slice sent to /api/callback.
type HandleCallbackParam struct {
	LogID         int64  `json:"logId"`
	LogDateTim    int64  `json:"logDateTim"`
	HandleCode    int    `json:"handleCode"`
	HandleMsg     string `json:"handleMsg,omitempty"`
	ExecuteResult string `json:"executeResult,omitempty"` // Compatible with both 2.3.x and 2.4.x+
}

// TriggerParam is the payload sent by XXL-JOB Admin to Executor POST /run.
type TriggerParam struct {
	JobID                 int    `json:"jobId"`
	ExecutorHandler       string `json:"executorHandler"`
	ExecutorParams        string `json:"executorParams"`
	ExecutorBlockStrategy string `json:"executorBlockStrategy"`
	ExecutorTimeout       int    `json:"executorTimeout"`
	LogID                 int64  `json:"logId"`
	LogDateTime           int64  `json:"logDateTime"`
	GlueType              string `json:"glueType"`
	GlueSource            string `json:"glueSource"`
	GlueUpdatetime        int64  `json:"glueUpdatetime"`
	BroadcastIndex        int    `json:"broadcastIndex"`
	BroadcastTotal        int    `json:"broadcastTotal"`
}

// IdleBeatParam is the payload sent by XXL-JOB Admin to Executor POST /idleBeat.
type IdleBeatParam struct {
	JobID int `json:"jobId"`
}

// KillParam is the payload sent by XXL-JOB Admin to Executor POST /kill.
type KillParam struct {
	JobID int `json:"jobId"`
}

// LogParam is the payload sent by XXL-JOB Admin to Executor POST /log.
type LogParam struct {
	LogDateTim  int64 `json:"logDateTim"`
	LogID       int64 `json:"logId"`
	FromLineNum int   `json:"fromLineNum"`
}

// LogResult is the response content returned by Executor POST /log.
type LogResult struct {
	FromLineNum int    `json:"fromLineNum"`
	ToLineNum   int    `json:"toLineNum"`
	LogContent  string `json:"logContent"`
	IsEnd       bool   `json:"isEnd"`
}

// JobGroup represents an executor group (xxl_job_group) in XXL-JOB Admin.
type JobGroup struct {
	ID           int      `json:"id"`
	AppName      string   `json:"appname"`
	Title        string   `json:"title"`
	AddressType  int      `json:"addressType"` // 0=自动注册, 1=手动录入
	AddressList  string   `json:"addressList"` // comma-separated addresses
	RegistryList []string `json:"registryList,omitempty"`
}

// JobGroupPageResult wraps the response of /jobgroup/pageList.
type JobGroupPageResult struct {
	RecordsTotal    int        `json:"recordsTotal"`
	RecordsFiltered int        `json:"recordsFiltered"`
	Data            []JobGroup `json:"data"`
}

// JobInfo represents a scheduled task (xxl_job_info) in XXL-JOB Admin.
type JobInfo struct {
	ID                     int    `json:"id"`
	JobGroup               int    `json:"jobGroup"`
	JobDesc                string `json:"jobDesc"`
	Author                 string `json:"author"`
	AlarmEmail             string `json:"alarmEmail"`
	ScheduleType           string `json:"scheduleType"`
	ScheduleConf           string `json:"scheduleConf"`
	MisfireStrategy        string `json:"misfireStrategy"`
	ExecutorRouteStrategy  string `json:"executorRouteStrategy"`
	ExecutorHandler        string `json:"executorHandler"`
	ExecutorParam          string `json:"executorParam"`
	ExecutorBlockStrategy  string `json:"executorBlockStrategy"`
	ExecutorTimeout        int    `json:"executorTimeout"`
	ExecutorFailRetryCount int    `json:"executorFailRetryCount"`
	GlueType               string `json:"glueType"`
	GlueSource             string `json:"glueSource"`
	GlueRemark             string `json:"glueRemark"`
	ChildJobID             string `json:"childJobId"`
	TriggerStatus          int    `json:"triggerStatus"` // 0=STOP, 1=RUNNING
	AllowGlueTypeRecreate  bool   `json:"-"`             // Requires explicit confirmation before delete-and-recreate on glueType change
	PreviousJobGroup       int    `json:"-"`             // Previous JobGroup ID recorded in XxlJobTask.Status for safe scoped group migration
	PreviousAdminURL       string `json:"-"`             // Previous AdminURL recorded in XxlJobTask.Status to prevent cross-Admin ID takeover
	OwnerKey               string `json:"-"`             // Kubernetes owner tag (e.g. "<ns>/<name>") embedded in jobDesc to prevent hijacking
	AdoptJobID             int    `json:"-"`             // Optional explicit jobId from xxljob.io/adopt-job-id annotation for adopting an existing unmarked job
}

// JobInfoPageResult wraps the response of /jobinfo/pageList.
type JobInfoPageResult struct {
	RecordsTotal    int       `json:"recordsTotal"`
	RecordsFiltered int       `json:"recordsFiltered"`
	Data            []JobInfo `json:"data"`
}

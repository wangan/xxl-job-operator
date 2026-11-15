package xxlclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockAdminState struct {
	mu            sync.Mutex
	accessToken   string
	groups        map[int]JobGroup
	nextGroupID   int
	jobs          map[int]JobInfo
	nextJobID     int
	registries    map[string]map[string]bool
	callbacks     []HandleCallbackParam
	updateCalls   int
	pageListCalls int
	failPageList  bool
	actionLog     []string
}

func newMockAdminServer(t *testing.T, accessToken string) (*httptest.Server, *mockAdminState) {
	t.Helper()
	state := &mockAdminState{
		accessToken: accessToken,
		groups:      make(map[int]JobGroup),
		nextGroupID: 1,
		jobs:        make(map[int]JobInfo),
		nextJobID:   100,
		registries:  make(map[string]map[string]bool),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.FormValue("userName") != "admin" || r.FormValue("password") != "123456" {
			_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "invalid credentials"})
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "XXL_JOB_LOGIN_IDENTITY", Value: "mock-session-token", Path: "/"})
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	checkCookie := func(w http.ResponseWriter, r *http.Request) bool {
		c, err := r.Cookie("XXL_JOB_LOGIN_IDENTITY")
		if err != nil || c.Value != "mock-session-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return false
		}
		return true
	}

	checkToken := func(w http.ResponseWriter, r *http.Request) bool {
		if state.accessToken != "" && r.Header.Get(AccessTokenHeader) != state.accessToken {
			_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "invalid access token"})
			return false
		}
		return true
	}

	mux.HandleFunc("/api/registry", func(w http.ResponseWriter, r *http.Request) {
		if !checkToken(w, r) {
			return
		}
		var param RegistryParam
		_ = json.NewDecoder(r.Body).Decode(&param)
		state.mu.Lock()
		if state.registries[param.RegistryKey] == nil {
			state.registries[param.RegistryKey] = make(map[string]bool)
		}
		state.registries[param.RegistryKey][param.RegistryValue] = true
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/api/registryRemove", func(w http.ResponseWriter, r *http.Request) {
		if !checkToken(w, r) {
			return
		}
		var param RegistryParam
		_ = json.NewDecoder(r.Body).Decode(&param)
		state.mu.Lock()
		if state.registries[param.RegistryKey] != nil {
			delete(state.registries[param.RegistryKey], param.RegistryValue)
		}
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/api/callback", func(w http.ResponseWriter, r *http.Request) {
		if !checkToken(w, r) {
			return
		}
		var params []HandleCallbackParam
		_ = json.NewDecoder(r.Body).Decode(&params)
		state.mu.Lock()
		state.callbacks = append(state.callbacks, params...)
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/jobgroup/pageList", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		appName := r.FormValue("appname")
		start, _ := strconv.Atoi(r.FormValue("start"))
		length, _ := strconv.Atoi(r.FormValue("length"))
		if length <= 0 {
			length = 100
		}
		state.mu.Lock()
		state.pageListCalls++
		var ids []int
		for id := range state.groups {
			ids = append(ids, id)
		}
		for i := 0; i < len(ids); i++ {
			for k := i + 1; k < len(ids); k++ {
				if ids[k] < ids[i] {
					ids[i], ids[k] = ids[k], ids[i]
				}
			}
		}
		var list []JobGroup
		for _, id := range ids {
			g := state.groups[id]
			if appName == "" || strings.Contains(g.AppName, appName) {
				list = append(list, g)
			}
		}
		total := len(list)
		var page []JobGroup
		if start < total {
			end := start + length
			if end > total {
				end = total
			}
			page = list[start:end]
		}
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(JobGroupPageResult{
			RecordsTotal:    total,
			RecordsFiltered: total,
			Data:            page,
		})
	})

	mux.HandleFunc("/jobgroup/save", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		addrType, _ := strconv.Atoi(r.FormValue("addressType"))
		state.mu.Lock()
		id := state.nextGroupID
		state.nextGroupID++
		state.groups[id] = JobGroup{
			ID:          id,
			AppName:     r.FormValue("appname"),
			Title:       r.FormValue("title"),
			AddressType: addrType,
			AddressList: r.FormValue("addressList"),
		}
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/jobgroup/update", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		addrType, _ := strconv.Atoi(r.FormValue("addressType"))
		state.mu.Lock()
		state.groups[id] = JobGroup{
			ID:          id,
			AppName:     r.FormValue("appname"),
			Title:       r.FormValue("title"),
			AddressType: addrType,
			AddressList: r.FormValue("addressList"),
		}
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/jobgroup/remove", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		state.mu.Lock()
		delete(state.groups, id)
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/jobinfo/pageList", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		groupID, _ := strconv.Atoi(r.FormValue("jobGroup"))
		handler := r.FormValue("executorHandler")
		start, _ := strconv.Atoi(r.FormValue("start"))
		length, _ := strconv.Atoi(r.FormValue("length"))
		if length <= 0 {
			length = 200
		}
		state.mu.Lock()
		state.pageListCalls++
		if state.failPageList {
			state.mu.Unlock()
			// XXL-JOB 2.4.1 WebExceptionResolver returns HTTP 200 with {"code":500,"msg":"..."} on DB errors
			_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "HikariPool-1 - Connection is not available"})
			return
		}
		var ids []int
		for id := range state.jobs {
			ids = append(ids, id)
		}
		// Sort IDs DESCENDING to match XXL-JOB 2.4.1 ORDER BY id DESC
		for i := 0; i < len(ids); i++ {
			for k := i + 1; k < len(ids); k++ {
				if ids[k] > ids[i] {
					ids[i], ids[k] = ids[k], ids[i]
				}
			}
		}
		var list []JobInfo
		for _, id := range ids {
			j := state.jobs[id]
			if (groupID == 0 || j.JobGroup == groupID) && (handler == "" || j.ExecutorHandler == handler) {
				list = append(list, j)
			}
		}
		total := len(list)
		var page []JobInfo
		if start < total {
			end := start + length
			if end > total {
				end = total
			}
			page = list[start:end]
		}
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(JobInfoPageResult{
			RecordsTotal:    total,
			RecordsFiltered: total,
			Data:            page,
		})
	})

	mux.HandleFunc("/jobinfo/add", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		groupID, _ := strconv.Atoi(r.FormValue("jobGroup"))
		timeout, _ := strconv.Atoi(r.FormValue("executorTimeout"))
		retries, _ := strconv.Atoi(r.FormValue("executorFailRetryCount"))
		state.mu.Lock()
		id := state.nextJobID
		state.nextJobID++
		state.jobs[id] = JobInfo{
			ID:                     id,
			JobGroup:               groupID,
			JobDesc:                r.FormValue("jobDesc"),
			Author:                 r.FormValue("author"),
			AlarmEmail:             r.FormValue("alarmEmail"),
			ScheduleType:           r.FormValue("scheduleType"),
			ScheduleConf:           r.FormValue("scheduleConf"),
			MisfireStrategy:        r.FormValue("misfireStrategy"),
			ExecutorHandler:        r.FormValue("executorHandler"),
			ExecutorParam:          r.FormValue("executorParam"),
			ExecutorRouteStrategy:  r.FormValue("executorRouteStrategy"),
			ExecutorBlockStrategy:  r.FormValue("executorBlockStrategy"),
			ExecutorTimeout:        timeout,
			ExecutorFailRetryCount: retries,
			GlueType:               r.FormValue("glueType"),
			GlueSource:             r.FormValue("glueSource"),
			GlueRemark:             r.FormValue("glueRemark"),
			ChildJobID:             r.FormValue("childJobId"),
			TriggerStatus:          0,
		}
		state.mu.Unlock()
		rawID, _ := json.Marshal(strconv.Itoa(id))
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode, Content: rawID})
	})

	mux.HandleFunc("/jobinfo/update", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		groupID, _ := strconv.Atoi(r.FormValue("jobGroup"))
		timeout, _ := strconv.Atoi(r.FormValue("executorTimeout"))
		retries, _ := strconv.Atoi(r.FormValue("executorFailRetryCount"))
		state.mu.Lock()
		state.updateCalls++
		state.actionLog = append(state.actionLog, "update:"+strconv.Itoa(id))
		existing, ok := state.jobs[id]
		if !ok {
			state.mu.Unlock()
			_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "任务信息未找到"})
			return
		}
		existing.JobGroup = groupID
		existing.JobDesc = r.FormValue("jobDesc")
		existing.Author = r.FormValue("author")
		existing.AlarmEmail = r.FormValue("alarmEmail")
		existing.ScheduleType = r.FormValue("scheduleType")
		existing.ScheduleConf = r.FormValue("scheduleConf")
		existing.MisfireStrategy = r.FormValue("misfireStrategy")
		existing.ExecutorHandler = r.FormValue("executorHandler")
		existing.ExecutorParam = r.FormValue("executorParam")
		existing.ExecutorRouteStrategy = r.FormValue("executorRouteStrategy")
		existing.ExecutorBlockStrategy = r.FormValue("executorBlockStrategy")
		existing.ExecutorTimeout = timeout
		existing.ExecutorFailRetryCount = retries
		existing.ChildJobID = r.FormValue("childJobId")
		state.jobs[id] = existing
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/jobcode/save", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		state.mu.Lock()
		existing, ok := state.jobs[id]
		if !ok {
			state.mu.Unlock()
			_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "任务信息未找到"})
			return
		}
		existing.GlueSource = r.FormValue("glueSource")
		existing.GlueRemark = r.FormValue("glueRemark")
		state.jobs[id] = existing
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/jobinfo/start", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		state.mu.Lock()
		existing, ok := state.jobs[id]
		if !ok {
			state.mu.Unlock()
			_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "任务信息未找到"})
			return
		}
		existing.TriggerStatus = 1
		state.jobs[id] = existing
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/jobinfo/stop", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		state.mu.Lock()
		state.actionLog = append(state.actionLog, "stop:"+strconv.Itoa(id))
		existing, ok := state.jobs[id]
		if !ok {
			state.mu.Unlock()
			_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "任务信息未找到"})
			return
		}
		existing.TriggerStatus = 0
		state.jobs[id] = existing
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	mux.HandleFunc("/jobinfo/remove", func(w http.ResponseWriter, r *http.Request) {
		if !checkCookie(w, r) {
			return
		}
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		state.mu.Lock()
		if existing, ok := state.jobs[id]; ok && existing.ChildJobID == "block-remove" {
			state.mu.Unlock()
			_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "存在子任务引用，拒绝删除"})
			return
		}
		delete(state.jobs, id)
		state.mu.Unlock()
		_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, state
}

func TestClientOpenAPIAndManagementLifecycle(t *testing.T) {
	srv, state := newMockAdminServer(t, "secret_token")
	cli, err := NewClient(Config{
		BaseURL:     srv.URL,
		Username:    "admin",
		Password:    "123456",
		AccessToken: "secret_token",
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()

	// 1. Test Executor Register & Remove
	addr := "http://10.244.0.15:9999/"
	if err := cli.RegisterExecutor(ctx, "order-exec", addr); err != nil {
		t.Fatalf("RegisterExecutor failed: %v", err)
	}
	if !state.registries["order-exec"][addr] {
		t.Fatalf("expected address %s to be registered", addr)
	}
	if err := cli.RemoveExecutor(ctx, "order-exec", addr); err != nil {
		t.Fatalf("RemoveExecutor failed: %v", err)
	}
	if state.registries["order-exec"][addr] {
		t.Fatalf("expected address %s to be removed", addr)
	}

	// 2. Test Callback
	if err := cli.Callback(ctx, []HandleCallbackParam{
		{LogID: 88, HandleCode: 200, HandleMsg: "OK"},
	}); err != nil {
		t.Fatalf("Callback failed: %v", err)
	}
	if len(state.callbacks) != 1 || state.callbacks[0].LogID != 88 {
		t.Fatalf("unexpected callbacks: %+v", state.callbacks)
	}

	// 3. Test EnsureJobGroup (create & update & remove)
	grp, err := cli.EnsureJobGroup(ctx, JobGroup{
		AppName:     "order-exec",
		Title:       "Order Executor",
		AddressType: 1,
		AddressList: "http://10.244.0.15:9999/",
	})
	if err != nil {
		t.Fatalf("EnsureJobGroup create failed: %v", err)
	}
	if grp.ID != 1 {
		t.Fatalf("expected group ID 1, got %d", grp.ID)
	}

	grpUpdated, err := cli.EnsureJobGroup(ctx, JobGroup{
		AppName:     "order-exec",
		Title:       "OrderExecV2",
		AddressType: 1,
		AddressList: "http://10.244.0.16:9999/",
	})
	if err != nil {
		t.Fatalf("EnsureJobGroup update failed: %v", err)
	}
	if state.groups[grpUpdated.ID].AddressList != "http://10.244.0.16:9999/" {
		t.Fatalf("expected updated addressList, got %s", state.groups[grpUpdated.ID].AddressList)
	}

	// 4. Test EnsureJobInfo (create + start + update + stop + remove)
	job, err := cli.EnsureJobInfo(ctx, JobInfo{
		JobGroup:        grp.ID,
		JobDesc:         "Daily Settlement",
		Author:          "team-fin",
		ScheduleType:    "CRON",
		ScheduleConf:    "0 0 2 * * ?",
		ExecutorHandler: "settlementJobHandler",
		GlueType:        "GLUE_SHELL",
		GlueSource:      "#!/bin/sh\necho hello",
	}, true)
	if err != nil {
		t.Fatalf("EnsureJobInfo create failed: %v", err)
	}
	if job.ID != 100 || job.TriggerStatus != 1 {
		t.Fatalf("unexpected created job: %+v", job)
	}

	// Pause job
	jobPaused, err := cli.EnsureJobInfo(ctx, JobInfo{
		JobGroup:        grp.ID,
		JobDesc:         "Daily Settlement",
		Author:          "team-fin",
		ScheduleType:    "CRON",
		ScheduleConf:    "0 0 3 * * ?",
		ExecutorHandler: "settlementJobHandler",
		GlueType:        "GLUE_SHELL",
		GlueSource:      "#!/bin/sh\necho hello v2",
	}, false)
	if err != nil {
		t.Fatalf("EnsureJobInfo pause failed: %v", err)
	}
	if jobPaused.TriggerStatus != 0 {
		t.Fatalf("expected TriggerStatus=0, got %d", jobPaused.TriggerStatus)
	}

	if err := cli.RemoveJobInfo(ctx, job.ID); err != nil {
		t.Fatalf("RemoveJobInfo failed: %v", err)
	}
	if err := cli.RemoveJobGroup(ctx, grp.ID); err != nil {
		t.Fatalf("RemoveJobGroup failed: %v", err)
	}

	// 5. Test EnsureJobGroup with AddressType=1 and empty AddressList (fixes N-P0-2: never downgrade to 0; use sentinel)
	emptyGrp, err := cli.EnsureJobGroup(ctx, JobGroup{
		AppName:     "empty-manual-exec",
		Title:       "EmptyManual",
		AddressType: 1,
		AddressList: "",
	})
	if err != nil {
		t.Fatalf("EnsureJobGroup with empty manual addressList failed: %v", err)
	}
	if state.groups[emptyGrp.ID].AddressType != 1 || state.groups[emptyGrp.ID].AddressList != DefaultUnavailableSentinelAddress {
		t.Fatalf("expected empty AddressList to stay AddressType=1 with sentinel address %q, got %+v",
			DefaultUnavailableSentinelAddress, state.groups[emptyGrp.ID])
	}
	// Once an address is available, it replaces the sentinel address
	nonEmptyGrp, err := cli.EnsureJobGroup(ctx, JobGroup{
		AppName:     "empty-manual-exec",
		Title:       "EmptyManual",
		AddressType: 1,
		AddressList: "http://10.244.0.99:9999/",
	})
	if err != nil {
		t.Fatalf("EnsureJobGroup update to real address failed: %v", err)
	}
	if state.groups[nonEmptyGrp.ID].AddressType != 1 || state.groups[nonEmptyGrp.ID].AddressList != "http://10.244.0.99:9999/" {
		t.Fatalf("expected AddressType=1 with real address, got %+v", state.groups[nonEmptyGrp.ID])
	}

	// 6. Test EnsureJobInfo rename in-place (fixes P1-5) and glueType change protection (fixes P1-8)
	jobV1, err := cli.EnsureJobInfo(ctx, JobInfo{
		JobGroup:        nonEmptyGrp.ID,
		JobDesc:         "Original Job Name",
		Author:          "dev",
		ScheduleType:    "CRON",
		ScheduleConf:    "0 0 1 * * ?",
		ExecutorHandler: "handlerV1",
		GlueType:        "BEAN",
	}, true)
	if err != nil {
		t.Fatalf("create jobV1 failed: %v", err)
	}

	// Rename both JobDesc and ExecutorHandler while passing ID: should update in-place without creating duplicate
	jobRenamed, err := cli.EnsureJobInfo(ctx, JobInfo{
		ID:              jobV1.ID,
		JobGroup:        nonEmptyGrp.ID,
		JobDesc:         "Renamed Job Name",
		Author:          "dev",
		ScheduleType:    "CRON",
		ScheduleConf:    "0 0 1 * * ?",
		ExecutorHandler: "handlerV2",
		GlueType:        "BEAN",
	}, true)
	if err != nil {
		t.Fatalf("rename job in-place failed: %v", err)
	}
	if jobRenamed.ID != jobV1.ID || len(state.jobs) != 1 {
		t.Fatalf("expected in-place update on ID=%d (total jobs=1), got ID=%d total=%d", jobV1.ID, jobRenamed.ID, len(state.jobs))
	}

	// Attempt to change GlueType without AllowGlueTypeRecreate -> must be rejected (fixes P1-8)
	if _, err := cli.EnsureJobInfo(ctx, JobInfo{
		ID:              jobRenamed.ID,
		JobGroup:        nonEmptyGrp.ID,
		JobDesc:         "Renamed Job Name",
		Author:          "dev",
		ScheduleType:    "CRON",
		ScheduleConf:    "0 0 1 * * ?",
		ExecutorHandler: "handlerV2",
		GlueType:        "GLUE_SHELL",
		GlueSource:      "echo hi",
	}, true); err == nil {
		t.Fatalf("expected glueType change without AllowGlueTypeRecreate to be rejected")
	}

	// Change GlueType with AllowGlueTypeRecreate=true -> should remove old ID and recreate cleanly
	jobGlueChanged, err := cli.EnsureJobInfo(ctx, JobInfo{
		ID:                    jobRenamed.ID,
		JobGroup:              nonEmptyGrp.ID,
		JobDesc:               "Renamed Job Name",
		Author:                "dev",
		ScheduleType:          "CRON",
		ScheduleConf:          "0 0 1 * * ?",
		ExecutorHandler:       "handlerV2",
		GlueType:              "GLUE_SHELL",
		GlueSource:            "echo hi",
		AllowGlueTypeRecreate: true,
	}, true)
	if err != nil {
		t.Fatalf("change glueType with AllowGlueTypeRecreate=true failed: %v", err)
	}
	if jobGlueChanged.ID == jobRenamed.ID || len(state.jobs) != 1 {
		t.Fatalf("expected recreated job with new ID and total jobs=1, got ID=%d total=%d", jobGlueChanged.ID, len(state.jobs))
	}

	// 7. Test pagination beyond 200 tasks in GetJobInfoByID and FindJobInfo (fixes P1-6)
	var lastJobID int
	for i := 0; i < 220; i++ {
		id, err := cli.AddJobInfo(ctx, JobInfo{
			JobGroup:        nonEmptyGrp.ID,
			JobDesc:         "Paginated Task " + strconv.Itoa(i),
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "paginatedHandler_" + strconv.Itoa(i),
			GlueType:        "BEAN",
		})
		if err != nil {
			t.Fatalf("AddJobInfo #%d failed: %v", i, err)
		}
		lastJobID = id
	}
	foundByID, err := cli.GetJobInfoByID(ctx, nonEmptyGrp.ID, lastJobID)
	if err != nil || foundByID == nil || foundByID.ID != lastJobID {
		t.Fatalf("expected GetJobInfoByID to find task across page boundary (id=%d), got %+v err=%v", lastJobID, foundByID, err)
	}
	foundByName, err := cli.FindJobInfo(ctx, nonEmptyGrp.ID, "paginatedHandler_219", "Paginated Task 219")
	if err != nil || foundByName == nil || foundByName.ID != lastJobID {
		t.Fatalf("expected FindJobInfo to find task #219 across page boundary, got %+v err=%v", foundByName, err)
	}

	// 8. Test GetJobGroupByAppName pagination when >100 prefix-sharing JobGroups exist (fixes R4-P2)
	for i := 0; i < 110; i++ {
		if _, err := cli.EnsureJobGroup(ctx, JobGroup{
			AppName:     "prefix-svc-" + strconv.Itoa(i),
			Title:       "Prefix Service " + strconv.Itoa(i),
			AddressType: 0,
		}); err != nil {
			t.Fatalf("EnsureJobGroup prefix-svc-%d failed: %v", i, err)
		}
	}
	exactPrefixGrp, err := cli.EnsureJobGroup(ctx, JobGroup{
		AppName:     "prefix-svc",
		Title:       "PrefixSvc",
		AddressType: 0,
	})
	if err != nil {
		t.Fatalf("EnsureJobGroup exact prefix-svc failed: %v", err)
	}
	foundExactGrp, err := cli.GetJobGroupByAppName(ctx, "prefix-svc")
	if err != nil || foundExactGrp == nil || foundExactGrp.ID != exactPrefixGrp.ID {
		t.Fatalf("expected GetJobGroupByAppName to find exact 'prefix-svc' on page 2 across 110模糊匹配 groups, got %+v err=%v", foundExactGrp, err)
	}

	// 9. Test R5-P1-1: Modifying executorRef (JobGroup migration) updates in-place without leaving an orphan in the old group
	t.Run("MigrateJobGroupOnExecutorRefChange", func(t *testing.T) {
		migrated, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:               lastJobID,
			JobGroup:         exactPrefixGrp.ID, // move from nonEmptyGrp.ID to exactPrefixGrp.ID
			PreviousJobGroup: nonEmptyGrp.ID,
			JobDesc:          "Paginated Task 219",
			Author:           "dev",
			ScheduleType:     "CRON",
			ScheduleConf:     "0 0 1 * * ?",
			ExecutorHandler:  "paginatedHandler_219",
			GlueType:         "BEAN",
			OwnerKey:         "default/paginated-219",
		}, true)
		if err != nil {
			t.Fatalf("EnsureJobInfo group migration failed: %v", err)
		}
		if migrated.ID != lastJobID || migrated.JobGroup != exactPrefixGrp.ID {
			t.Fatalf("expected same ID=%d migrated to group=%d, got %+v", lastJobID, exactPrefixGrp.ID, migrated)
		}
		state.mu.Lock()
		actualGrp := state.jobs[lastJobID].JobGroup
		state.mu.Unlock()
		if actualGrp != exactPrefixGrp.ID {
			t.Fatalf("expected server state job %d to have JobGroup=%d, got %d", lastJobID, exactPrefixGrp.ID, actualGrp)
		}
	})

	// 10. Test R5-P1-3: Repeated EnsureJobInfo with unchanged spec performs zero /jobinfo/update calls and reuses snapshot cache
	t.Run("NoUpdateWhenUnchangedAndSnapshotCacheReuse", func(t *testing.T) {
		state.mu.Lock()
		beforeUpdates := state.updateCalls
		beforePageLists := state.pageListCalls
		state.mu.Unlock()

		for i := 0; i < 5; i++ {
			if _, err := cli.EnsureJobInfo(ctx, JobInfo{
				ID:              lastJobID,
				JobGroup:        exactPrefixGrp.ID,
				JobDesc:         "Paginated Task 219",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 0 1 * * ?",
				ExecutorHandler: "paginatedHandler_219",
				GlueType:        "BEAN",
				OwnerKey:        "default/paginated-219",
			}, true); err != nil {
				t.Fatalf("EnsureJobInfo #%d failed: %v", i, err)
			}
		}

		state.mu.Lock()
		afterUpdates := state.updateCalls
		afterPageLists := state.pageListCalls
		state.mu.Unlock()

		if afterUpdates != beforeUpdates {
			t.Fatalf("expected 0 /jobinfo/update calls when spec is unchanged (R5-P1-3), got %d", afterUpdates-beforeUpdates)
		}
		if afterPageLists != beforePageLists {
			t.Fatalf("expected 0 /jobinfo/pageList calls within 15s snapshot cache TTL (R5-P1-3), got %d", afterPageLists-beforePageLists)
		}

		// Force a fresh fetch by invalidating the cache and verify convergence still holds (0 updates)
		cli.InvalidateGroupSnapshot(exactPrefixGrp.ID)
		if _, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:              lastJobID,
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Paginated Task 219",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "paginatedHandler_219",
			GlueType:        "BEAN",
			OwnerKey:        "default/paginated-219",
		}, true); err != nil {
			t.Fatalf("EnsureJobInfo after cache invalidation failed: %v", err)
		}
		state.mu.Lock()
		afterFreshUpdates := state.updateCalls
		state.mu.Unlock()
		if afterFreshUpdates != beforeUpdates {
			t.Fatalf("expected 0 /jobinfo/update calls after fresh fetch convergence, got %d", afterFreshUpdates-beforeUpdates)
		}
	})

	// 11. Test R6-P0-1: HTTP 200 + {"code":500} error envelope from /jobinfo/pageList must NOT be treated as empty list or cause duplicate AddJobInfo
	t.Run("PageListErrorDoesNotCreateDuplicateJob", func(t *testing.T) {
		cli.InvalidateGroupSnapshot(exactPrefixGrp.ID)
		state.mu.Lock()
		jobsBefore := len(state.jobs)
		state.failPageList = true
		state.mu.Unlock()
		defer func() {
			state.mu.Lock()
			state.failPageList = false
			state.mu.Unlock()
		}()

		_, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:              lastJobID,
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Paginated Task 219",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "paginatedHandler_219",
			GlueType:        "BEAN",
		}, true)
		if err == nil {
			t.Fatalf("R6-P0-1: expected error when /jobinfo/pageList returns HTTP 200 with code=500")
		}
		if !IsBusinessRejection(err) {
			t.Fatalf("R6-P0-1: expected IsBusinessRejection=true on AdminError, got %v", err)
		}
		state.mu.Lock()
		jobsAfter := len(state.jobs)
		state.mu.Unlock()
		if jobsAfter != jobsBefore {
			t.Fatalf("R6-P0-1 regression: pageList error caused duplicate job creation (%d -> %d)", jobsBefore, jobsAfter)
		}
	})

	// 12. Test R6-P1-1: Concurrent EnsureJobInfo / GetJobInfoByID / FindJobInfo / Stop / Start is race-free under -race
	t.Run("ConcurrentSnapshotCacheRaceFree", func(t *testing.T) {
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				owner := "default/concurrent-task-" + strconv.Itoa(idx)
				for iter := 0; iter < 5; iter++ {
					info, err := cli.EnsureJobInfo(ctx, JobInfo{
						JobGroup:        exactPrefixGrp.ID,
						JobDesc:         "Concurrent Job",
						Author:          "dev",
						ScheduleType:    "CRON",
						ScheduleConf:    "0 0 * * * ?",
						ExecutorHandler: "concurrentHandler",
						GlueType:        "BEAN",
						OwnerKey:        owner,
					}, iter%2 == 0)
					if err != nil {
						t.Errorf("concurrent EnsureJobInfo failed: %v", err)
						return
					}
					_, _ = cli.GetJobInfoByID(ctx, exactPrefixGrp.ID, info.ID)
					_, _ = cli.FindJobInfo(ctx, exactPrefixGrp.ID, "concurrentHandler", "")
				}
			}(g)
		}
		wg.Wait()
	})

	// 13. Test R6-P1-3: Two tasks with identical handler+jobDesc but distinct OwnerKeys never hijack each other,
	// and cross-Admin executorRef change never hijacks a colliding job ID on the new Admin.
	t.Run("OwnershipMarkerAndCrossAdminProtection", func(t *testing.T) {
		t1, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Shared Description",
			Author:          "team-a",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "sharedHandler",
			ExecutorParam:   "param-a",
			OwnerKey:        "default/task-a",
		}, false)
		if err != nil {
			t.Fatalf("create task-a failed: %v", err)
		}
		t2, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Shared Description",
			Author:          "team-b",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 2 * * ?",
			ExecutorHandler: "sharedHandler",
			ExecutorParam:   "param-b",
			OwnerKey:        "default/task-b",
		}, false)
		if err != nil {
			t.Fatalf("create task-b failed: %v", err)
		}
		if t1.ID == t2.ID {
			t.Fatalf("R6-P1-3 regression: task-b hijacked task-a's job ID %d", t1.ID)
		}

		// Cross-Admin ID collision protection against an unmarked manual job (so sameAdmin guard is directly tested!)
		unmarkedManual, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Manual Console Job Without Tag",
			Author:          "ops",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 4 * * ?",
			ExecutorHandler: "manualHandler",
		}, false)
		if err != nil {
			t.Fatalf("create unmarked manual job failed: %v", err)
		}
		t3, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:               unmarkedManual.ID,
			JobGroup:         exactPrefixGrp.ID,
			JobDesc:          "Migrated From Old Admin",
			Author:           "team-c",
			ScheduleType:     "CRON",
			ScheduleConf:     "0 0 3 * * ?",
			ExecutorHandler:  "handlerC",
			PreviousAdminURL: "http://old-admin.default.svc:8080/xxl-job-admin",
			OwnerKey:         "default/task-c",
		}, false)
		if err != nil {
			t.Fatalf("create task-c after cross-admin switch failed: %v", err)
		}
		if t3.ID == unmarkedManual.ID {
			t.Fatalf("R6-P1-3 regression: cross-Admin task overwrote unmarked manual job ID %d on new Admin", unmarkedManual.ID)
		}

		// Verify FormatOwnedJobDesc truncates long UTF-8 descriptions to <= 255 runes while preserving the tag (R7-P2-14)
		longDesc := strings.Repeat("高并发对账任务描述", 30) // 270 runes
		formatted := FormatOwnedJobDesc(longDesc, "production/reconcile-daily-task")
		if len([]rune(formatted)) > 255 || ExtractJobDescOwnerKey(formatted) != "production/reconcile-daily-task" {
			t.Fatalf("expected FormatOwnedJobDesc <= 255 runes with intact owner tag, got runes=%d tag=%q",
				len([]rune(formatted)), ExtractJobDescOwnerKey(formatted))
		}
	})

	// 14. Test R6-P2-9: Transitioning running CRON job to stopped NONE calls Stop BEFORE Update, and rejects NONE + wantRunning=true
	t.Run("StopBeforeUpdateAndRejectNoneRunning", func(t *testing.T) {
		if _, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Invalid Running NONE",
			Author:          "dev",
			ScheduleType:    "NONE",
			ExecutorHandler: "noneHandler",
			OwnerKey:        "default/none-running",
		}, true); err == nil {
			t.Fatalf("R6-P2-9: expected error when scheduleType=NONE and wantRunning=true")
		}

		runningJob, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Stop Before Update Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "stopOrderHandler",
			OwnerKey:        "default/stop-order",
		}, true)
		if err != nil {
			t.Fatalf("create runningJob failed: %v", err)
		}

		state.mu.Lock()
		state.actionLog = nil
		state.mu.Unlock()

		// Now transition to ScheduleType=NONE with wantRunning=false
		if _, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:              runningJob.ID,
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Stop Before Update Job",
			Author:          "dev",
			ScheduleType:    "NONE",
			ExecutorHandler: "stopOrderHandler",
			OwnerKey:        "default/stop-order",
		}, false); err != nil {
			t.Fatalf("transition CRON->NONE with wantRunning=false failed: %v", err)
		}

		state.mu.Lock()
		logCopy := append([]string{}, state.actionLog...)
		state.mu.Unlock()
		expectedStop := "stop:" + strconv.Itoa(runningJob.ID)
		expectedUpdate := "update:" + strconv.Itoa(runningJob.ID)
		if len(logCopy) < 2 || logCopy[0] != expectedStop || logCopy[1] != expectedUpdate {
			t.Fatalf("R6-P2-9: expected stop before update (%s then %s), got %v", expectedStop, expectedUpdate, logCopy)
		}
	})

	// 15. Test R8-P1-7 & R8-P2-8: UpdateJobInfo preserves cached TriggerStatus=1 and long/malformed ownerKeys are handled
	t.Run("UpdateJobInfoPreservesTriggerStatusAndLongOwnerKey", func(t *testing.T) {
		j, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Preserve TriggerStatus Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 3 * * ?",
			ExecutorHandler: "preserveTriggerHandler",
			GlueRemark:      "ok", // < 4 runes, normalized by normalizeGlueRemark
			OwnerKey:        "default/preserve-trigger",
		}, true)
		if err != nil {
			t.Fatalf("EnsureJobInfo failed: %v", err)
		}
		// Call UpdateJobInfo directly without setting TriggerStatus (defaults to 0 in input struct)
		if err := cli.UpdateJobInfo(ctx, JobInfo{
			ID:              j.ID,
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         FormatOwnedJobDesc("Preserve TriggerStatus Job V2", "default/preserve-trigger"),
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 4 * * ?",
			ExecutorHandler: "preserveTriggerHandler",
		}); err != nil {
			t.Fatalf("UpdateJobInfo failed: %v", err)
		}
		cached, err := cli.FindJobInfoByOwnerKey(ctx, exactPrefixGrp.ID, "default/preserve-trigger")
		if err != nil || cached == nil {
			t.Fatalf("FindJobInfoByOwnerKey failed: %v", err)
		}
		if cached.TriggerStatus != 1 {
			t.Fatalf("R8-P1-7 regression: UpdateJobInfo overwrote cached TriggerStatus=1 with %d", cached.TriggerStatus)
		}

		// Verify malformed trailing bracket does not corrupt ExtractJobDescOwnerKey (R8-P2-8)
		if got := ExtractJobDescOwnerKey("Task [xjt:ns/a] [env:prod]"); got != "" {
			t.Fatalf("R8-P2-8 regression: expected empty key for non-trailing owner tag, got %q", got)
		}
		// Verify >120 rune ownerKey is deterministically compacted and total jobDesc <= 255 runes
		veryLongKey := "very-long-namespace-" + strings.Repeat("a", 80) + "/very-long-task-name-" + strings.Repeat("b", 80)
		desc := FormatOwnedJobDesc(strings.Repeat("描述", 200), veryLongKey)
		if len([]rune(desc)) > 255 || !IsOwnerCompatible(desc, veryLongKey) {
			t.Fatalf("R8-P2-8 regression: expected <= 255 runes and compatible ownerKey, got runes=%d desc=%q", len([]rune(desc)), desc)
		}

		// Verify R9-P2-4: EnsureJobInfo SaveGlueCode normalizes GlueRemark on returned JobInfo
		glueTask, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Glue Remark Task",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 3 * * ?",
			ExecutorHandler: "glueRemarkHandler",
			GlueType:        "GLUE_SHELL",
			GlueSource:      "echo v1",
			GlueRemark:      "v1-remark",
			OwnerKey:        "default/glue-remark-task",
		}, false)
		if err != nil {
			t.Fatalf("create glueTask failed: %v", err)
		}
		updatedGlue, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:              glueTask.ID,
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Glue Remark Task",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 3 * * ?",
			ExecutorHandler: "glueRemarkHandler",
			GlueType:        "GLUE_SHELL",
			GlueSource:      "echo v2",
			GlueRemark:      "v2", // <4 runes -> normalized
			OwnerKey:        "default/glue-remark-task",
		}, false)
		if err != nil || len([]rune(updatedGlue.GlueRemark)) < 4 {
			t.Fatalf("R9-P2-4 regression: expected GlueRemark to be normalized to >=4 runes after SaveGlueCode, got err=%v remark=%q", err, updatedGlue.GlueRemark)
		}
	})

	// 16. Test R10-P1-2 & R10-P2-3: IsTerminalCallbackRejection classification and EnsureJobInfo recovery after external deletion of a cached job
	t.Run("Round10TerminalCallbackAndExternalDeletionRecovery", func(t *testing.T) {
		repeatErr := &AdminError{Path: "/api/callback", HTTPStatus: 200, Code: FailCode, Msg: "log repeate callback."}
		notFoundErr := &AdminError{Path: "/api/callback", HTTPStatus: 200, Code: FailCode, Msg: "log item not found."}
		tokenErr := &AdminError{Path: "/api/callback", HTTPStatus: 200, Code: FailCode, Msg: "The access token is wrong."}
		if !IsTerminalCallbackRejection(repeatErr) || !IsTerminalCallbackRejection(notFoundErr) {
			t.Fatalf("R10-P1-2: expected repeat/not-found callback errors to be terminal rejections")
		}
		if IsTerminalCallbackRejection(tokenErr) {
			t.Fatalf("R10-P1-2: expected access token error NOT to be classified as terminal callback rejection")
		}

		// Create a job so it is cached in jobCache
		cachedJob, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Externally Deleted Cached Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 5 * * ?",
			ExecutorHandler: "extDeletedHandler",
			OwnerKey:        "default/ext-deleted-job",
		}, false)
		if err != nil {
			t.Fatalf("create cachedJob failed: %v", err)
		}

		// Simulate an operator or admin user deleting the job directly in XXL-JOB Admin while jobCache still holds cachedJob
		state.mu.Lock()
		delete(state.jobs, cachedJob.ID)
		state.mu.Unlock()

		// Now mutate spec (e.g., ScheduleConf or wantRunning=true) while jobCache is still warm:
		// EnsureJobInfo should detect that the mutation failed because the job was externally deleted,
		// invalidate jobCache, refresh from Admin, and recreate + start the job in the same call!
		recreatedJob, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:              cachedJob.ID,
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Externally Deleted Cached Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 6 * * ?",
			ExecutorHandler: "extDeletedHandler",
			OwnerKey:        "default/ext-deleted-job",
		}, true)
		if err != nil {
			t.Fatalf("R10-P2-3: expected EnsureJobInfo to self-heal when cached job was externally deleted, got: %v", err)
		}
		if recreatedJob.ID == cachedJob.ID || recreatedJob.TriggerStatus != 1 {
			t.Fatalf("R10-P2-3: expected new job ID != %d with TriggerStatus=1, got %+v", cachedJob.ID, recreatedJob)
		}
	})

	// 17. Test R11-P2-3: EnsureJobInfo retries against freshExisting when cached ID was replaced by a new job with same OwnerKey
	t.Run("Round11EnsureJobInfoFreshExistingRetryAndGlueTypeStop", func(t *testing.T) {
		origJob, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Replaced In Admin Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 7 * * ?",
			ExecutorHandler: "replacedHandler",
			OwnerKey:        "default/replaced-in-admin-job",
		}, false)
		if err != nil {
			t.Fatalf("create origJob failed: %v", err)
		}

		// Simulate external replacement in Admin DB while cli's groupSnapshot cache still holds origJob.ID:
		// delete origJob.ID in Admin DB and insert a replacement job with the same [xjt:default/replaced-in-admin-job] tag!
		state.mu.Lock()
		replacement := state.jobs[origJob.ID]
		delete(state.jobs, origJob.ID)
		replacement.ID = state.nextJobID
		state.nextJobID++
		state.jobs[replacement.ID] = replacement
		replacementID := replacement.ID
		state.mu.Unlock()

		// Now call EnsureJobInfo with the stale cached origJob.ID and wantRunning=true:
		// reconcileExistingJobInfo on origJob.ID fails with business rejection (not found),
		// lookupExistingJob(forceFresh=true) finds replacementID via OwnerKey, and EnsureJobInfo must reconcile replacementID!
		syncedReplacement, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:              origJob.ID,
			JobGroup:        exactPrefixGrp.ID,
			JobDesc:         "Replaced In Admin Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 8 * * ?",
			ExecutorHandler: "replacedHandler",
			OwnerKey:        "default/replaced-in-admin-job",
		}, true)
		if err != nil {
			t.Fatalf("R11-P2-3 regression: expected EnsureJobInfo to retry reconcileExistingJobInfo on freshExisting, got: %v", err)
		}
		if syncedReplacement.ID != replacementID || syncedReplacement.TriggerStatus != 1 {
			t.Fatalf("R11-P2-3 regression: expected syncedReplacement ID=%d with TriggerStatus=1, got %+v", replacementID, syncedReplacement)
		}

		// R12-P1-2: Verify EnsureJobInfo finds existing task in PreviousJobGroup by OwnerKey even when desired.ID == 0
		targetGrpR12, err := cli.EnsureJobGroup(ctx, JobGroup{
			AppName:     "r12-target-grp",
			Title:       "R12Target",
			AddressType: 0,
		})
		if err != nil {
			t.Fatalf("EnsureJobGroup r12-target-grp failed: %v", err)
		}
		byIdGrp, err := cli.GetJobGroupByID(ctx, targetGrpR12.ID)
		if err != nil || byIdGrp == nil || byIdGrp.AppName != "r12-target-grp" {
			t.Fatalf("GetJobGroupByID failed: got %+v err=%v", byIdGrp, err)
		}
		migratedWithoutID, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:               0, // simulate lost Status.JobID while migrating from exactPrefixGrp.ID -> targetGrpR12.ID
			JobGroup:         targetGrpR12.ID,
			PreviousJobGroup: exactPrefixGrp.ID,
			JobDesc:          "Replaced In Admin Job",
			Author:           "dev",
			ScheduleType:     "CRON",
			ScheduleConf:     "0 0 9 * * ?",
			ExecutorHandler:  "replacedHandler",
			OwnerKey:         "default/replaced-in-admin-job",
		}, true)
		if err != nil {
			t.Fatalf("R12-P1-2: EnsureJobInfo migration with ID=0 failed: %v", err)
		}
		if migratedWithoutID.ID != replacementID || migratedWithoutID.JobGroup != targetGrpR12.ID {
			t.Fatalf("R12-P1-2 regression: expected existing job %d to be migrated in-place from PreviousJobGroup via OwnerKey when ID=0, got %+v",
				replacementID, migratedWithoutID)
		}

		// Verify UpdateJobInfo preserves existing GlueSource in snapshot cache until SaveGlueCode succeeds
		gluePreserveJob, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        targetGrpR12.ID,
			JobDesc:         "Glue Preserve Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 10 * * ?",
			ExecutorHandler: "gluePreserveHandler",
			GlueType:        "GLUE_SHELL",
			GlueSource:      "echo committed-v1",
			GlueRemark:      "committed-v1",
			OwnerKey:        "default/glue-preserve-job",
		}, false)
		if err != nil {
			t.Fatalf("create gluePreserveJob failed: %v", err)
		}
		// Evict snapshot cache and call UpdateJobInfo with a new GlueSource (which /jobinfo/update does not persist in Admin DB)
		cli.InvalidateGroupSnapshot(targetGrpR12.ID)
		updateAttempt := *gluePreserveJob
		updateAttempt.JobDesc = FormatOwnedJobDesc("Glue Preserve Job Updated", "default/glue-preserve-job")
		updateAttempt.GlueSource = "echo uncommitted-v2"
		if err := cli.UpdateJobInfo(ctx, updateAttempt); err != nil {
			t.Fatalf("UpdateJobInfo failed: %v", err)
		}
		// Ensure reconcileExistingJobInfo still detects that GlueSource differs from existing ("echo committed-v1")
		syncedGluePreserve, err := cli.EnsureJobInfo(ctx, JobInfo{
			ID:              gluePreserveJob.ID,
			JobGroup:        targetGrpR12.ID,
			JobDesc:         "Glue Preserve Job Updated",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 10 * * ?",
			ExecutorHandler: "gluePreserveHandler",
			GlueType:        "GLUE_SHELL",
			GlueSource:      "echo committed-v2",
			GlueRemark:      "committed-v2",
			OwnerKey:        "default/glue-preserve-job",
		}, false)
		if err != nil || syncedGluePreserve.GlueSource != "echo committed-v2" {
			t.Fatalf("expected EnsureJobInfo to persist committed-v2 via SaveGlueCode, got %+v err=%v", syncedGluePreserve, err)
		}

		// Verify EnsureJobInfo with AllowGlueTypeRecreate=true does NOT create a duplicate job if RemoveJobInfo
		// returns a business rejection and the old job still exists in Admin:
		blockedJob, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        targetGrpR12.ID,
			JobDesc:         "Blocked GlueType Recreate Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 11 * * ?",
			ExecutorHandler: "blockedRecreateHandler",
			GlueType:        "BEAN",
			ChildJobID:      "block-remove",
			OwnerKey:        "default/blocked-recreate-job",
		}, false)
		if err != nil {
			t.Fatalf("create blockedJob failed: %v", err)
		}
		_, err = cli.EnsureJobInfo(ctx, JobInfo{
			ID:                    blockedJob.ID,
			JobGroup:              targetGrpR12.ID,
			JobDesc:               "Blocked GlueType Recreate Job",
			Author:                "dev",
			ScheduleType:          "CRON",
			ScheduleConf:          "0 0 11 * * ?",
			ExecutorHandler:       "blockedRecreateHandler",
			GlueType:              "GLUE_SHELL",
			GlueSource:            "echo should-not-add-duplicate",
			ChildJobID:            "block-remove",
			AllowGlueTypeRecreate: true,
			OwnerKey:              "default/blocked-recreate-job",
		}, false)
		if err == nil {
			t.Fatalf("expected EnsureJobInfo to return error when RemoveJobInfo is rejected and old job still exists in Admin")
		}
	})

	// 18. Test Endgame Round 2 Fixes (EG2-02 & EG2-03):
	// - postOpenAPI preserves terminal callback rejections ("repeate callback") during previousAccessToken rotation retry
	// - NewClient trims surrounding whitespace/newlines on BaseURL, AccessToken, and PreviousAccessToken
	// - Concurrent fetchGroupSnapshotFresh on an uncached group never overwrites an in-flight AddJobInfo mutation
	// - Torn multi-page /jobinfo/pageList returns a transient error without poisoning groupSnapshots
	// - UpdateJobInfo on an uncached job invalidates target group snapshot rather than poisoning immutable fields
	// - RemoveJobGroup invalidates cached group snapshot
	t.Run("Endgame2OpenAPIRotationAndSnapshotConsistency", func(t *testing.T) {
		// (a) EG2-02: postOpenAPI fallback token retry preserves terminal business rejection + NewClient trims tokens/URL
		rotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := r.Header.Get(AccessTokenHeader)
			if tok == "new-rotated-tok" {
				_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "The access token is wrong."})
				return
			}
			if tok == "prev-grace-tok" {
				_ = json.NewEncoder(w).Encode(ReturnT{Code: FailCode, Msg: "log repeate callback."})
				return
			}
			w.WriteHeader(http.StatusForbidden)
		}))
		defer rotSrv.Close()

		rotCli, err := NewClient(Config{
			BaseURL:             "  " + rotSrv.URL + "/ \n",
			AccessToken:         " new-rotated-tok\n",
			PreviousAccessToken: "\tprev-grace-tok\r\n",
		})
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		if rotCli.BaseURL() != rotSrv.URL {
			t.Fatalf("EG2-02: expected trimmed BaseURL %q, got %q", rotSrv.URL, rotCli.BaseURL())
		}
		cbErr := rotCli.Callback(ctx, []HandleCallbackParam{{LogID: 777, HandleCode: SuccessCode}})
		if !IsTerminalCallbackRejection(cbErr) {
			t.Fatalf("EG2-02 regression: expected repeat callback error from previousAccessToken retry to be preserved as terminal rejection, got: %v", cbErr)
		}

		// (b) EG2-03: Concurrent fetchGroupSnapshotFresh vs AddJobInfo on an uncached JobGroup
		enteredFetch := make(chan struct{})
		releaseFetch := make(chan struct{})
		var gateFirstFetch atomic.Bool
		gateFirstFetch.Store(true)
		var raceMu sync.Mutex
		raceJobs := make(map[int]JobInfo)
		raceNextID := 500

		raceMux := http.NewServeMux()
		raceMux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
			http.SetCookie(w, &http.Cookie{Name: "XXL_JOB_LOGIN_IDENTITY", Value: "ok", Path: "/"})
			_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
		})
		raceMux.HandleFunc("/jobinfo/pageList", func(w http.ResponseWriter, r *http.Request) {
			if gateFirstFetch.CompareAndSwap(true, false) {
				raceMu.Lock()
				var stale []JobInfo
				for _, j := range raceJobs {
					stale = append(stale, j)
				}
				raceMu.Unlock()
				close(enteredFetch)
				<-releaseFetch
				_ = json.NewEncoder(w).Encode(JobInfoPageResult{RecordsTotal: len(stale), Data: stale})
				return
			}
			raceMu.Lock()
			var list []JobInfo
			for _, j := range raceJobs {
				list = append(list, j)
			}
			raceMu.Unlock()
			_ = json.NewEncoder(w).Encode(JobInfoPageResult{RecordsTotal: len(list), Data: list})
		})
		raceMux.HandleFunc("/jobinfo/add", func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			grp, _ := strconv.Atoi(r.FormValue("jobGroup"))
			raceMu.Lock()
			id := raceNextID
			raceNextID++
			raceJobs[id] = JobInfo{
				ID:              id,
				JobGroup:        grp,
				JobDesc:         r.FormValue("jobDesc"),
				ExecutorHandler: r.FormValue("executorHandler"),
			}
			raceMu.Unlock()
			rawID, _ := json.Marshal(strconv.Itoa(id))
			_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode, Content: rawID})
		})
		raceSrv := httptest.NewServer(raceMux)
		defer raceSrv.Close()

		raceCli, err := NewClient(Config{BaseURL: raceSrv.URL, JobCacheTTL: 30 * time.Second})
		if err != nil {
			t.Fatalf("NewClient raceCli failed: %v", err)
		}
		var fetchWG sync.WaitGroup
		fetchWG.Add(1)
		go func() {
			defer fetchWG.Done()
			_, _ = raceCli.FindJobInfo(ctx, 42, "nonExistent", "")
		}()
		<-enteredFetch
		addedID, err := raceCli.AddJobInfo(ctx, JobInfo{
			JobGroup:        42,
			JobDesc:         "Concurrent Added Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "concurrentHandler",
		})
		if err != nil {
			close(releaseFetch)
			t.Fatalf("AddJobInfo during concurrent fetch failed: %v", err)
		}
		close(releaseFetch)
		fetchWG.Wait()

		foundAfterRace, err := raceCli.GetJobInfoByID(ctx, 42, addedID)
		if err != nil || foundAfterRace == nil || foundAfterRace.ID != addedID {
			t.Fatalf("EG2-03 regression: stale concurrent fetch overwrote uncached group 42 after AddJobInfo(id=%d): got=%+v err=%v", addedID, foundAfterRace, err)
		}

		// (c) EG2-03: Torn multi-page /jobinfo/pageList must return an error and NOT cache partial results
		tornMux := http.NewServeMux()
		tornMux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
			http.SetCookie(w, &http.Cookie{Name: "XXL_JOB_LOGIN_IDENTITY", Value: "ok", Path: "/"})
			_ = json.NewEncoder(w).Encode(ReturnT{Code: SuccessCode})
		})
		tornMux.HandleFunc("/jobinfo/pageList", func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			start, _ := strconv.Atoi(r.FormValue("start"))
			if start == 0 {
				page0 := make([]JobInfo, 100)
				for i := 0; i < 100; i++ {
					page0[i] = JobInfo{ID: 300 - i, JobGroup: 9, ExecutorHandler: "h"}
				}
				_ = json.NewEncoder(w).Encode(JobInfoPageResult{RecordsTotal: 150, Data: page0})
				return
			}
			// Simulate concurrent row deletion shifting OFFSET so RecordsTotal changed to 149
			page1 := make([]JobInfo, 49)
			for i := 0; i < 49; i++ {
				page1[i] = JobInfo{ID: 199 - i, JobGroup: 9, ExecutorHandler: "h"}
			}
			_ = json.NewEncoder(w).Encode(JobInfoPageResult{RecordsTotal: 149, Data: page1})
		})
		tornSrv := httptest.NewServer(tornMux)
		defer tornSrv.Close()
		tornCli, _ := NewClient(Config{BaseURL: tornSrv.URL, JobCacheTTL: 30 * time.Second})
		if _, err := tornCli.FindJobInfo(ctx, 9, "h", ""); err == nil {
			t.Fatalf("EG2-03 regression: expected error on persistently torn multi-page /jobinfo/pageList")
		}
		tornCli.cacheMu.Lock()
		_, poisoned := tornCli.groupSnapshots[9]
		tornCli.cacheMu.Unlock()
		if poisoned {
			t.Fatalf("EG2-03 regression: torn /jobinfo/pageList snapshot was cached in groupSnapshots")
		}

		// (d) EG2-03: UpdateJobInfo on an uncached job moving into a cached group invalidates rather than poisoning TriggerStatus=0,
		// and RemoveJobGroup invalidates its cached snapshot
		grpSrc, err := cli.EnsureJobGroup(ctx, JobGroup{AppName: "eg2-src-grp", Title: "EG2Src", AddressType: 0})
		if err != nil {
			t.Fatalf("EnsureJobGroup eg2-src-grp failed: %v", err)
		}
		grpDst, err := cli.EnsureJobGroup(ctx, JobGroup{AppName: "eg2-dst-grp", Title: "EG2Dst", AddressType: 0})
		if err != nil {
			t.Fatalf("EnsureJobGroup eg2-dst-grp failed: %v", err)
		}
		// Warm grpDst snapshot in cache
		_, _ = cli.FindJobInfo(ctx, grpDst.ID, "none", "")
		movingJob, err := cli.EnsureJobInfo(ctx, JobInfo{
			JobGroup:        grpSrc.ID,
			JobDesc:         "Moving Active Job",
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "movingHandler",
			OwnerKey:        "default/eg2-moving-job",
		}, true)
		if err != nil {
			t.Fatalf("create movingJob failed: %v", err)
		}
		// Evict grpSrc so movingJob.ID is NOT in any cached snapshot when UpdateJobInfo moves it to grpDst
		cli.InvalidateGroupSnapshot(grpSrc.ID)
		if err := cli.UpdateJobInfo(ctx, JobInfo{
			ID:              movingJob.ID,
			JobGroup:        grpDst.ID,
			JobDesc:         movingJob.JobDesc,
			Author:          "dev",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "movingHandler",
		}); err != nil {
			t.Fatalf("UpdateJobInfo movingJob to grpDst failed: %v", err)
		}
		fetchedMoved, err := cli.GetJobInfoByID(ctx, grpDst.ID, movingJob.ID)
		if err != nil || fetchedMoved == nil || fetchedMoved.TriggerStatus != 1 {
			t.Fatalf("EG2-03 regression: UpdateJobInfo on uncached job poisoned grpDst cache with TriggerStatus=0: got=%+v err=%v", fetchedMoved, err)
		}

		// Warm grpSrc snapshot and then RemoveJobGroup(grpSrc.ID)
		_, _ = cli.FindJobInfo(ctx, grpSrc.ID, "none", "")
		if err := cli.RemoveJobGroup(ctx, grpSrc.ID); err != nil {
			t.Fatalf("RemoveJobGroup grpSrc failed: %v", err)
		}
		cli.cacheMu.Lock()
		_, srcStillCached := cli.groupSnapshots[grpSrc.ID]
		cli.cacheMu.Unlock()
		if srcStillCached {
			t.Fatalf("EG2-03 regression: expected RemoveJobGroup(%d) to invalidate cached snapshot", grpSrc.ID)
		}
	})
}

func TestBoostEndgameClientPaginationCacheAndTokenSanitization(t *testing.T) {
	ctx := context.Background()

	// (a) GetJobGroupByAppName & GetJobGroupByID SQL OFFSET pagination drift retry and persistent-drift failure
	var pageCalls atomic.Int32
	persistDrift := atomic.Bool{}
	var callbackTokens []string
	var tokMu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/login":
			http.SetCookie(w, &http.Cookie{Name: "XXL_JOB_LOGIN_IDENTITY", Value: "ok"})
			_, _ = w.Write([]byte(`{"code":200}`))
		case "/api/callback":
			tok := r.Header.Get(AccessTokenHeader)
			tokMu.Lock()
			callbackTokens = append(callbackTokens, tok)
			tokMu.Unlock()
			if tok != "prev-clean-tok" {
				_, _ = w.Write([]byte(`{"code":500,"msg":"The access token is wrong."}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":200}`))
		case "/jobgroup/remove":
			_ = r.ParseForm()
			if r.FormValue("id") == "77" {
				_, _ = w.Write([]byte(`{"code":500,"msg":"拒绝删除，该执行器下仍有任务"}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":200}`))
		case "/jobinfo/pageList":
			_, _ = w.Write([]byte(`{"recordsTotal":0,"recordsFiltered":0,"data":[]}`))
		case "/jobgroup/pageList":
			_ = r.ParseForm()
			start, _ := strconv.Atoi(r.FormValue("start"))
			callNum := pageCalls.Add(1)
			// Build 120 groups sorted ascending by ID (1..120), target is ID=101 ("drift-target") right at page 2 boundary
			all := make([]JobGroup, 0, 120)
			for i := 1; i <= 120; i++ {
				app := fmt.Sprintf("grp-%03d", i)
				if i == 101 {
					app = "drift-target"
				}
				all = append(all, JobGroup{ID: i, AppName: app, Title: app})
			}
			// Simulate concurrent delete of an earlier row before page 2 (start=100) is fetched,
			// shifting ID=101 ("drift-target") onto page 1's tail so OFFSET 100 skips ID=101 and starts at ID=102!
			if start == 100 && (persistDrift.Load() || callNum == 2) {
				_ = json.NewEncoder(w).Encode(JobGroupPageResult{
					RecordsTotal:    119,
					RecordsFiltered: 119,
					Data:            all[101:],
				})
				return
			}
			end := start + 100
			if end > len(all) {
				end = len(all)
			}
			var slice []JobGroup
			if start < len(all) {
				slice = all[start:end]
			}
			_ = json.NewEncoder(w).Encode(JobGroupPageResult{
				RecordsTotal:    len(all),
				RecordsFiltered: len(all),
				Data:            slice,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cli, err := NewClient(Config{
		BaseURL:             srv.URL,
		AccessToken:         "  primary-tok\r\n  ",
		PreviousAccessToken: "\tprev-clean-tok\r\n",
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// Verify transient drift on attempt 1 recovers on attempt 2 and finds ID=101
	grpByApp, err := cli.GetJobGroupByAppName(ctx, "drift-target")
	if err != nil || grpByApp == nil || grpByApp.ID != 101 {
		t.Fatalf("expected GetJobGroupByAppName to recover from page drift and return ID=101, got grp=%+v err=%v", grpByApp, err)
	}

	pageCalls.Store(0)
	grpByID, err := cli.GetJobGroupByID(ctx, 101)
	if err != nil || grpByID == nil || grpByID.AppName != "drift-target" {
		t.Fatalf("expected GetJobGroupByID to recover from page drift and return drift-target, got grp=%+v err=%v", grpByID, err)
	}

	// Verify persistent drift across all 3 attempts returns an explicit error instead of nil, nil
	persistDrift.Store(true)
	if _, err := cli.GetJobGroupByAppName(ctx, "drift-target"); err == nil {
		t.Fatalf("expected GetJobGroupByAppName to fail when pagination drift persists across all retries")
	}
	if _, err := cli.GetJobGroupByID(ctx, 101); err == nil {
		t.Fatalf("expected GetJobGroupByID to fail when pagination drift persists across all retries")
	}
	persistDrift.Store(false)

	// (b) Verify CRLF/whitespace-sanitized tokens work without net/http header errors and fallback succeeds
	if err := cli.Callback(ctx, []HandleCallbackParam{{LogID: 1, HandleCode: 200}}); err != nil {
		t.Fatalf("Callback with CRLF-sanitized tokens failed: %v", err)
	}
	tokMu.Lock()
	gotTokens := append([]string{}, callbackTokens...)
	tokMu.Unlock()
	if len(gotTokens) != 2 || gotTokens[0] != "primary-tok" || gotTokens[1] != "prev-clean-tok" {
		t.Fatalf("expected sanitized tokens [primary-tok, prev-clean-tok], got %v", gotTokens)
	}

	// (c) Verify RemoveJobGroup preserves cached snapshot on business rejection (id=77) and invalidates on success (id=78)
	_, _ = cli.FindJobInfo(ctx, 77, "h", "")
	_, _ = cli.FindJobInfo(ctx, 78, "h", "")
	if err := cli.RemoveJobGroup(ctx, 77); err == nil || !IsBusinessRejection(err) {
		t.Fatalf("expected business rejection on RemoveJobGroup(77), got %v", err)
	}
	cli.cacheMu.Lock()
	_, still77 := cli.groupSnapshots[77]
	cli.cacheMu.Unlock()
	if !still77 {
		t.Fatalf("expected RemoveJobGroup business rejection to preserve cached snapshot for group 77")
	}
	if err := cli.RemoveJobGroup(ctx, 78); err != nil {
		t.Fatalf("RemoveJobGroup(78) failed: %v", err)
	}
	cli.cacheMu.Lock()
	_, still78 := cli.groupSnapshots[78]
	cli.cacheMu.Unlock()
	if still78 {
		t.Fatalf("expected successful RemoveJobGroup(78) to invalidate cached snapshot")
	}

	// (d) Verify maxGroupSnapshots bounded eviction and groupGen (>1024) pruning bumping unscopedMutGen
	baseTime := time.Now().Add(-10 * time.Minute)
	cli.cacheMu.Lock()
	cli.groupSnapshots = make(map[int]jobGroupSnapshot)
	cli.groupGen = make(map[int]uint64)
	for g := 1; g <= maxGroupSnapshots; g++ {
		cli.groupSnapshots[g] = jobGroupSnapshot{
			fetchedAt: baseTime.Add(time.Duration(g) * time.Millisecond),
			byID:      map[int]JobInfo{},
		}
	}
	for g := 1000; g <= 2105; g++ {
		cli.groupGen[g] = 1
	}
	unscopedBefore := cli.unscopedMutGen
	cli.cacheMu.Unlock()

	// Fetching group 9999 should prune excess groupGen (>1024) and evict oldest groupSnapshot (g=1)
	if _, err := cli.FindJobInfo(ctx, 9999, "h", ""); err != nil {
		t.Fatalf("FindJobInfo(9999) failed: %v", err)
	}
	cli.cacheMu.Lock()
	snapCount := len(cli.groupSnapshots)
	_, oldestStillPresent := cli.groupSnapshots[1]
	_, newPresent := cli.groupSnapshots[9999]
	genCount := len(cli.groupGen)
	unscopedAfter := cli.unscopedMutGen
	cli.cacheMu.Unlock()
	if snapCount > maxGroupSnapshots || oldestStillPresent || !newPresent {
		t.Fatalf("expected maxGroupSnapshots=%d eviction of group 1 and insertion of 9999, got count=%d oldest=%v new=%v",
			maxGroupSnapshots, snapCount, oldestStillPresent, newPresent)
	}
	if genCount > 1025 || unscopedAfter <= unscopedBefore {
		t.Fatalf("expected groupGen pruning (got %d) and unscopedMutGen bump (%d -> %d)", genCount, unscopedBefore, unscopedAfter)
	}
}

func TestSnapshotEvictionPreventsStaleResurrectionAndJSON302Relogin(t *testing.T) {
	ctx := context.Background()

	// Part A: Concurrent slow fetchGroupSnapshotFresh started at genBefore=0 BEFORE group 1 is mutated and
	// subsequently evicted by maxGroupSnapshots MUST NOT resurrect the stale pre-mutation snapshot.
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	var serveUpdatedGroup1 atomic.Bool
	var blockFirstGroup1 atomic.Bool
	blockFirstGroup1.Store(true)

	srvSnap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/login":
			_, _ = w.Write([]byte(`{"code":200}`))
		case "/jobinfo/pageList":
			_ = r.ParseForm()
			grp := r.FormValue("jobGroup")
			if grp == "1" {
				if blockFirstGroup1.CompareAndSwap(true, false) {
					close(slowStarted)
					<-releaseSlow
					_ = json.NewEncoder(w).Encode(JobInfoPageResult{
						RecordsTotal:    1,
						RecordsFiltered: 1,
						Data:            []JobInfo{{ID: 10, JobGroup: 1, JobDesc: "old", ExecutorHandler: "staleHandler"}},
					})
					return
				}
				handlerName := "staleHandler"
				if serveUpdatedGroup1.Load() {
					handlerName = "freshHandler"
				}
				_ = json.NewEncoder(w).Encode(JobInfoPageResult{
					RecordsTotal:    1,
					RecordsFiltered: 1,
					Data:            []JobInfo{{ID: 10, JobGroup: 1, JobDesc: "new", ExecutorHandler: handlerName}},
				})
				return
			}
			grpID, _ := strconv.Atoi(grp)
			_ = json.NewEncoder(w).Encode(JobInfoPageResult{
				RecordsTotal:    1,
				RecordsFiltered: 1,
				Data:            []JobInfo{{ID: grpID * 10, JobGroup: grpID, JobDesc: "g", ExecutorHandler: "h"}},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srvSnap.Close()

	cliSnap, err := NewClient(Config{BaseURL: srvSnap.URL, JobCacheTTL: time.Minute})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	slowDone := make(chan error, 1)
	go func() {
		_, fErr := cliSnap.FindJobInfo(ctx, 1, "staleHandler", "")
		slowDone <- fErr
	}()
	<-slowStarted

	// Mutate group 1 (groupGen[1] becomes 1) and populate group 1 + maxGroupSnapshots other groups so group 1 is evicted!
	serveUpdatedGroup1.Store(true)
	cliSnap.InvalidateGroupSnapshot(1)
	if _, err := cliSnap.FindJobInfo(ctx, 1, "freshHandler", ""); err != nil {
		t.Fatalf("fresh FindJobInfo(1) failed: %v", err)
	}
	for g := 2; g <= maxGroupSnapshots+1; g++ {
		if _, err := cliSnap.FindJobInfo(ctx, g, "h", ""); err != nil {
			t.Fatalf("FindJobInfo(%d) failed: %v", g, err)
		}
	}
	cliSnap.cacheMu.Lock()
	_, grp1StillCachedBeforeSlowFinish := cliSnap.groupSnapshots[1]
	cliSnap.cacheMu.Unlock()
	if grp1StillCachedBeforeSlowFinish {
		t.Fatalf("expected group 1 to have been evicted from groupSnapshots by maxGroupSnapshots")
	}

	// Unblock the slow pre-mutation fetch for group 1: it MUST NOT overwrite groupSnapshots[1] with staleHandler!
	close(releaseSlow)
	if sErr := <-slowDone; sErr != nil {
		t.Fatalf("slow FindJobInfo returned unexpected error: %v", sErr)
	}
	gotFresh, err := cliSnap.FindJobInfo(ctx, 1, "freshHandler", "")
	if err != nil || gotFresh == nil || gotFresh.ExecutorHandler != "freshHandler" {
		t.Fatalf("stale snapshot resurrected after maxGroupSnapshots eviction: got=%+v err=%v", gotFresh, err)
	}

	// Part B: HTTP 200 JSON {"code":302,"msg":"login invalid"} triggers automatic re-login and resets loggedIn on failed re-login.
	var loginCalls atomic.Int32
	var expireNextCall atomic.Bool
	var failNextLogin atomic.Bool

	srvAuth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/login":
			n := loginCalls.Add(1)
			if failNextLogin.CompareAndSwap(true, false) {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte("upstream down"))
				return
			}
			http.SetCookie(w, &http.Cookie{Name: "XXL_JOB_LOGIN_IDENTITY", Value: fmt.Sprintf("tok-%d", n), Path: "/"})
			_, _ = w.Write([]byte(`{"code":200}`))
		case "/jobgroup/pageList":
			if expireNextCall.CompareAndSwap(true, false) {
				_, _ = w.Write([]byte(`{"code":302,"msg":"login session expired"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(JobGroupPageResult{
				RecordsTotal:    1,
				RecordsFiltered: 1,
				Data:            []JobGroup{{ID: 7, AppName: "relogin-app", Title: "Relogin"}},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srvAuth.Close()

	cliAuth, err := NewClient(Config{BaseURL: srvAuth.URL, Username: "admin", Password: "pwd"})
	if err != nil {
		t.Fatalf("NewClient auth failed: %v", err)
	}
	if grp, err := cliAuth.GetJobGroupByAppName(ctx, "relogin-app"); err != nil || grp == nil || grp.ID != 7 {
		t.Fatalf("initial GetJobGroupByAppName failed: grp=%+v err=%v", grp, err)
	}
	if loginCalls.Load() != 1 {
		t.Fatalf("expected 1 login call initially, got %d", loginCalls.Load())
	}

	// Expire session with HTTP 200 {"code":302} -> must re-login automatically and succeed!
	expireNextCall.Store(true)
	if grp, err := cliAuth.GetJobGroupByAppName(ctx, "relogin-app"); err != nil || grp == nil || grp.ID != 7 {
		t.Fatalf("expected automatic re-login on JSON code=302 to succeed, got grp=%+v err=%v", grp, err)
	}
	if loginCalls.Load() != 2 {
		t.Fatalf("expected loginCalls=2 after JSON code=302 re-login, got %d", loginCalls.Load())
	}

	// Expire session AND fail /login transiently -> c.loggedIn must be reset to false!
	expireNextCall.Store(true)
	failNextLogin.Store(true)
	if _, err := cliAuth.GetJobGroupByAppName(ctx, "relogin-app"); err == nil {
		t.Fatalf("expected transient re-login failure to return error")
	}
	cliAuth.mu.Lock()
	stillLoggedIn := cliAuth.loggedIn
	cliAuth.mu.Unlock()
	if stillLoggedIn {
		t.Fatalf("expected failed loginLocked to reset c.loggedIn=false")
	}
}

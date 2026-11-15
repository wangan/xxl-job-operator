package xxlclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// maxResponseBodyBytes bounds HTTP response reads from XXL-JOB Admin (8 MiB) (fixes R6-P2-13, R7-P2-13).
	maxResponseBodyBytes = 8 * 1024 * 1024
	// maxErrorBodySnippet caps raw response snippets embedded in error messages (fixes R6-P2-13).
	maxErrorBodySnippet = 512
	// maxJobDescRunes is XXL-JOB 2.4.1's xxl_job_info.job_desc varchar(255) character limit (fixes R7-P2-14).
	maxJobDescRunes = 255
	// maxGroupSnapshots bounds in-memory cached JobGroup snapshots per Client under high JobGroup churn.
	maxGroupSnapshots = 512
)

// Config holds configuration for connecting to XXL-JOB Admin.
type Config struct {
	BaseURL             string
	Username            string
	Password            string
	AccessToken         string
	PreviousAccessToken string
	Timeout             time.Duration
	JobCacheTTL         time.Duration
}

// jobGroupSnapshot is an immutable point-in-time snapshot of all JobInfo items in a JobGroup (fixes R6-P1-1).
// Once published to Client.groupSnapshots, neither items nor byID is ever mutated in place;
// all updates use copy-on-write under cacheMu.
type jobGroupSnapshot struct {
	fetchedAt time.Time
	items     []JobInfo
	byID      map[int]JobInfo
}

// Client is a thread-safe client for XXL-JOB Admin OpenAPI and Management endpoints.
type Client struct {
	baseURL             string
	username            string
	password            string
	accessToken         string
	previousAccessToken string
	httpClient          *http.Client

	mu       sync.Mutex
	loggedIn bool
	loginGen uint64

	cacheMu        sync.Mutex
	jobCacheTTL    time.Duration
	groupSnapshots map[int]jobGroupSnapshot
	groupGen       map[int]uint64
	unscopedMutGen uint64
}

// NewClient constructs a new XXL-JOB Admin client.
func NewClient(cfg Config) (*Client, error) {
	trimmedBaseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if trimmedBaseURL == "" {
		return nil, fmt.Errorf("xxl-job admin baseURL must not be empty")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	cacheTTL := cfg.JobCacheTTL
	if cacheTTL <= 0 {
		cacheTTL = 15 * time.Second
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("create cookie jar: %w", err)
	}
	username := cfg.Username
	if username == "" {
		username = "admin"
	}
	password := cfg.Password
	if password == "" {
		password = "123456"
	}

	return &Client{
		baseURL:             trimmedBaseURL,
		username:            username,
		password:            password,
		accessToken:         sanitizeAccessToken(cfg.AccessToken),
		previousAccessToken: sanitizeAccessToken(cfg.PreviousAccessToken),
		httpClient: &http.Client{
			Timeout: timeout,
			Jar:     jar,
		},
		jobCacheTTL:    cacheTTL,
		groupSnapshots: make(map[int]jobGroupSnapshot),
		groupGen:       make(map[int]uint64),
	}, nil
}

func sanitizeAccessToken(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if !strings.ContainsAny(trimmed, "\r\n") {
		return trimmed
	}
	return strings.NewReplacer("\r", "", "\n", "").Replace(trimmed)
}

func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxRunes])
}

func normalizeOwnerKey(ownerKey string) string {
	ownerKey = strings.TrimSpace(ownerKey)
	if ownerKey == "" {
		return ""
	}
	const maxOwnerKeyRunes = 120
	if utf8.RuneCountInString(ownerKey) <= maxOwnerKeyRunes {
		return ownerKey
	}
	sum := sha256.Sum256([]byte(ownerKey))
	hashSuffix := "~" + hex.EncodeToString(sum[:6])
	return truncateRunes(ownerKey, maxOwnerKeyRunes-len(hashSuffix)) + hashSuffix
}

// FormatOwnedJobDesc appends an ownership tag " [xjt:<ownerKey>]" to desc while guaranteeing the total
// UTF-8 character count never exceeds XXL-JOB's varchar(255) column limit (fixes R6-P1-3, R7-P2-14, R8-P2-8).
func FormatOwnedJobDesc(desc, ownerKey string) string {
	ownerKey = normalizeOwnerKey(ownerKey)
	if ownerKey == "" {
		return truncateRunes(desc, maxJobDescRunes)
	}
	tag := " [xjt:" + ownerKey + "]"
	base := StripJobDescOwnerTag(desc)
	tagRunes := utf8.RuneCountInString(tag)
	if tagRunes < maxJobDescRunes {
		base = truncateRunes(base, maxJobDescRunes-tagRunes)
	} else {
		base = ""
	}
	return base + tag
}

// ExtractJobDescOwnerKey extracts "<ownerKey>" from a jobDesc ending with " [xjt:<ownerKey>]" (fixes R8-P2-8).
func ExtractJobDescOwnerKey(desc string) string {
	idx := strings.LastIndex(desc, " [xjt:")
	if idx < 0 || !strings.HasSuffix(desc, "]") {
		return ""
	}
	key := desc[idx+len(" [xjt:") : len(desc)-1]
	if key == "" || strings.ContainsAny(key, "[] \t\r\n") {
		return ""
	}
	return key
}

// StripJobDescOwnerTag removes any trailing " [xjt:<ownerKey>]" tag from desc (fixes R8-P2-8).
func StripJobDescOwnerTag(desc string) string {
	if ExtractJobDescOwnerKey(desc) == "" {
		return desc
	}
	idx := strings.LastIndex(desc, " [xjt:")
	return desc[:idx]
}

func normalizeChildJobID(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parts := strings.Split(raw, ",")
	var out []string
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return strings.Join(out, ",")
}

func truncateBody(b []byte) string {
	s := strings.TrimSpace(string(b))
	if utf8.RuneCountInString(s) > maxErrorBodySnippet {
		return truncateRunes(s, maxErrorBodySnippet) + "...(truncated)"
	}
	return s
}

func readBoundedBody(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxResponseBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxResponseBodyBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes limit", maxResponseBodyBytes)
	}
	return b, nil
}

// decodeReturnT strictly decodes a ReturnT JSON envelope and returns *AdminError on non-200 Code (fixes R6-P0-1, R6-P2-11).
func decodeReturnT(path string, body []byte) (ReturnT, error) {
	var ret ReturnT
	if err := json.Unmarshal(body, &ret); err != nil {
		return ReturnT{}, fmt.Errorf("unmarshal ReturnT from %s: %w (body=%s)", path, err, truncateBody(body))
	}
	if !ret.IsSuccess() {
		return ret, &AdminError{
			Path:       path,
			HTTPStatus: http.StatusOK,
			Code:       ret.Code,
			Msg:        ret.Msg,
		}
	}
	return ret, nil
}

// decodePageResponse strictly validates a /jobgroup/pageList or /jobinfo/pageList response before decoding.
// XXL-JOB 2.4.1's WebExceptionResolver returns HTTP 200 with {"code":500,"msg":"..."} when an exception occurs
// (such as a transient DB timeout). Without checking for "code" != 200 and the presence of "data",
// json.Unmarshal into PageResult would silently produce an empty slice and cause duplicate creation (fixes R6-P0-1).
func decodePageResponse(path string, body []byte, dest any) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("unmarshal %s: %w (body=%s)", path, err, truncateBody(body))
	}
	if codeRaw, hasCode := raw["code"]; hasCode {
		var code int
		if err := json.Unmarshal(codeRaw, &code); err == nil && code != SuccessCode {
			var msg string
			if msgRaw, hasMsg := raw["msg"]; hasMsg {
				_ = json.Unmarshal(msgRaw, &msg)
			}
			return &AdminError{
				Path:       path,
				HTTPStatus: http.StatusOK,
				Code:       code,
				Msg:        msg,
			}
		}
	}
	_, hasData := raw["data"]
	if !hasData {
		return &AdminError{
			Path:       path,
			HTTPStatus: http.StatusOK,
			Code:       FailCode,
			Msg:        fmt.Sprintf("pageList response from %s missing data field (body=%s)", path, truncateBody(body)),
		}
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("unmarshal %s payload: %w (body=%s)", path, err, truncateBody(body))
	}
	return nil
}

// InvalidateGroupSnapshot invalidates the cached snapshot for jobGroup (and increments its generation).
func (c *Client) InvalidateGroupSnapshot(jobGroup int) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.invalidateGroupLocked(jobGroup)
}

func (c *Client) invalidateGroupLocked(jobGroup int) {
	if jobGroup > 0 {
		c.groupGen[jobGroup]++
		delete(c.groupSnapshots, jobGroup)
	}
	c.groupGen[0]++
	delete(c.groupSnapshots, 0)
	c.pruneGroupGenLocked(jobGroup)
}

func (c *Client) pruneGroupGenLocked(keepGroup int) {
	if len(c.groupGen) <= 1024 {
		return
	}
	pruned := false
	for grp := range c.groupGen {
		if grp != 0 && grp != keepGroup {
			if _, active := c.groupSnapshots[grp]; !active {
				delete(c.groupGen, grp)
				pruned = true
			}
		}
	}
	if pruned {
		c.unscopedMutGen++
	}
}

func (c *Client) invalidateJobByID(jobID int) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	found := false
	for grp, snap := range c.groupSnapshots {
		if _, ok := snap.byID[jobID]; ok {
			c.groupGen[grp]++
			delete(c.groupSnapshots, grp)
			found = true
		}
	}
	if !found {
		c.unscopedMutGen++
	}
}

// mutateCachedJobByID applies fn to a copy of the cached JobInfo using copy-on-write so concurrent readers
// holding a previous jobGroupSnapshot never race on shared maps or slices (fixes R6-P1-1).
func (c *Client) mutateCachedJobByID(jobID int, fn func(*JobInfo)) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	found := false
	for grp, snap := range c.groupSnapshots {
		item, ok := snap.byID[jobID]
		if !ok {
			continue
		}
		found = true
		fn(&item)
		newByID := make(map[int]JobInfo, len(snap.byID))
		for k, v := range snap.byID {
			newByID[k] = v
		}
		newByID[jobID] = item

		newItems := make([]JobInfo, len(snap.items))
		copy(newItems, snap.items)
		for i := range newItems {
			if newItems[i].ID == jobID {
				newItems[i] = item
				break
			}
		}
		c.groupGen[grp]++
		c.groupSnapshots[grp] = jobGroupSnapshot{
			fetchedAt: snap.fetchedAt,
			items:     newItems,
			byID:      newByID,
		}
	}
	if !found {
		c.unscopedMutGen++
	}
}

// BaseURL returns the normalized base URL of XXL-JOB Admin.
func (c *Client) BaseURL() string {
	return c.baseURL
}

// ============================================================================
// 1. XXL-JOB EmbedServer OpenAPI (/api/registry, /api/registryRemove, /api/callback)
// ============================================================================

// RegisterExecutor registers an executor endpoint with XXL-JOB Admin via /api/registry.
func (c *Client) RegisterExecutor(ctx context.Context, appName, address string) error {
	param := RegistryParam{
		RegistryGroup: RegistryGroupExecutor,
		RegistryKey:   appName,
		RegistryValue: address,
	}
	return c.postOpenAPI(ctx, "/api/registry", param)
}

// RemoveExecutor proactively deregisters an executor endpoint from XXL-JOB Admin via /api/registryRemove.
func (c *Client) RemoveExecutor(ctx context.Context, appName, address string) error {
	param := RegistryParam{
		RegistryGroup: RegistryGroupExecutor,
		RegistryKey:   appName,
		RegistryValue: address,
	}
	return c.postOpenAPI(ctx, "/api/registryRemove", param)
}

// Callback reports asynchronous job execution results to XXL-JOB Admin via /api/callback.
func (c *Client) Callback(ctx context.Context, callbacks []HandleCallbackParam) error {
	return c.postOpenAPI(ctx, "/api/callback", callbacks)
}

func isAccessTokenRejection(err error) bool {
	if err == nil {
		return false
	}
	var adminErr *AdminError
	if errors.As(err, &adminErr) && (adminErr.HTTPStatus == http.StatusUnauthorized || adminErr.HTTPStatus == http.StatusForbidden) {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "access token") || strings.Contains(lower, "accesstoken") || strings.Contains(lower, "unauthorized")
}

func (c *Client) postOpenAPI(ctx context.Context, path string, payload any) error {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal openapi payload: %w", err)
	}
	err = c.doOpenAPIOnce(ctx, path, bodyBytes, c.accessToken)
	if err != nil && c.previousAccessToken != "" && c.previousAccessToken != c.accessToken && isAccessTokenRejection(err) {
		// During Secret rotation, older Admin pods may still expect the previous access token (fixes R7-P1-6).
		// Preserve non-token business rejections (e.g. "repeate callback" / "log item not found") from the
		// fallback attempt so IsTerminalCallbackRejection is never masked by the initial token error (fixes EG2-02).
		retryErr := c.doOpenAPIOnce(ctx, path, bodyBytes, c.previousAccessToken)
		if retryErr == nil || !isAccessTokenRejection(retryErr) {
			return retryErr
		}
	}
	return err
}

func (c *Client) doOpenAPIOnce(ctx context.Context, path string, bodyBytes []byte, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("build request for %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	if token != "" {
		req.Header.Set(AccessTokenHeader, token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request %s failed: %w", path, err)
	}
	defer resp.Body.Close()

	respBody, err := readBoundedBody(resp.Body)
	if err != nil {
		return fmt.Errorf("read response %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return &AdminError{
			Path:       path,
			HTTPStatus: resp.StatusCode,
			Msg:        truncateBody(respBody),
		}
	}

	_, err = decodeReturnT(path, respBody)
	return err
}

// ============================================================================
// 2. XXL-JOB Admin Web Console Management API (/login, /jobgroup/*, /jobinfo/*)
// ============================================================================

// Login authenticates with XXL-JOB Admin and stores the session cookie in the client's CookieJar.
func (c *Client) Login(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loginLocked(ctx)
}

func (c *Client) loginIfNeeded(ctx context.Context, seenGen uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loggedIn && c.loginGen != seenGen {
		// Another goroutine already completed re-login since seenGen (prevents login stampede).
		return nil
	}
	return c.loginLocked(ctx)
}

func (c *Client) loginLocked(ctx context.Context) error {
	c.loggedIn = false
	form := url.Values{}
	form.Set("userName", c.username)
	form.Set("password", c.password)
	form.Set("ifRemember", "on")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/login", strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("create login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute login request: %w", err)
	}
	defer resp.Body.Close()

	body, err := readBoundedBody(resp.Body)
	if err != nil {
		return fmt.Errorf("read login response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return &AdminError{
			Path:       "/login",
			HTTPStatus: resp.StatusCode,
			Msg:        truncateBody(body),
		}
	}

	if _, err := decodeReturnT("/login", body); err != nil {
		return err
	}
	c.loggedIn = true
	c.loginGen++
	return nil
}

func (c *Client) postFormWithAuth(ctx context.Context, path string, form url.Values) ([]byte, error) {
	c.mu.Lock()
	loggedIn := c.loggedIn
	gen := c.loginGen
	c.mu.Unlock()

	if !loggedIn {
		if err := c.loginIfNeeded(ctx, gen); err != nil {
			return nil, err
		}
		c.mu.Lock()
		gen = c.loginGen
		c.mu.Unlock()
	}

	body, needRelogin, err := c.doPostForm(ctx, path, form)
	if err != nil {
		return nil, err
	}
	if needRelogin {
		if err := c.loginIfNeeded(ctx, gen); err != nil {
			return nil, fmt.Errorf("re-login failed: %w", err)
		}
		var stillNeedRelogin bool
		body, stillNeedRelogin, err = c.doPostForm(ctx, path, form)
		if err != nil {
			return nil, err
		}
		if stillNeedRelogin {
			return nil, fmt.Errorf("session still unauthorized after re-login for %s", path)
		}
	}
	return body, nil
}

func (c *Client) doPostForm(ctx context.Context, path string, form url.Values) ([]byte, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, false, fmt.Errorf("build form request %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("post form %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := readBoundedBody(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("read response %s: %w", path, err)
	}

	lowerTrimmed := strings.ToLower(strings.TrimSpace(string(body)))
	// If redirected to /toLogin, status is 401/302, HTML login page returned, or ReturnT code is 302/401, trigger re-login.
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusFound ||
		strings.Contains(resp.Request.URL.Path, "toLogin") ||
		strings.HasPrefix(lowerTrimmed, "<!doctype html") ||
		strings.HasPrefix(lowerTrimmed, "<html") ||
		isSessionExpiredJSON(body) {
		return nil, true, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, &AdminError{
			Path:       path,
			HTTPStatus: resp.StatusCode,
			Msg:        truncateBody(body),
		}
	}
	return body, false, nil
}

func isSessionExpiredJSON(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	var probe struct {
		Code *int `json:"code"`
	}
	if err := json.Unmarshal(trimmed, &probe); err != nil || probe.Code == nil {
		return false
	}
	return *probe.Code == http.StatusFound || *probe.Code == http.StatusUnauthorized
}

// GetJobGroupByAppName queries XXL-JOB Admin for a JobGroup matching the exact appName across all pages,
// retrying on cross-page SQL OFFSET drift so concurrent group creations/deletions never hide an existing group (fixes EG2-03).
func (c *Client) GetJobGroupByAppName(ctx context.Context, appName string) (*JobGroup, error) {
	const pageSize = 100
	for attempt := 0; attempt < 3; attempt++ {
		firstRecordsTotal := -1
		pagesFetched := 0
		seenIDs := make(map[int]bool)
		consistent := true

		for start := 0; ; start += pageSize {
			form := url.Values{}
			form.Set("start", strconv.Itoa(start))
			form.Set("length", strconv.Itoa(pageSize))
			form.Set("appname", appName)

			body, err := c.postFormWithAuth(ctx, "/jobgroup/pageList", form)
			if err != nil {
				return nil, err
			}
			var page JobGroupPageResult
			if err := decodePageResponse("/jobgroup/pageList", body, &page); err != nil {
				return nil, err
			}
			pagesFetched++
			if firstRecordsTotal == -1 {
				firstRecordsTotal = page.RecordsTotal
			} else if page.RecordsTotal != firstRecordsTotal {
				consistent = false
			}
			for i := range page.Data {
				if page.Data[i].AppName == appName {
					found := page.Data[i]
					return &found, nil
				}
				if seenIDs[page.Data[i].ID] {
					consistent = false
				}
				seenIDs[page.Data[i].ID] = true
			}
			if len(page.Data) < pageSize || (page.RecordsTotal > 0 && start+len(page.Data) >= page.RecordsTotal) {
				break
			}
		}
		complete := consistent && ((pagesFetched == 1 && firstRecordsTotal <= pageSize) || firstRecordsTotal <= 0 || len(seenIDs) == firstRecordsTotal)
		if complete {
			return nil, nil
		}
	}
	return nil, fmt.Errorf("inconsistent /jobgroup/pageList pagination for appName=%q across concurrent modifications", appName)
}

// GetJobGroupByID searches all pages for a JobGroup with an exact ID match, retrying on cross-page SQL OFFSET drift (fixes EG2-03).
func (c *Client) GetJobGroupByID(ctx context.Context, id int) (*JobGroup, error) {
	if id <= 0 {
		return nil, nil
	}
	const pageSize = 100
	for attempt := 0; attempt < 3; attempt++ {
		firstRecordsTotal := -1
		pagesFetched := 0
		seenIDs := make(map[int]bool)
		consistent := true

		for start := 0; ; start += pageSize {
			form := url.Values{}
			form.Set("start", strconv.Itoa(start))
			form.Set("length", strconv.Itoa(pageSize))

			body, err := c.postFormWithAuth(ctx, "/jobgroup/pageList", form)
			if err != nil {
				return nil, err
			}
			var page JobGroupPageResult
			if err := decodePageResponse("/jobgroup/pageList", body, &page); err != nil {
				return nil, err
			}
			pagesFetched++
			if firstRecordsTotal == -1 {
				firstRecordsTotal = page.RecordsTotal
			} else if page.RecordsTotal != firstRecordsTotal {
				consistent = false
			}
			for i := range page.Data {
				if page.Data[i].ID == id {
					found := page.Data[i]
					return &found, nil
				}
				if seenIDs[page.Data[i].ID] {
					consistent = false
				}
				seenIDs[page.Data[i].ID] = true
			}
			if len(page.Data) < pageSize || (page.RecordsTotal > 0 && start+len(page.Data) >= page.RecordsTotal) {
				break
			}
		}
		complete := consistent && ((pagesFetched == 1 && firstRecordsTotal <= pageSize) || firstRecordsTotal <= 0 || len(seenIDs) == firstRecordsTotal)
		if complete {
			return nil, nil
		}
	}
	return nil, fmt.Errorf("inconsistent /jobgroup/pageList pagination for id=%d across concurrent modifications", id)
}

// SaveJobGroup creates a new JobGroup in XXL-JOB Admin.
func (c *Client) SaveJobGroup(ctx context.Context, group JobGroup) error {
	form := url.Values{}
	form.Set("appname", group.AppName)
	form.Set("title", group.Title)
	form.Set("addressType", strconv.Itoa(group.AddressType))
	form.Set("addressList", group.AddressList)

	body, err := c.postFormWithAuth(ctx, "/jobgroup/save", form)
	if err != nil {
		return err
	}
	_, err = decodeReturnT("/jobgroup/save", body)
	return err
}

// UpdateJobGroup updates an existing JobGroup in XXL-JOB Admin.
func (c *Client) UpdateJobGroup(ctx context.Context, group JobGroup) error {
	form := url.Values{}
	form.Set("id", strconv.Itoa(group.ID))
	form.Set("appname", group.AppName)
	form.Set("title", group.Title)
	form.Set("addressType", strconv.Itoa(group.AddressType))
	form.Set("addressList", group.AddressList)

	body, err := c.postFormWithAuth(ctx, "/jobgroup/update", form)
	if err != nil {
		return err
	}
	_, err = decodeReturnT("/jobgroup/update", body)
	return err
}

// EnsureJobGroup idempotently creates or updates a JobGroup and returns its numeric ID.
func (c *Client) EnsureJobGroup(ctx context.Context, desired JobGroup) (*JobGroup, error) {
	effective := desired
	if effective.AddressType == 1 && strings.TrimSpace(effective.AddressList) == "" {
		effective.AddressList = DefaultUnavailableSentinelAddress
	}

	existing, err := c.GetJobGroupByAppName(ctx, effective.AppName)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		if err := c.SaveJobGroup(ctx, effective); err != nil {
			return nil, err
		}
		existing, err = c.GetJobGroupByAppName(ctx, effective.AppName)
		if err != nil {
			return nil, err
		}
		if existing == nil {
			return nil, fmt.Errorf("jobgroup %s not found after save", effective.AppName)
		}
		return existing, nil
	}

	effective.ID = existing.ID
	if existing.Title != effective.Title || existing.AddressType != effective.AddressType ||
		(effective.AddressType == 1 && existing.AddressList != effective.AddressList) {
		if err := c.UpdateJobGroup(ctx, effective); err != nil {
			return nil, err
		}
	}
	return &effective, nil
}

// RemoveJobGroup deletes a JobGroup by ID from XXL-JOB Admin and invalidates its cached snapshot.
func (c *Client) RemoveJobGroup(ctx context.Context, id int) error {
	form := url.Values{}
	form.Set("id", strconv.Itoa(id))

	body, err := c.postFormWithAuth(ctx, "/jobgroup/remove", form)
	if err != nil {
		if !IsBusinessRejection(err) {
			c.InvalidateGroupSnapshot(id)
		}
		return err
	}
	if _, err = decodeReturnT("/jobgroup/remove", body); err != nil {
		if !IsBusinessRejection(err) {
			c.InvalidateGroupSnapshot(id)
		}
		return err
	}
	c.InvalidateGroupSnapshot(id)
	return nil
}

func (c *Client) getOrFetchGroupSnapshot(ctx context.Context, jobGroup int) (jobGroupSnapshot, error) {
	now := time.Now()
	c.cacheMu.Lock()
	for grp, s := range c.groupSnapshots {
		if now.Sub(s.fetchedAt) > 5*c.jobCacheTTL {
			delete(c.groupSnapshots, grp)
		}
	}
	c.pruneGroupGenLocked(jobGroup)
	if jobGroup > 0 {
		if snap, ok := c.groupSnapshots[jobGroup]; ok && now.Sub(snap.fetchedAt) < c.jobCacheTTL {
			c.cacheMu.Unlock()
			return snap, nil
		}
	}
	c.cacheMu.Unlock()

	return c.fetchGroupSnapshotFresh(ctx, jobGroup)
}

func (c *Client) fetchGroupSnapshotFresh(ctx context.Context, jobGroup int) (jobGroupSnapshot, error) {
	c.cacheMu.Lock()
	genBefore := c.groupGen[jobGroup]
	unscopedBefore := c.unscopedMutGen
	c.cacheMu.Unlock()

	var snap jobGroupSnapshot
	var complete bool
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		snap, complete, lastErr = c.fetchGroupPagesOnce(ctx, jobGroup)
		if lastErr != nil {
			return jobGroupSnapshot{}, lastErr
		}
		if complete {
			break
		}
	}
	if !complete {
		return jobGroupSnapshot{}, fmt.Errorf("inconsistent /jobinfo/pageList pagination for jobGroup=%d across concurrent modifications", jobGroup)
	}

	if jobGroup > 0 {
		c.cacheMu.Lock()
		if c.groupGen[jobGroup] == genBefore && c.unscopedMutGen == unscopedBefore {
			if _, exists := c.groupSnapshots[jobGroup]; !exists {
				for len(c.groupSnapshots) >= maxGroupSnapshots {
					oldestGroup := -1
					var oldestTime time.Time
					for grp, existingSnap := range c.groupSnapshots {
						if oldestGroup == -1 || existingSnap.fetchedAt.Before(oldestTime) {
							oldestGroup = grp
							oldestTime = existingSnap.fetchedAt
						}
					}
					if oldestGroup == -1 {
						break
					}
					// Only evict the heavy snapshot payload; preserve groupGen[oldestGroup] (pruned safely with
					// unscopedMutGen++ in pruneGroupGenLocked) so an in-flight fetchGroupSnapshotFresh that started
					// at genBefore=0 before a mutation can never resurrect a stale pre-mutation snapshot.
					delete(c.groupSnapshots, oldestGroup)
				}
			}
			c.groupSnapshots[jobGroup] = snap
		}
		c.cacheMu.Unlock()
	}
	return snap, nil
}

func (c *Client) fetchGroupPagesOnce(ctx context.Context, jobGroup int) (jobGroupSnapshot, bool, error) {
	const pageSize = 100
	var allItems []JobInfo
	byID := make(map[int]JobInfo)
	firstRecordsTotal := -1
	prevLastID := 0
	enforceDescID := true
	consistent := true

	for start := 0; ; start += pageSize {
		form := url.Values{}
		form.Set("start", strconv.Itoa(start))
		form.Set("length", strconv.Itoa(pageSize))
		form.Set("jobGroup", strconv.Itoa(jobGroup))
		form.Set("triggerStatus", "-1")

		body, err := c.postFormWithAuth(ctx, "/jobinfo/pageList", form)
		if err != nil {
			return jobGroupSnapshot{}, false, err
		}
		var page JobInfoPageResult
		if err := decodePageResponse("/jobinfo/pageList", body, &page); err != nil {
			return jobGroupSnapshot{}, false, err
		}
		if firstRecordsTotal == -1 {
			firstRecordsTotal = page.RecordsTotal
		} else if page.RecordsTotal != firstRecordsTotal {
			consistent = false
		}
		if start == 0 {
			for i := 1; i < len(page.Data); i++ {
				if page.Data[i].ID >= page.Data[i-1].ID {
					enforceDescID = false
					break
				}
			}
		}
		for i := range page.Data {
			item := page.Data[i]
			if start > 0 && enforceDescID && prevLastID > 0 && item.ID >= prevLastID {
				consistent = false
			}
			if _, exists := byID[item.ID]; !exists {
				allItems = append(allItems, item)
			} else {
				consistent = false
			}
			byID[item.ID] = item
			prevLastID = item.ID
		}
		if len(page.Data) < pageSize || (page.RecordsTotal > 0 && start+len(page.Data) >= page.RecordsTotal) {
			break
		}
	}

	complete := consistent && (firstRecordsTotal <= 0 || len(byID) == firstRecordsTotal)
	return jobGroupSnapshot{
		fetchedAt: time.Now(),
		items:     allItems,
		byID:      byID,
	}, complete, nil
}

// upsertCachedJobInfo updates the cached snapshot using copy-on-write so existing snapshots remain immutable (fixes R6-P1-1),
// and unconditionally bumps groupGen even when the target group is not yet cached so concurrent in-flight fetches never overwrite mutations (fixes EG2-03).
func (c *Client) upsertCachedJobInfo(info JobInfo) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if info.JobGroup > 0 {
		if _, cached := c.groupSnapshots[info.JobGroup]; !cached {
			c.groupGen[info.JobGroup]++
		}
	}
	if _, cachedZero := c.groupSnapshots[0]; !cachedZero {
		c.groupGen[0]++
	}
	for grp, snap := range c.groupSnapshots {
		if grp != 0 && grp != info.JobGroup {
			if _, existed := snap.byID[info.ID]; existed {
				newByID := make(map[int]JobInfo, len(snap.byID))
				for k, v := range snap.byID {
					if k != info.ID {
						newByID[k] = v
					}
				}
				newItems := make([]JobInfo, 0, len(snap.items))
				for _, item := range snap.items {
					if item.ID != info.ID {
						newItems = append(newItems, item)
					}
				}
				c.groupGen[grp]++
				c.groupSnapshots[grp] = jobGroupSnapshot{
					fetchedAt: snap.fetchedAt,
					items:     newItems,
					byID:      newByID,
				}
			}
			continue
		}
		newByID := make(map[int]JobInfo, len(snap.byID)+1)
		for k, v := range snap.byID {
			newByID[k] = v
		}
		newByID[info.ID] = info

		newItems := make([]JobInfo, len(snap.items))
		copy(newItems, snap.items)
		updated := false
		for i := range newItems {
			if newItems[i].ID == info.ID {
				newItems[i] = info
				updated = true
				break
			}
		}
		if !updated {
			newItems = append(newItems, info)
		}
		c.groupGen[grp]++
		c.groupSnapshots[grp] = jobGroupSnapshot{
			fetchedAt: snap.fetchedAt,
			items:     newItems,
			byID:      newByID,
		}
	}
}

func (c *Client) removeCachedJobInfo(jobID int) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	found := false
	for grp, snap := range c.groupSnapshots {
		if _, existed := snap.byID[jobID]; existed {
			found = true
			newByID := make(map[int]JobInfo, len(snap.byID))
			for k, v := range snap.byID {
				if k != jobID {
					newByID[k] = v
				}
			}
			newItems := make([]JobInfo, 0, len(snap.items))
			for _, item := range snap.items {
				if item.ID != jobID {
					newItems = append(newItems, item)
				}
			}
			c.groupGen[grp]++
			c.groupSnapshots[grp] = jobGroupSnapshot{
				fetchedAt: snap.fetchedAt,
				items:     newItems,
				byID:      newByID,
			}
		}
	}
	if !found {
		c.unscopedMutGen++
	}
}

// GetJobInfoByID searches for a task by its numeric ID in the specified jobGroup using the short-TTL snapshot cache.
// When jobGroup > 0 and the ID is not found in jobGroup, it only searches cached group snapshots (or when jobGroup == 0,
// queries jobGroup=0). Cross-group migration in EnsureJobInfo explicitly passes PreviousJobGroup (fixes R6-P1-3).
func (c *Client) GetJobInfoByID(ctx context.Context, jobGroup, jobID int) (*JobInfo, error) {
	if jobID <= 0 {
		return nil, nil
	}
	if jobGroup > 0 {
		snap, err := c.getOrFetchGroupSnapshot(ctx, jobGroup)
		if err != nil {
			return nil, err
		}
		if item, ok := snap.byID[jobID]; ok {
			copyItem := item
			return &copyItem, nil
		}
		return nil, nil
	}
	allSnap, err := c.getOrFetchGroupSnapshot(ctx, 0)
	if err != nil {
		return nil, err
	}
	if item, ok := allSnap.byID[jobID]; ok {
		copyItem := item
		return &copyItem, nil
	}
	return nil, nil
}

// FindJobInfo searches for a task by jobGroup and executorHandler (and optionally jobDesc) using the group snapshot cache.
func (c *Client) FindJobInfo(ctx context.Context, jobGroup int, executorHandler, jobDesc string) (*JobInfo, error) {
	snap, err := c.getOrFetchGroupSnapshot(ctx, jobGroup)
	if err != nil {
		return nil, err
	}
	return findJobInfoInSnapshot(snap, jobGroup, executorHandler, jobDesc), nil
}

func findJobInfoInSnapshot(snap jobGroupSnapshot, jobGroup int, executorHandler, jobDesc string) *JobInfo {
	for i := range snap.items {
		item := snap.items[i]
		if (jobGroup == 0 || item.JobGroup == jobGroup) && item.ExecutorHandler == executorHandler &&
			(jobDesc == "" || item.JobDesc == jobDesc) {
			copyItem := item
			return &copyItem
		}
	}
	return nil
}

// FindJobInfoByOwnerKey searches for a task by jobGroup and ownership marker [xjt:<ownerKey>] (fixes R8-P1-8).
func (c *Client) FindJobInfoByOwnerKey(ctx context.Context, jobGroup int, ownerKey string) (*JobInfo, error) {
	ownerKey = normalizeOwnerKey(ownerKey)
	if ownerKey == "" {
		return nil, nil
	}
	snap, err := c.getOrFetchGroupSnapshot(ctx, jobGroup)
	if err != nil {
		return nil, err
	}
	return findJobInfoByOwnerKeyInSnapshot(snap, jobGroup, ownerKey), nil
}

func findJobInfoByOwnerKeyInSnapshot(snap jobGroupSnapshot, jobGroup int, ownerKey string) *JobInfo {
	ownerKey = normalizeOwnerKey(ownerKey)
	if ownerKey == "" {
		return nil
	}
	for i := range snap.items {
		item := snap.items[i]
		if (jobGroup == 0 || item.JobGroup == jobGroup) && ExtractJobDescOwnerKey(item.JobDesc) == ownerKey {
			copyItem := item
			return &copyItem
		}
	}
	return nil
}

func normalizeGlueRemark(remark, fallback string) string {
	res := defaultString(remark, fallback)
	if utf8.RuneCountInString(res) < 4 {
		res = res + " (operator)"
	}
	if utf8.RuneCountInString(res) > 100 {
		res = truncateRunes(res, 100)
	}
	return res
}

func jobInfoToForm(info JobInfo) url.Values {
	form := url.Values{}
	if info.ID > 0 {
		form.Set("id", strconv.Itoa(info.ID))
	}
	form.Set("jobGroup", strconv.Itoa(info.JobGroup))
	form.Set("jobDesc", info.JobDesc)
	form.Set("author", info.Author)
	form.Set("alarmEmail", info.AlarmEmail)
	form.Set("scheduleType", defaultString(info.ScheduleType, "CRON"))
	form.Set("scheduleConf", info.ScheduleConf)
	form.Set("misfireStrategy", defaultString(info.MisfireStrategy, "DO_NOTHING"))
	form.Set("executorRouteStrategy", defaultString(info.ExecutorRouteStrategy, "FIRST"))
	form.Set("executorHandler", info.ExecutorHandler)
	form.Set("executorParam", info.ExecutorParam)
	form.Set("executorBlockStrategy", defaultString(info.ExecutorBlockStrategy, "SERIAL_EXECUTION"))
	form.Set("executorTimeout", strconv.Itoa(info.ExecutorTimeout))
	form.Set("executorFailRetryCount", strconv.Itoa(info.ExecutorFailRetryCount))
	form.Set("glueType", defaultString(info.GlueType, "BEAN"))
	form.Set("glueSource", info.GlueSource)
	form.Set("glueRemark", normalizeGlueRemark(info.GlueRemark, "Managed by xxl-job-operator"))
	form.Set("childJobId", normalizeChildJobID(info.ChildJobID))
	return form
}

// AddJobInfo creates a new scheduled task in XXL-JOB Admin and returns its assigned jobId.
func (c *Client) AddJobInfo(ctx context.Context, info JobInfo) (int, error) {
	// Ensure /jobinfo/add never sends a stale id or assumes a running triggerStatus (fixes R8-P1-7).
	info.ID = 0
	info.TriggerStatus = 0
	form := jobInfoToForm(info)
	body, err := c.postFormWithAuth(ctx, "/jobinfo/add", form)
	if err != nil {
		if !IsBusinessRejection(err) {
			c.InvalidateGroupSnapshot(info.JobGroup)
		}
		return 0, err
	}
	ret, err := decodeReturnT("/jobinfo/add", body)
	if err != nil {
		if !IsBusinessRejection(err) {
			c.InvalidateGroupSnapshot(info.JobGroup)
		}
		return 0, err
	}

	rawID := strings.Trim(string(ret.Content), `" `)
	id, err := strconv.Atoi(rawID)
	if err != nil {
		c.InvalidateGroupSnapshot(info.JobGroup)
		return 0, fmt.Errorf("parse jobId from /jobinfo/add content %q: %w", string(ret.Content), err)
	}
	info.ID = id
	info.TriggerStatus = 0
	info.ScheduleType = defaultString(info.ScheduleType, "CRON")
	info.MisfireStrategy = defaultString(info.MisfireStrategy, "DO_NOTHING")
	info.ExecutorRouteStrategy = defaultString(info.ExecutorRouteStrategy, "FIRST")
	info.ExecutorBlockStrategy = defaultString(info.ExecutorBlockStrategy, "SERIAL_EXECUTION")
	info.GlueType = defaultString(info.GlueType, "BEAN")
	info.GlueRemark = normalizeGlueRemark(info.GlueRemark, "Managed by xxl-job-operator")
	info.ChildJobID = normalizeChildJobID(info.ChildJobID)
	c.upsertCachedJobInfo(info)
	return id, nil
}

func (c *Client) preserveImmutableFieldsFromCache(info *JobInfo) bool {
	if info == nil || info.ID <= 0 {
		return false
	}
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	for _, snap := range c.groupSnapshots {
		if prev, ok := snap.byID[info.ID]; ok {
			info.TriggerStatus = prev.TriggerStatus
			if prev.GlueType != "" {
				info.GlueType = prev.GlueType
			}
			info.GlueSource = prev.GlueSource
			if prev.GlueRemark != "" {
				info.GlueRemark = prev.GlueRemark
			}
			return true
		}
	}
	return false
}

// UpdateJobInfo updates an existing scheduled task in XXL-JOB Admin.
func (c *Client) UpdateJobInfo(ctx context.Context, info JobInfo) error {
	form := jobInfoToForm(info)
	body, err := c.postFormWithAuth(ctx, "/jobinfo/update", form)
	if err != nil {
		if !IsBusinessRejection(err) {
			c.InvalidateGroupSnapshot(info.JobGroup)
		}
		c.invalidateJobByID(info.ID)
		return err
	}
	if _, err := decodeReturnT("/jobinfo/update", body); err != nil {
		if !IsBusinessRejection(err) {
			c.InvalidateGroupSnapshot(info.JobGroup)
		}
		c.invalidateJobByID(info.ID)
		return err
	}
	info.ScheduleType = defaultString(info.ScheduleType, "CRON")
	info.MisfireStrategy = defaultString(info.MisfireStrategy, "DO_NOTHING")
	info.ExecutorRouteStrategy = defaultString(info.ExecutorRouteStrategy, "FIRST")
	info.ExecutorBlockStrategy = defaultString(info.ExecutorBlockStrategy, "SERIAL_EXECUTION")
	info.GlueType = defaultString(info.GlueType, "BEAN")
	info.GlueRemark = normalizeGlueRemark(info.GlueRemark, "Managed by xxl-job-operator")
	info.ChildJobID = normalizeChildJobID(info.ChildJobID)
	// XXL-JOB 2.4 /jobinfo/update does not modify trigger_status, glue_type, glue_source, or glue_remark;
	// preserve cached values when present, or invalidate the group snapshot if the job was not cached so
	// unverified zero/stale immutable fields never poison a cached snapshot (fixes R8-P1-7, EG2-03).
	if c.preserveImmutableFieldsFromCache(&info) {
		c.upsertCachedJobInfo(info)
	} else {
		c.InvalidateGroupSnapshot(info.JobGroup)
		c.invalidateJobByID(info.ID)
	}
	return nil
}

// SaveGlueCode updates the script content for GLUE_SHELL / GLUE_PYTHON tasks via /jobcode/save.
func (c *Client) SaveGlueCode(ctx context.Context, jobID int, glueSource, glueRemark string) error {
	remark := normalizeGlueRemark(glueRemark, "Updated by xxl-job-operator")
	form := url.Values{}
	form.Set("id", strconv.Itoa(jobID))
	form.Set("glueSource", glueSource)
	form.Set("glueRemark", remark)

	body, err := c.postFormWithAuth(ctx, "/jobcode/save", form)
	if err != nil {
		c.invalidateJobByID(jobID)
		return err
	}
	if _, err := decodeReturnT("/jobcode/save", body); err != nil {
		c.invalidateJobByID(jobID)
		return err
	}
	c.mutateCachedJobByID(jobID, func(j *JobInfo) {
		j.GlueSource = glueSource
		j.GlueRemark = remark
	})
	return nil
}

// StartJobInfo starts scheduling for the given jobId.
func (c *Client) StartJobInfo(ctx context.Context, jobID int) error {
	form := url.Values{}
	form.Set("id", strconv.Itoa(jobID))
	body, err := c.postFormWithAuth(ctx, "/jobinfo/start", form)
	if err != nil {
		c.invalidateJobByID(jobID)
		return err
	}
	if _, err := decodeReturnT("/jobinfo/start", body); err != nil {
		c.invalidateJobByID(jobID)
		return err
	}
	c.mutateCachedJobByID(jobID, func(j *JobInfo) {
		j.TriggerStatus = 1
	})
	return nil
}

// StopJobInfo pauses scheduling for the given jobId.
func (c *Client) StopJobInfo(ctx context.Context, jobID int) error {
	form := url.Values{}
	form.Set("id", strconv.Itoa(jobID))
	body, err := c.postFormWithAuth(ctx, "/jobinfo/stop", form)
	if err != nil {
		c.invalidateJobByID(jobID)
		return err
	}
	if _, err := decodeReturnT("/jobinfo/stop", body); err != nil {
		c.invalidateJobByID(jobID)
		return err
	}
	c.mutateCachedJobByID(jobID, func(j *JobInfo) {
		j.TriggerStatus = 0
	})
	return nil
}

// RemoveJobInfo deletes the given jobId from XXL-JOB Admin.
func (c *Client) RemoveJobInfo(ctx context.Context, jobID int) error {
	form := url.Values{}
	form.Set("id", strconv.Itoa(jobID))
	body, err := c.postFormWithAuth(ctx, "/jobinfo/remove", form)
	if err != nil {
		c.invalidateJobByID(jobID)
		return err
	}
	if _, err := decodeReturnT("/jobinfo/remove", body); err != nil {
		c.invalidateJobByID(jobID)
		return err
	}
	c.removeCachedJobInfo(jobID)
	return nil
}

func jobInfoNeedsUpdate(existing, desired JobInfo) bool {
	return existing.JobGroup != desired.JobGroup ||
		existing.JobDesc != desired.JobDesc ||
		existing.Author != desired.Author ||
		existing.AlarmEmail != desired.AlarmEmail ||
		defaultString(existing.ScheduleType, "CRON") != defaultString(desired.ScheduleType, "CRON") ||
		existing.ScheduleConf != desired.ScheduleConf ||
		defaultString(existing.MisfireStrategy, "DO_NOTHING") != defaultString(desired.MisfireStrategy, "DO_NOTHING") ||
		defaultString(existing.ExecutorRouteStrategy, "FIRST") != defaultString(desired.ExecutorRouteStrategy, "FIRST") ||
		existing.ExecutorHandler != desired.ExecutorHandler ||
		existing.ExecutorParam != desired.ExecutorParam ||
		defaultString(existing.ExecutorBlockStrategy, "SERIAL_EXECUTION") != defaultString(desired.ExecutorBlockStrategy, "SERIAL_EXECUTION") ||
		existing.ExecutorTimeout != desired.ExecutorTimeout ||
		existing.ExecutorFailRetryCount != desired.ExecutorFailRetryCount ||
		defaultString(existing.GlueType, "BEAN") != defaultString(desired.GlueType, "BEAN") ||
		normalizeChildJobID(existing.ChildJobID) != normalizeChildJobID(desired.ChildJobID)
}

func (c *Client) lookupExistingJob(ctx context.Context, desired JobInfo, forceFresh bool) (*JobInfo, error) {
	fetchSnap := c.getOrFetchGroupSnapshot
	if forceFresh {
		fetchSnap = c.fetchGroupSnapshotFresh
	}

	targetSnap, err := fetchSnap(ctx, desired.JobGroup)
	if err != nil {
		return nil, err
	}

	// 1. Lookup by desired.ID in target group or scoped PreviousJobGroup (only when on the same AdminURL) (fixes R6-P1-3).
	sameAdmin := desired.PreviousAdminURL == "" || strings.TrimRight(desired.PreviousAdminURL, "/") == c.baseURL
	var prevSnap jobGroupSnapshot
	hasPrevSnap := false
	getPrevSnap := func() (jobGroupSnapshot, error) {
		if !hasPrevSnap {
			var snapErr error
			prevSnap, snapErr = fetchSnap(ctx, desired.PreviousJobGroup)
			if snapErr != nil {
				return jobGroupSnapshot{}, snapErr
			}
			hasPrevSnap = true
		}
		return prevSnap, nil
	}

	if desired.ID > 0 && sameAdmin {
		if item, ok := targetSnap.byID[desired.ID]; ok {
			if isOwnerCompatible(item.JobDesc, desired.OwnerKey) {
				copyItem := item
				return &copyItem, nil
			}
		}
		if desired.PreviousJobGroup > 0 && desired.PreviousJobGroup != desired.JobGroup {
			pSnap, err := getPrevSnap()
			if err != nil {
				return nil, err
			}
			if item, ok := pSnap.byID[desired.ID]; ok {
				if isOwnerCompatible(item.JobDesc, desired.OwnerKey) {
					copyItem := item
					return &copyItem, nil
				}
			}
		} else if desired.PreviousJobGroup == 0 && desired.OwnerKey == "" {
			// Direct Client caller without PreviousJobGroup metadata: check cached snapshots or group 0
			allSnap, err := fetchSnap(ctx, 0)
			if err != nil {
				return nil, err
			}
			if item, ok := allSnap.byID[desired.ID]; ok {
				copyItem := item
				return &copyItem, nil
			}
		}
	}

	// 2. Explicit adoption via AdoptJobID (annotation xxljob.io/adopt-job-id) (fixes R6-P1-3).
	if desired.AdoptJobID > 0 {
		if item, ok := targetSnap.byID[desired.AdoptJobID]; ok {
			if isOwnerCompatible(item.JobDesc, desired.OwnerKey) {
				copyItem := item
				return &copyItem, nil
			}
		}
	}

	// 3. Lookup by ownership marker when OwnerKey is set, or by (executorHandler, jobDesc) when OwnerKey is empty.
	if desired.OwnerKey != "" {
		if found := findJobInfoByOwnerKeyInSnapshot(targetSnap, desired.JobGroup, desired.OwnerKey); found != nil {
			return found, nil
		}
		if sameAdmin && desired.PreviousJobGroup > 0 && desired.PreviousJobGroup != desired.JobGroup {
			pSnap, err := getPrevSnap()
			if err != nil {
				return nil, err
			}
			if found := findJobInfoByOwnerKeyInSnapshot(pSnap, desired.PreviousJobGroup, desired.OwnerKey); found != nil {
				return found, nil
			}
		}
		return nil, nil
	}
	return findJobInfoInSnapshot(targetSnap, desired.JobGroup, desired.ExecutorHandler, desired.JobDesc), nil
}

// IsOwnerCompatible reports whether an existing task's jobDesc ownership tag is compatible with desiredOwnerKey.
func IsOwnerCompatible(existingDesc, desiredOwnerKey string) bool {
	desiredOwnerKey = normalizeOwnerKey(desiredOwnerKey)
	if desiredOwnerKey == "" {
		return true
	}
	existingOwner := ExtractJobDescOwnerKey(existingDesc)
	return existingOwner == "" || existingOwner == desiredOwnerKey
}

func isOwnerCompatible(existingDesc, desiredOwnerKey string) bool {
	return IsOwnerCompatible(existingDesc, desiredOwnerKey)
}

// EnsureJobInfo idempotently creates or updates a JobInfo and reconciles its TriggerStatus (start/stop).
func (c *Client) EnsureJobInfo(ctx context.Context, desired JobInfo, wantRunning bool) (*JobInfo, error) {
	desired.GlueType = defaultString(desired.GlueType, "BEAN")
	desired.ChildJobID = normalizeChildJobID(desired.ChildJobID)
	if desired.OwnerKey != "" {
		desired.JobDesc = FormatOwnedJobDesc(desired.JobDesc, desired.OwnerKey)
	}

	if strings.EqualFold(desired.ScheduleType, "NONE") && wantRunning {
		return nil, fmt.Errorf("scheduleType NONE cannot be started with triggerStatus=true")
	}

	fromCache := true
	existing, err := c.lookupExistingJob(ctx, desired, false)
	if err != nil {
		return nil, err
	}

	// Before creating a brand new job, always perform a fresh non-cached read so a stale snapshot or
	// transient error never causes duplicate job creation (fixes R6-P0-1, R6-P1-2).
	if existing == nil {
		fromCache = false
		existing, err = c.lookupExistingJob(ctx, desired, true)
		if err != nil {
			return nil, err
		}
	}

	// XXL-JOB 2.4 /jobinfo/update does not update glueType; recreating changes jobId and breaks childJobId/logs.
	// Require explicit confirmation via AllowGlueTypeRecreate (annotation xxljob.io/allow-gluetype-recreate=true).
	if existing != nil && existing.GlueType != "" && existing.GlueType != desired.GlueType && fromCache {
		fromCache = false
		existing, err = c.lookupExistingJob(ctx, desired, true)
		if err != nil {
			return nil, err
		}
	}
	if existing != nil && existing.GlueType != "" && existing.GlueType != desired.GlueType {
		if err := c.removeExistingJobForGlueTypeChange(ctx, existing, desired); err != nil {
			return nil, err
		}
		existing = nil
	}

	if existing != nil {
		synced, err := c.reconcileExistingJobInfo(ctx, existing, desired, wantRunning)
		if err == nil {
			return synced, nil
		}
		if !fromCache || !IsBusinessRejection(err) {
			return nil, err
		}
		// If existing came from the short-TTL snapshot cache and Admin rejected the mutation (e.g. the job
		// was manually deleted or modified in the Admin console while cached), refresh once and either reconcile
		// the fresh state or recreate if absent (fixes R10-P2-3, R11-P2-3).
		freshExisting, freshErr := c.lookupExistingJob(ctx, desired, true)
		if freshErr != nil {
			return nil, err
		}
		if freshExisting != nil {
			if freshExisting.GlueType != "" && freshExisting.GlueType != desired.GlueType {
				if err := c.removeExistingJobForGlueTypeChange(ctx, freshExisting, desired); err != nil {
					return nil, err
				}
				freshExisting = nil
			} else {
				return c.reconcileExistingJobInfo(ctx, freshExisting, desired, wantRunning)
			}
		}
		existing = nil
	}

	if existing == nil {
		id, err := c.AddJobInfo(ctx, desired)
		if err != nil {
			return nil, err
		}
		desired.ID = id
		// Note: /jobinfo/add already persists glueSource and glueRemark, so do not call SaveGlueCode redundantly on create (fixes R6-P2-10).
		if wantRunning {
			if err := c.StartJobInfo(ctx, id); err != nil {
				return nil, err
			}
			desired.TriggerStatus = 1
		} else {
			desired.TriggerStatus = 0
		}
		return &desired, nil
	}

	return &desired, nil
}

func (c *Client) removeExistingJobForGlueTypeChange(ctx context.Context, existing *JobInfo, desired JobInfo) error {
	if !desired.AllowGlueTypeRecreate {
		return fmt.Errorf("refusing to change glueType from %s to %s on jobId=%d without annotation xxljob.io/allow-gluetype-recreate=true (recreation assigns a new jobId)", existing.GlueType, desired.GlueType, existing.ID)
	}
	if existing.TriggerStatus != 0 {
		_ = c.StopJobInfo(ctx, existing.ID)
	}
	if err := c.RemoveJobInfo(ctx, existing.ID); err != nil {
		if !IsBusinessRejection(err) {
			return fmt.Errorf("remove jobId=%d for glueType migration (%s -> %s): %w", existing.ID, existing.GlueType, desired.GlueType, err)
		}
		// If Admin returned a business rejection (ReturnT.Code=500), verify whether the old job was already deleted.
		// If the old job still exists (e.g. referenced by childJobId or still running), do NOT proceed to AddJobInfo.
		c.InvalidateGroupSnapshot(existing.JobGroup)
		stillExisting, checkErr := c.GetJobInfoByID(ctx, existing.JobGroup, existing.ID)
		if checkErr != nil || stillExisting != nil {
			return fmt.Errorf("remove jobId=%d for glueType migration (%s -> %s): %w", existing.ID, existing.GlueType, desired.GlueType, err)
		}
	}
	return nil
}

func (c *Client) reconcileExistingJobInfo(ctx context.Context, existing *JobInfo, desired JobInfo, wantRunning bool) (*JobInfo, error) {
	desired.ID = existing.ID
	desired.TriggerStatus = existing.TriggerStatus

	// When transitioning to stopped (!wantRunning), stop the job BEFORE updating scheduleType/scheduleConf
	// so changing CRON -> NONE on a running job never fails XXL-JOB's next-trigger-time calculation (fixes R6-P2-9).
	if !wantRunning && existing.TriggerStatus != 0 {
		if err := c.StopJobInfo(ctx, desired.ID); err != nil {
			return nil, err
		}
		existing.TriggerStatus = 0
		desired.TriggerStatus = 0
	}

	if jobInfoNeedsUpdate(*existing, desired) {
		// Preserve existing GlueSource/GlueRemark when calling UpdateJobInfo because /jobinfo/update does not
		// persist glue code; SaveGlueCode below updates the cached GlueSource only after /jobcode/save succeeds.
		updatePayload := desired
		updatePayload.GlueSource = existing.GlueSource
		updatePayload.GlueRemark = existing.GlueRemark
		if err := c.UpdateJobInfo(ctx, updatePayload); err != nil {
			return nil, err
		}
	}
	if desired.GlueSource != "" && strings.HasPrefix(strings.ToUpper(desired.GlueType), "GLUE_") && existing.GlueSource != desired.GlueSource {
		if err := c.SaveGlueCode(ctx, desired.ID, desired.GlueSource, desired.GlueRemark); err != nil {
			return nil, err
		}
		desired.GlueRemark = normalizeGlueRemark(desired.GlueRemark, "Updated by xxl-job-operator")
	}

	if wantRunning && existing.TriggerStatus != 1 {
		if err := c.StartJobInfo(ctx, desired.ID); err != nil {
			return nil, err
		}
		desired.TriggerStatus = 1
	}

	return &desired, nil
}

func defaultString(val, fallback string) string {
	if strings.TrimSpace(val) == "" {
		return fallback
	}
	return val
}

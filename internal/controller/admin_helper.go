package controller

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
	"github.com/xxl-job/xxl-job-operator/pkg/proxyexecutor"
	"github.com/xxl-job/xxl-job-operator/pkg/xxlclient"
)

const (
	// AnnotationForceDelete allows force-removing finalizers when XXL-JOB Admin is permanently unreachable.
	AnnotationForceDelete = "xxljob.io/force-delete"
	// AnnotationAllowGlueTypeRecreate explicitly authorizes delete-and-recreate when glueType is modified (fixes P1-8).
	AnnotationAllowGlueTypeRecreate = "xxljob.io/allow-gluetype-recreate"

	// MaxFinalizerRetryDuration is the maximum duration a deleting CR will wait for remote Admin API cleanup
	// before bypassing remote network/API errors and releasing its finalizer (fixes P1-2 & R4-P1-3).
	MaxFinalizerRetryDuration = 10 * time.Minute

	maxClientCacheEntries    = 64
	maxTokenCacheEntries     = 256
	maxPreviousAdminURLs     = 8
	tokenCacheTTL            = 30 * time.Second
	tokenRotationGracePeriod = 10 * time.Minute
	forceRefreshCooldown     = 5 * time.Second

	ConditionAdminEndpointSynced    = proxyexecutor.ConditionAdminEndpointSynced
	ConditionPreviousAdminURLs      = proxyexecutor.ConditionPreviousAdminURLs
	ConditionPreviousAdminEndpoints = "PreviousAdminEndpoints"
)

// syncedAdminEndpointMeta aliases proxyexecutor.SyncedAdminEndpointMeta so controllers and K8sJobProxy
// share the exact same ConditionAdminEndpointSynced JSON wire format.
type syncedAdminEndpointMeta = proxyexecutor.SyncedAdminEndpointMeta

type cachedEndpointAuthSnapshot struct {
	adminName string
	adminUID  types.UID
	auth      xxljobv1alpha1.AuthSpec
}

// ClientFactory allows overriding xxlclient creation in unit tests.
type ClientFactory func(cfg xxlclient.Config) (*xxlclient.Client, error)

type cachedTokenEntry struct {
	adminName      string
	adminUID       types.UID
	secretRef      string
	token          string
	expiresAt      time.Time
	prevToken      string
	prevValidUntil time.Time
	lastForcedAt   time.Time
	errMsg         string
}

func readerCacheID(reader client.Reader) string {
	if reader == nil {
		return "nil"
	}
	v := reflect.ValueOf(reader)
	switch v.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return fmt.Sprintf("%T:0x%x", reader, v.Pointer())
	default:
		return fmt.Sprintf("%T", reader)
	}
}

func adminAccessTokenSecretRefKey(admin *xxljobv1alpha1.XxlJobAdmin) string {
	if admin == nil || admin.Spec.Auth.AccessTokenSecretRef == nil {
		return ""
	}
	return strings.TrimSpace(admin.Spec.Auth.AccessTokenSecretRef.Name) + "/" + strings.TrimSpace(admin.Spec.Auth.AccessTokenSecretRef.Key)
}

func sameAdminIdentity(prevName string, prevUID types.UID, admin *xxljobv1alpha1.XxlJobAdmin) bool {
	if admin == nil {
		return false
	}
	if prevName == "" {
		return true
	}
	if prevName != admin.Name {
		return false
	}
	if prevUID != "" && admin.UID != "" && prevUID != admin.UID {
		return false
	}
	return true
}

var (
	clientCacheMu      sync.Mutex
	clientCache        = make(map[string]*xxlclient.Client)
	clientCacheByAdmin = make(map[string]string)
	clientCacheOrder   []string

	tokenCacheMu sync.Mutex
	tokenCache   = make(map[string]cachedTokenEntry)

	secretCacheMu sync.Mutex
	secretCache   = make(map[string]cachedTokenEntry)

	endpointAuthMu    sync.Mutex
	endpointAuthCache = make(map[string]cachedEndpointAuthSnapshot)
	endpointAuthOrder []string
)

func recordEndpointAuthSnapshot(namespace, adminURL string, admin *xxljobv1alpha1.XxlJobAdmin) {
	u := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	if namespace == "" || u == "" || admin == nil {
		return
	}
	key := namespace + "|" + u
	snap := cachedEndpointAuthSnapshot{
		adminName: admin.Name,
		adminUID:  admin.UID,
		auth:      *admin.Spec.Auth.DeepCopy(),
	}
	endpointAuthMu.Lock()
	defer endpointAuthMu.Unlock()
	if _, exists := endpointAuthCache[key]; exists {
		for i, k := range endpointAuthOrder {
			if k == key {
				endpointAuthOrder = append(endpointAuthOrder[:i], endpointAuthOrder[i+1:]...)
				break
			}
		}
	} else {
		for len(endpointAuthCache) >= maxClientCacheEntries && len(endpointAuthOrder) > 0 {
			oldest := endpointAuthOrder[0]
			endpointAuthOrder = endpointAuthOrder[1:]
			delete(endpointAuthCache, oldest)
		}
	}
	endpointAuthCache[key] = snap
	endpointAuthOrder = append(endpointAuthOrder, key)
}

func lookupEndpointAuthSnapshot(namespace, adminURL string) (cachedEndpointAuthSnapshot, bool) {
	u := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	if namespace == "" || u == "" {
		return cachedEndpointAuthSnapshot{}, false
	}
	endpointAuthMu.Lock()
	defer endpointAuthMu.Unlock()
	snap, ok := endpointAuthCache[namespace+"|"+u]
	return snap, ok
}

func encodeSyncedAdminEndpointMeta(admin *xxljobv1alpha1.XxlJobAdmin, adminURL string) string {
	return proxyexecutor.EncodeSyncedAdminEndpointMeta(admin, adminURL)
}

func extractSyncedAdminEndpointMeta(conditions []metav1.Condition) *syncedAdminEndpointMeta {
	return proxyexecutor.DecodeSyncedAdminEndpointMeta(conditions)
}

func extractPreviousAdminEndpointMetas(admin *xxljobv1alpha1.XxlJobAdmin) []syncedAdminEndpointMeta {
	if admin == nil {
		return nil
	}
	cond := meta.FindStatusCondition(admin.Status.Conditions, ConditionPreviousAdminEndpoints)
	if cond == nil || strings.TrimSpace(cond.Message) == "" {
		return nil
	}
	var parsed []syncedAdminEndpointMeta
	if err := json.Unmarshal([]byte(cond.Message), &parsed); err != nil {
		return nil
	}
	return parsed
}

func lookupAdminRecordedEndpointMeta(admin *xxljobv1alpha1.XxlJobAdmin, targetURL string) *syncedAdminEndpointMeta {
	targetURL = strings.TrimRight(strings.TrimSpace(targetURL), "/")
	if admin == nil || targetURL == "" {
		return nil
	}
	if cur := extractSyncedAdminEndpointMeta(admin.Status.Conditions); cur != nil &&
		strings.TrimRight(strings.TrimSpace(cur.AdminURL), "/") == targetURL &&
		sameAdminIdentity(cur.AdminName, types.UID(cur.AdminUID), admin) {
		cp := *cur
		return &cp
	}
	prevList := extractPreviousAdminEndpointMetas(admin)
	for i := len(prevList) - 1; i >= 0; i-- {
		item := prevList[i]
		if strings.TrimRight(strings.TrimSpace(item.AdminURL), "/") == targetURL &&
			sameAdminIdentity(item.AdminName, types.UID(item.AdminUID), admin) {
			cp := item
			return &cp
		}
	}
	return nil
}

func recordPreviousAdminURLCondition(admin *xxljobv1alpha1.XxlJobAdmin, prevURL string) {
	prevURL = strings.TrimRight(strings.TrimSpace(prevURL), "/")
	if admin == nil || prevURL == "" {
		return
	}
	currentURL := strings.TrimRight(ResolveAdminURL(admin), "/")
	var urls []string
	for _, existing := range extractPreviousAdminURLs(admin) {
		if existing != "" && existing != prevURL && existing != currentURL {
			urls = append(urls, existing)
		}
	}
	urls = append(urls, prevURL)
	if len(urls) > maxPreviousAdminURLs {
		urls = urls[len(urls)-maxPreviousAdminURLs:]
	}
	meta.SetStatusCondition(&admin.Status.Conditions, metav1.Condition{
		Type:               ConditionPreviousAdminURLs,
		Status:             metav1.ConditionTrue,
		Reason:             "AdminURLUpdated",
		Message:            strings.Join(urls, ","),
		LastTransitionTime: metav1.Now(),
	})

	var prevMeta *syncedAdminEndpointMeta
	if cur := extractSyncedAdminEndpointMeta(admin.Status.Conditions); cur != nil &&
		strings.TrimRight(strings.TrimSpace(cur.AdminURL), "/") == prevURL &&
		sameAdminIdentity(cur.AdminName, types.UID(cur.AdminUID), admin) {
		cp := *cur
		prevMeta = &cp
	} else if snap, ok := lookupEndpointAuthSnapshot(admin.Namespace, prevURL); ok && sameAdminIdentity(snap.adminName, snap.adminUID, admin) {
		trimmedPw := strings.TrimSpace(snap.auth.Password)
		sanitizedTok := sanitizeTokenValue(snap.auth.AccessToken)
		m := syncedAdminEndpointMeta{
			AdminName:              admin.Name,
			AdminUID:               string(admin.UID),
			AdminURL:               prevURL,
			ManagedInCluster:       proxyexecutor.IsClusterLocalAdminServiceURL(prevURL, admin.Name, admin.Namespace),
			AuthCaptured:           true,
			Username:               snap.auth.Username,
			UsedDefaultPassword:    snap.auth.PasswordSecretRef == nil && (trimmedPw == "" || trimmedPw == "123456"),
			UsedDefaultAccessToken: snap.auth.AccessTokenSecretRef == nil && (sanitizedTok == "" || sanitizedTok == "default_token"),
		}
		if snap.auth.PasswordSecretRef != nil {
			m.PasswordSecretRef = snap.auth.PasswordSecretRef.DeepCopy()
		}
		if snap.auth.AccessTokenSecretRef != nil {
			m.AccessTokenSecretRef = snap.auth.AccessTokenSecretRef.DeepCopy()
		}
		prevMeta = &m
	}
	if prevMeta != nil {
		var metas []syncedAdminEndpointMeta
		for _, existing := range extractPreviousAdminEndpointMetas(admin) {
			eu := strings.TrimRight(strings.TrimSpace(existing.AdminURL), "/")
			if eu != "" && eu != prevURL && eu != currentURL {
				metas = append(metas, existing)
			}
		}
		metas = append(metas, *prevMeta)
		if len(metas) > maxPreviousAdminURLs {
			metas = metas[len(metas)-maxPreviousAdminURLs:]
		}
		if raw, err := json.Marshal(metas); err == nil {
			meta.SetStatusCondition(&admin.Status.Conditions, metav1.Condition{
				Type:               ConditionPreviousAdminEndpoints,
				Status:             metav1.ConditionTrue,
				Reason:             "AdminURLUpdated",
				Message:            string(raw),
				LastTransitionTime: metav1.Now(),
			})
		}
	}
}

func syncAdminCurrentEndpointCondition(admin *xxljobv1alpha1.XxlJobAdmin, adminURL string) bool {
	if admin == nil {
		return false
	}
	recordEndpointAuthSnapshot(admin.Namespace, adminURL, admin)
	msg := encodeSyncedAdminEndpointMeta(admin, adminURL)
	if msg == "" {
		return false
	}
	prev := meta.FindStatusCondition(admin.Status.Conditions, ConditionAdminEndpointSynced)
	if prev != nil && prev.Status == metav1.ConditionTrue && prev.Message == msg {
		return false
	}
	meta.SetStatusCondition(&admin.Status.Conditions, metav1.Condition{
		Type:               ConditionAdminEndpointSynced,
		Status:             metav1.ConditionTrue,
		Reason:             "EndpointSynced",
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
	return true
}

func extractPreviousAdminURLs(admin *xxljobv1alpha1.XxlJobAdmin) []string {
	if admin == nil {
		return nil
	}
	cond := meta.FindStatusCondition(admin.Status.Conditions, ConditionPreviousAdminURLs)
	if cond == nil || strings.TrimSpace(cond.Message) == "" {
		return nil
	}
	var res []string
	for _, part := range strings.Split(cond.Message, ",") {
		u := strings.TrimRight(strings.TrimSpace(part), "/")
		if u != "" {
			res = append(res, u)
		}
	}
	return res
}

func adminHasRecordedPreviousURL(admin *xxljobv1alpha1.XxlJobAdmin, targetURL string) bool {
	targetURL = strings.TrimRight(strings.TrimSpace(targetURL), "/")
	if admin == nil || targetURL == "" {
		return false
	}
	if strings.TrimRight(strings.TrimSpace(admin.Status.AdminURL), "/") == targetURL {
		return true
	}
	for _, u := range extractPreviousAdminURLs(admin) {
		if u == targetURL {
			return true
		}
	}
	return false
}

// getOrCreateCachedClient returns a cached *xxlclient.Client for the given Config so login session cookies
// and HTTP keep-alive connections are reused across Reconcile cycles (with LRU eviction and immediate
// replacement when credentials rotate for the same Admin BaseURL, fixes R6-P2-23).
func getOrCreateCachedClient(cfg xxlclient.Config) (*xxlclient.Client, error) {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s|%s", cfg.BaseURL, cfg.Username, cfg.Password, cfg.AccessToken, cfg.PreviousAccessToken)))
	key := hex.EncodeToString(h[:])
	adminIdentity := fmt.Sprintf("%s|%s", strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/"), cfg.Username)

	clientCacheMu.Lock()
	defer clientCacheMu.Unlock()

	if cli, ok := clientCache[key]; ok {
		touchClientCacheKeyLocked(key)
		return cli, nil
	}

	// Evict any stale client entry for the same Admin URL whose credentials just rotated
	if prevKey, exists := clientCacheByAdmin[adminIdentity]; exists && prevKey != key {
		removeClientCacheKeyLocked(prevKey)
	}

	for len(clientCache) >= maxClientCacheEntries && len(clientCacheOrder) > 0 {
		oldest := clientCacheOrder[0]
		removeClientCacheKeyLocked(oldest)
	}
	cli, err := xxlclient.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	clientCache[key] = cli
	clientCacheByAdmin[adminIdentity] = key
	clientCacheOrder = append(clientCacheOrder, key)
	return cli, nil
}

func touchClientCacheKeyLocked(key string) {
	for i, k := range clientCacheOrder {
		if k == key {
			clientCacheOrder = append(clientCacheOrder[:i], clientCacheOrder[i+1:]...)
			break
		}
	}
	clientCacheOrder = append(clientCacheOrder, key)
}

func removeClientCacheKeyLocked(key string) {
	delete(clientCache, key)
	for i, k := range clientCacheOrder {
		if k == key {
			clientCacheOrder = append(clientCacheOrder[:i], clientCacheOrder[i+1:]...)
			break
		}
	}
	for adminID, k := range clientCacheByAdmin {
		if k == key {
			delete(clientCacheByAdmin, adminID)
		}
	}
}

// ResolveAdminAndClient fetches the referenced XxlJobAdmin resource, resolves Secret credentials,
// and returns a cached or newly initialized *xxlclient.Client.
func ResolveAdminAndClient(
	ctx context.Context,
	k8sClient client.Client,
	factory ClientFactory,
	ref xxljobv1alpha1.LocalObjectReference,
	defaultNamespace string,
) (*xxljobv1alpha1.XxlJobAdmin, *xxlclient.Client, error) {
	return ResolveAdminAndClientWithReader(ctx, k8sClient, k8sClient, factory, ref, defaultNamespace)
}

// ResolveAdminAndClientWithReader uses secretReader (e.g., mgr.GetAPIReader()) to read Secrets without
// caching all cluster Secrets in the informer cache (fixes P1-9), and enforces same-namespace references (fixes P1-7).
func ResolveAdminAndClientWithReader(
	ctx context.Context,
	k8sClient client.Client,
	secretReader client.Reader,
	factory ClientFactory,
	ref xxljobv1alpha1.LocalObjectReference,
	defaultNamespace string,
) (*xxljobv1alpha1.XxlJobAdmin, *xxlclient.Client, error) {
	if secretReader == nil {
		secretReader = k8sClient
	}
	ns := ref.Namespace
	if ns == "" {
		ns = defaultNamespace
	}
	if defaultNamespace != "" && ns != defaultNamespace {
		return nil, nil, fmt.Errorf("cross-namespace adminRef (%s/%s) from namespace %s is not allowed", ns, ref.Name, defaultNamespace)
	}
	var admin xxljobv1alpha1.XxlJobAdmin
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: ref.Name}, &admin); err != nil {
		return nil, nil, fmt.Errorf("get XxlJobAdmin %s/%s: %w", ns, ref.Name, err)
	}

	if strings.TrimSpace(admin.Spec.ExternalURL) != "" {
		if err := validateExternalAdminURL(admin.Spec.ExternalURL); err != nil {
			return &admin, nil, err
		}
	} else if _, err := normalizeAndValidateContextPath(admin.Spec.ContextPath); err != nil {
		return &admin, nil, err
	}

	baseURL := ResolveAdminURL(&admin)
	username := admin.Spec.Auth.Username
	if username == "" {
		username = "admin"
	}

	password, err := resolveSecretOrValue(ctx, secretReader, admin.Namespace, admin.Spec.Auth.PasswordSecretRef, admin.Spec.Auth.Password, "123456")
	if err != nil {
		return &admin, nil, fmt.Errorf("resolve admin password: %w", err)
	}

	accessToken, prevAccessToken, err := resolveSecretOrValueWithPrevious(ctx, secretReader, admin.Namespace, admin.Spec.Auth.AccessTokenSecretRef, admin.Spec.Auth.AccessToken, "default_token")
	if err != nil {
		return &admin, nil, fmt.Errorf("resolve admin accessToken: %w", err)
	}
	accessToken = sanitizeTokenValue(accessToken)
	if accessToken == "" {
		if admin.Spec.Auth.AccessTokenSecretRef != nil && admin.Spec.Auth.AccessTokenSecretRef.Name != "" {
			invalidateSecretCacheOnly(secretReader, admin.Namespace, admin.Spec.Auth.AccessTokenSecretRef)
			return &admin, nil, fmt.Errorf("resolve admin accessToken: key %q in Secret %s/%s must not be empty or whitespace-only",
				admin.Spec.Auth.AccessTokenSecretRef.Key, admin.Namespace, admin.Spec.Auth.AccessTokenSecretRef.Name)
		}
		accessToken = "default_token"
	}
	prevAccessToken = sanitizeTokenValue(prevAccessToken)

	if factory == nil {
		factory = getOrCreateCachedClient
	}
	cli, err := factory(xxlclient.Config{
		BaseURL:             baseURL,
		Username:            username,
		Password:            password,
		AccessToken:         accessToken,
		PreviousAccessToken: prevAccessToken,
	})
	if err != nil {
		return &admin, nil, err
	}
	recordEndpointAuthSnapshot(admin.Namespace, baseURL, &admin)
	return &admin, cli, nil
}

// resolvePreviousAdminClient locates the XxlJobAdmin CR responsible for prevAdminURL (including when an
// XxlJobAdmin modified Spec.ExternalURL / Port / ContextPath in place) and builds a client explicitly
// targeting prevAdminURL using saved endpoint credentials.
func resolvePreviousAdminClient(
	ctx context.Context,
	k8sClient client.Client,
	secretReader client.Reader,
	factory ClientFactory,
	namespace, prevAdminURL string,
	savedMeta *syncedAdminEndpointMeta,
	fallbackAdminName string,
) (*xxljobv1alpha1.XxlJobAdmin, *xxlclient.Client, error) {
	return resolvePreviousAdminClientInternal(ctx, k8sClient, secretReader, factory, namespace, prevAdminURL, savedMeta, fallbackAdminName, false)
}

func resolveReachablePreviousAdminClient(
	ctx context.Context,
	k8sClient client.Client,
	secretReader client.Reader,
	factory ClientFactory,
	namespace, prevAdminURL string,
	savedMeta *syncedAdminEndpointMeta,
	fallbackAdminName string,
) (*xxljobv1alpha1.XxlJobAdmin, *xxlclient.Client, error) {
	return resolvePreviousAdminClientInternal(ctx, k8sClient, secretReader, factory, namespace, prevAdminURL, savedMeta, fallbackAdminName, true)
}

func resolvePreviousAdminClientInternal(
	ctx context.Context,
	k8sClient client.Client,
	secretReader client.Reader,
	factory ClientFactory,
	namespace, prevAdminURL string,
	savedMeta *syncedAdminEndpointMeta,
	fallbackAdminName string,
	skipRetiredManaged bool,
) (*xxljobv1alpha1.XxlJobAdmin, *xxlclient.Client, error) {
	if secretReader == nil {
		secretReader = k8sClient
	}
	prevAdminURL = strings.TrimRight(strings.TrimSpace(prevAdminURL), "/")
	if namespace == "" || prevAdminURL == "" {
		return nil, nil, nil
	}
	if err := validateExternalAdminURL(prevAdminURL); err != nil {
		return nil, nil, nil
	}

	listReader := secretReader
	if listReader == nil {
		listReader = k8sClient
	}
	var adminList xxljobv1alpha1.XxlJobAdminList
	if err := listReader.List(ctx, &adminList, client.InNamespace(namespace)); err != nil {
		return nil, nil, err
	}

	pickCandidate := func(matchFn func(*xxljobv1alpha1.XxlJobAdmin) bool) *xxljobv1alpha1.XxlJobAdmin {
		var deleting *xxljobv1alpha1.XxlJobAdmin
		for i := range adminList.Items {
			item := &adminList.Items[i]
			if !matchFn(item) {
				continue
			}
			if item.DeletionTimestamp.IsZero() {
				return item
			}
			if deleting == nil {
				deleting = item
			}
		}
		return deleting
	}

	// 1. Exact current Spec URL match
	matchedAdmin := pickCandidate(func(item *xxljobv1alpha1.XxlJobAdmin) bool {
		return strings.TrimRight(ResolveAdminURL(item), "/") == prevAdminURL
	})
	// 2. Recorded Status.AdminURL or PreviousAdminURLs condition match (in-place URL edit)
	if matchedAdmin == nil {
		matchedAdmin = pickCandidate(func(item *xxljobv1alpha1.XxlJobAdmin) bool {
			return adminHasRecordedPreviousURL(item, prevAdminURL)
		})
	}
	// 3. Persisted AdminEndpointSynced metadata match on the dependent CR
	if matchedAdmin == nil && savedMeta != nil && strings.TrimSpace(savedMeta.AdminName) != "" &&
		(savedMeta.AdminURL == "" || strings.TrimRight(strings.TrimSpace(savedMeta.AdminURL), "/") == prevAdminURL) {
		savedName := strings.TrimSpace(savedMeta.AdminName)
		savedUID := strings.TrimSpace(savedMeta.AdminUID)
		matchedAdmin = pickCandidate(func(item *xxljobv1alpha1.XxlJobAdmin) bool {
			if item.Name != savedName {
				return false
			}
			if savedUID != "" && item.UID != "" && string(item.UID) != savedUID {
				return false
			}
			return true
		})
	}
	// 4. In-memory auth snapshot match for prevAdminURL
	snap, hasSnap := lookupEndpointAuthSnapshot(namespace, prevAdminURL)
	if matchedAdmin == nil && hasSnap && snap.adminName != "" {
		matchedAdmin = pickCandidate(func(item *xxljobv1alpha1.XxlJobAdmin) bool {
			if item.Name != snap.adminName {
				return false
			}
			if snap.adminUID != "" && item.UID != "" && item.UID != snap.adminUID {
				return false
			}
			return true
		})
	}
	// 5. Pre-upgrade fallback for cluster-internal Service URLs (<fallbackAdminName>.<namespace>.svc.cluster.local)
	// when ConditionAdminEndpointSynced was not yet recorded on the dependent CR.
	if matchedAdmin == nil && savedMeta == nil && strings.TrimSpace(fallbackAdminName) != "" {
		fbName := strings.TrimSpace(fallbackAdminName)
		svcHost := strings.ToLower(fmt.Sprintf("%s.%s.svc.cluster.local", fbName, namespace))
		if u, parseErr := url.Parse(prevAdminURL); parseErr == nil && strings.EqualFold(u.Hostname(), svcHost) {
			matchedAdmin = pickCandidate(func(item *xxljobv1alpha1.XxlJobAdmin) bool {
				return item.Name == fbName
			})
		}
	}

	if matchedAdmin == nil {
		return nil, nil, nil
	}
	if skipRetiredManaged && (isSameManagedAdminDBUpdate(matchedAdmin, prevAdminURL, savedMeta) ||
		isRetiredManagedAdminEndpoint(matchedAdmin, prevAdminURL, savedMeta)) {
		return matchedAdmin, nil, nil
	}

	authSpec := matchedAdmin.Spec.Auth
	if hasSnap && sameAdminIdentity(snap.adminName, snap.adminUID, matchedAdmin) {
		// If SecretRefs are still set on matchedAdmin and match, use live SecretRefs; if inline credentials were
		// modified simultaneously with ExternalURL, prefer the snapshot captured for prevAdminURL.
		authSpec = snap.auth
	} else {
		var effectiveMeta *syncedAdminEndpointMeta
		adminMeta := lookupAdminRecordedEndpointMeta(matchedAdmin, prevAdminURL)
		if adminMeta != nil && adminMeta.AuthCaptured {
			effectiveMeta = adminMeta
		} else if savedMeta != nil && (savedMeta.AdminURL == "" || strings.TrimRight(strings.TrimSpace(savedMeta.AdminURL), "/") == prevAdminURL) {
			effectiveMeta = savedMeta
		} else if adminMeta != nil {
			effectiveMeta = adminMeta
		}
		if effectiveMeta != nil && (effectiveMeta.AdminURL == "" || strings.TrimRight(strings.TrimSpace(effectiveMeta.AdminURL), "/") == prevAdminURL) {
			if effectiveMeta.AuthCaptured {
				authSpec.Username = effectiveMeta.Username
				authSpec.PasswordSecretRef = effectiveMeta.PasswordSecretRef
				authSpec.AccessTokenSecretRef = effectiveMeta.AccessTokenSecretRef
				if effectiveMeta.UsedDefaultPassword {
					authSpec.Password = ""
				}
				if effectiveMeta.UsedDefaultAccessToken {
					authSpec.AccessToken = ""
				}
			} else {
				if effectiveMeta.Username != "" {
					authSpec.Username = effectiveMeta.Username
				}
				if effectiveMeta.PasswordSecretRef != nil {
					authSpec.PasswordSecretRef = effectiveMeta.PasswordSecretRef
				}
				if effectiveMeta.AccessTokenSecretRef != nil {
					authSpec.AccessTokenSecretRef = effectiveMeta.AccessTokenSecretRef
				}
			}
		}
	}

	username := authSpec.Username
	if username == "" {
		username = "admin"
	}
	password, err := resolveSecretOrValue(ctx, secretReader, namespace, authSpec.PasswordSecretRef, authSpec.Password, "123456")
	if err != nil {
		return matchedAdmin, nil, fmt.Errorf("resolve previous admin password: %w", err)
	}
	accessToken, prevAccessToken, err := resolveSecretOrValueWithPrevious(ctx, secretReader, namespace, authSpec.AccessTokenSecretRef, authSpec.AccessToken, "default_token")
	if err != nil {
		return matchedAdmin, nil, fmt.Errorf("resolve previous admin accessToken: %w", err)
	}
	accessToken = sanitizeTokenValue(accessToken)
	if accessToken == "" {
		if authSpec.AccessTokenSecretRef != nil && authSpec.AccessTokenSecretRef.Name != "" {
			invalidateSecretCacheOnly(secretReader, namespace, authSpec.AccessTokenSecretRef)
			return matchedAdmin, nil, fmt.Errorf("resolve previous admin accessToken: key %q in Secret %s/%s must not be empty or whitespace-only",
				authSpec.AccessTokenSecretRef.Key, namespace, authSpec.AccessTokenSecretRef.Name)
		}
		accessToken = "default_token"
	}
	prevAccessToken = sanitizeTokenValue(prevAccessToken)

	if factory == nil {
		factory = getOrCreateCachedClient
	}
	cli, err := factory(xxlclient.Config{
		BaseURL:             prevAdminURL,
		Username:            username,
		Password:            password,
		AccessToken:         accessToken,
		PreviousAccessToken: prevAccessToken,
	})
	if err != nil {
		return matchedAdmin, nil, err
	}
	return matchedAdmin, cli, nil
}

// ResolveAdminExplicitAccessToken resolves the configured accessToken on XxlJobAdmin without falling back
// to "default_token", used to enforce tenant isolation in K8sJobProxy mode (fixes R4-P0-2).
func ResolveAdminExplicitAccessToken(
	ctx context.Context,
	secretReader client.Reader,
	admin *xxljobv1alpha1.XxlJobAdmin,
) (string, error) {
	token, _, err := ResolveAdminExplicitAccessTokens(ctx, secretReader, admin)
	return token, err
}

// ResolveAdminExplicitAccessTokens resolves both the current and (during Secret rotation grace period)
// previous explicit non-default accessToken on XxlJobAdmin (fixes R11-P2-1).
func ResolveAdminExplicitAccessTokens(
	ctx context.Context,
	secretReader client.Reader,
	admin *xxljobv1alpha1.XxlJobAdmin,
) (string, string, error) {
	token, prevToken, _, _, err := resolveAdminExplicitAccessTokensWithFreshness(ctx, secretReader, admin)
	return token, prevToken, err
}

func resolveAdminExplicitAccessTokensWithFreshness(
	ctx context.Context,
	secretReader client.Reader,
	admin *xxljobv1alpha1.XxlJobAdmin,
) (string, string, time.Time, bool, error) {
	if admin == nil {
		return "", "", time.Time{}, false, fmt.Errorf("admin CR is nil")
	}
	token, prevToken, prevValidUntil, secretFresh, err := resolveSecretOrValueWithPreviousAndFreshness(ctx, secretReader, admin.Namespace, admin.Spec.Auth.AccessTokenSecretRef, admin.Spec.Auth.AccessToken, "")
	if err != nil {
		return "", "", time.Time{}, secretFresh, err
	}
	token = sanitizeTokenValue(token)
	prevToken = sanitizeTokenValue(prevToken)
	if prevToken == "default_token" {
		prevToken = ""
		prevValidUntil = time.Time{}
	}
	if token == "" || token == "default_token" {
		return "", "", time.Time{}, secretFresh, fmt.Errorf("XxlJobAdmin %s/%s must configure an explicit non-default accessToken or accessTokenSecretRef for K8sJobProxy tenant isolation", admin.Namespace, admin.Name)
	}
	return token, prevToken, prevValidUntil, secretFresh, nil
}

// ResolveExecutorAccessToken resolves the expected XXL-JOB-ACCESS-TOKEN for a given (namespace, executorName),
// caching the result for 30s to avoid hitting APIServer on every /beat or /log poll and rejecting "default_token" (fixes R4-P0-2 & P2).
func ResolveExecutorAccessToken(
	ctx context.Context,
	k8sClient client.Client,
	secretReader client.Reader,
	namespace, executorName string,
) (string, error) {
	entry, _, err := resolveExecutorAccessTokenEntry(ctx, k8sClient, secretReader, namespace, executorName, false)
	if err != nil {
		return "", err
	}
	return entry.token, nil
}

// ValidateExecutorAccessToken verifies providedToken against the executor's configured XXL-JOB-ACCESS-TOKEN.
// When a Secret rotation is in progress, it also accepts the immediately preceding token within a 10-minute
// rotation grace window so rolling Admin/Sidecar pods do not experience authentication brownouts (fixes R6-P1-9, R8-P1-6).
func ValidateExecutorAccessToken(
	ctx context.Context,
	k8sClient client.Client,
	secretReader client.Reader,
	namespace, executorName, providedToken string,
) error {
	trimmed := sanitizeTokenValue(providedToken)
	if trimmed == "" || trimmed == "default_token" {
		return fmt.Errorf("missing or default XXL-JOB-ACCESS-TOKEN for executor %s/%s", namespace, executorName)
	}
	entry, fetchedFresh, err := resolveExecutorAccessTokenEntry(ctx, k8sClient, secretReader, namespace, executorName, false)
	if err != nil {
		return err
	}
	if matchExecutorTokenEntry(entry, trimmed, time.Now()) {
		return nil
	}
	// If the non-forced call was already a cache miss that just read the latest Secret from APIServer,
	// do not immediately query APIServer a second time (fixes R8-P1-6).
	if fetchedFresh {
		return fmt.Errorf("invalid XXL-JOB-ACCESS-TOKEN for executor %s/%s", namespace, executorName)
	}
	// On cached mismatch, force a fresh Secret read once (rate-limited by forceRefreshCooldown) in case the Secret was just rotated.
	freshEntry, _, err := resolveExecutorAccessTokenEntry(ctx, k8sClient, secretReader, namespace, executorName, true)
	if err != nil {
		return err
	}
	if matchExecutorTokenEntry(freshEntry, trimmed, time.Now()) {
		return nil
	}
	return fmt.Errorf("invalid XXL-JOB-ACCESS-TOKEN for executor %s/%s", namespace, executorName)
}

func matchExecutorTokenEntry(entry cachedTokenEntry, provided string, now time.Time) bool {
	if provided == "" || provided == "default_token" {
		return false
	}
	if subtle.ConstantTimeCompare([]byte(provided), []byte(entry.token)) == 1 {
		return true
	}
	if entry.prevToken != "" && now.Before(entry.prevValidUntil) {
		if subtle.ConstantTimeCompare([]byte(provided), []byte(entry.prevToken)) == 1 {
			return true
		}
	}
	return false
}

func resolveExecutorAccessTokenEntry(
	ctx context.Context,
	k8sClient client.Client,
	secretReader client.Reader,
	namespace, executorName string,
	forceRefresh bool,
) (cachedTokenEntry, bool, error) {
	if secretReader == nil {
		secretReader = k8sClient
	}
	readerID := readerCacheID(secretReader)
	cacheKey := fmt.Sprintf("%s|%s/%s", readerID, namespace, executorName)

	// Always read XxlJobExecutor and XxlJobAdmin from the in-memory informer cache (k8sClient) BEFORE
	// returning any tokenCache hit so terminating executors, non-K8sJobProxy modes, AdminRef switches,
	// and same-name Admin UID recreations take effect immediately across all replicas (fixes EG2-05).
	var execCR xxljobv1alpha1.XxlJobExecutor
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: executorName}, &execCR); err != nil {
		if apierrors.IsNotFound(err) {
			tokenCacheMu.Lock()
			delete(tokenCache, cacheKey)
			tokenCacheMu.Unlock()
		}
		return cachedTokenEntry{}, false, err
	}
	if !execCR.DeletionTimestamp.IsZero() {
		tokenCacheMu.Lock()
		delete(tokenCache, cacheKey)
		tokenCacheMu.Unlock()
		return cachedTokenEntry{}, false, fmt.Errorf("XxlJobExecutor %s/%s is terminating", namespace, executorName)
	}
	if execCR.Spec.Mode != xxljobv1alpha1.ExecutorModeK8sJobProxy {
		tokenCacheMu.Lock()
		delete(tokenCache, cacheKey)
		tokenCacheMu.Unlock()
		return cachedTokenEntry{}, false, fmt.Errorf("XxlJobExecutor %s/%s mode %q is not eligible for K8sJobProxy endpoints", namespace, executorName, execCR.Spec.Mode)
	}
	adminNS := execCR.Spec.AdminRef.Namespace
	if adminNS == "" {
		adminNS = execCR.Namespace
	}
	if adminNS != execCR.Namespace {
		tokenCacheMu.Lock()
		delete(tokenCache, cacheKey)
		tokenCacheMu.Unlock()
		return cachedTokenEntry{}, false, fmt.Errorf("cross-namespace adminRef (%s/%s) is not allowed", adminNS, execCR.Spec.AdminRef.Name)
	}
	var admin xxljobv1alpha1.XxlJobAdmin
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: adminNS, Name: execCR.Spec.AdminRef.Name}, &admin); err != nil {
		if apierrors.IsNotFound(err) {
			tokenCacheMu.Lock()
			delete(tokenCache, cacheKey)
			tokenCacheMu.Unlock()
		}
		return cachedTokenEntry{}, false, err
	}

	currentSecretRef := adminAccessTokenSecretRefKey(&admin)
	now := time.Now()
	tokenCacheMu.Lock()
	existing, hasExisting := tokenCache[cacheKey]
	alignedWithAdmin := hasExisting && sameAdminIdentity(existing.adminName, existing.adminUID, &admin) && existing.secretRef == currentSecretRef
	if alignedWithAdmin && admin.Spec.Auth.AccessTokenSecretRef == nil {
		inlineTok := sanitizeTokenValue(admin.Spec.Auth.AccessToken)
		if inlineTok == "" || inlineTok == "default_token" {
			alignedWithAdmin = existing.errMsg != ""
		} else {
			alignedWithAdmin = existing.errMsg == "" && existing.token == inlineTok
		}
	}
	if alignedWithAdmin {
		if !forceRefresh && now.Before(existing.expiresAt) {
			tokenCacheMu.Unlock()
			if existing.errMsg != "" {
				return cachedTokenEntry{}, false, fmt.Errorf("%s", existing.errMsg)
			}
			return existing, false, nil
		}
		// Rate-limit forceRefresh to prevent unauthenticated bad-token requests from amplifying into APIServer DoS (fixes R7-P2-9, R8-P1-6).
		if forceRefresh && now.Before(existing.expiresAt) && !existing.lastForcedAt.IsZero() && now.Sub(existing.lastForcedAt) < forceRefreshCooldown {
			tokenCacheMu.Unlock()
			if existing.errMsg != "" {
				return cachedTokenEntry{}, false, fmt.Errorf("%s", existing.errMsg)
			}
			return existing, false, nil
		}
		if forceRefresh {
			existing.lastForcedAt = now
			tokenCache[cacheKey] = existing
		}
	}
	tokenCacheMu.Unlock()

	if forceRefresh && admin.Spec.Auth.AccessTokenSecretRef != nil {
		invalidateSecretCacheOnly(secretReader, admin.Namespace, admin.Spec.Auth.AccessTokenSecretRef)
	}
	token, secretPrevToken, secretPrevValidUntil, secretFresh, err := resolveAdminExplicitAccessTokensWithFreshness(ctx, secretReader, &admin)
	if err != nil {
		tokenCacheMu.Lock()
		prev := tokenCache[cacheKey]
		pruneTokenCacheMapLocked(tokenCache, now)
		sameAdmin := sameAdminIdentity(prev.adminName, prev.adminUID, &admin)
		var prevTok, prevPrevTok string
		var prevValid time.Time
		if sameAdmin {
			prevTok = prev.token
			prevPrevTok = prev.prevToken
			prevValid = prev.prevValidUntil
		}
		lastForced := prev.lastForcedAt
		if forceRefresh {
			lastForced = now
		}
		tokenCache[cacheKey] = cachedTokenEntry{
			adminName:      admin.Name,
			adminUID:       admin.UID,
			secretRef:      currentSecretRef,
			token:          prevTok,
			expiresAt:      now.Add(forceRefreshCooldown),
			prevToken:      prevPrevTok,
			prevValidUntil: prevValid,
			lastForcedAt:   lastForced,
			errMsg:         err.Error(),
		}
		tokenCacheMu.Unlock()
		return cachedTokenEntry{}, secretFresh, err
	}

	tokenCacheMu.Lock()
	prev, hadPrev := tokenCache[cacheKey]
	pruneTokenCacheMapLocked(tokenCache, now)
	lastForced := prev.lastForcedAt
	if forceRefresh {
		lastForced = now
	}
	sameAdmin := !hadPrev || sameAdminIdentity(prev.adminName, prev.adminUID, &admin)
	var prevTok string
	var prevValid time.Time
	if sameAdmin {
		prevTok = prev.prevToken
		prevValid = prev.prevValidUntil
	}
	tokenRotated := hadPrev && sameAdmin && prev.token != "" && prev.token != token
	if tokenRotated || (hadPrev && !sameAdmin) {
		lastForced = time.Time{}
	}
	updated := cachedTokenEntry{
		adminName:      admin.Name,
		adminUID:       admin.UID,
		secretRef:      currentSecretRef,
		token:          token,
		expiresAt:      now.Add(tokenCacheTTL),
		prevToken:      prevTok,
		prevValidUntil: prevValid,
		lastForcedAt:   lastForced,
	}
	if tokenRotated {
		updated.prevToken = prev.token
		updated.prevValidUntil = now.Add(tokenRotationGracePeriod)
	} else if secretPrevToken != "" && secretPrevToken != token && (updated.prevToken != secretPrevToken || !now.Before(updated.prevValidUntil)) {
		updated.prevToken = secretPrevToken
		if !secretPrevValidUntil.IsZero() {
			updated.prevValidUntil = secretPrevValidUntil
		} else {
			updated.prevValidUntil = now.Add(tokenRotationGracePeriod)
		}
	}
	tokenCache[cacheKey] = updated
	if tokenRotated {
		// When a real token rotation is observed for admin.Name, expire sibling executor tokenCache entries
		// in the same namespace referencing the same Admin without zeroing their lastForcedAt cooldown.
		prefix := fmt.Sprintf("%s|%s/", readerID, admin.Namespace)
		for k, sib := range tokenCache {
			if k != cacheKey && strings.HasPrefix(k, prefix) && sameAdminIdentity(sib.adminName, sib.adminUID, &admin) {
				sib.expiresAt = time.Time{}
				sib.errMsg = ""
				tokenCache[k] = sib
			}
		}
	}
	tokenCacheMu.Unlock()
	return updated, secretFresh, nil
}

func pruneTokenCacheMapLocked(m map[string]cachedTokenEntry, now time.Time) {
	if len(m) < maxTokenCacheEntries {
		return
	}
	for k, v := range m {
		if now.After(v.expiresAt) && (v.prevToken == "" || !now.Before(v.prevValidUntil)) {
			delete(m, k)
		}
	}
	for len(m) >= maxTokenCacheEntries {
		// Tier 1: Evict negative error entries that hold no valid token and no active rotation grace window.
		var victimKey string
		var victimExp time.Time
		for k, v := range m {
			hasActiveGrace := v.prevToken != "" && now.Before(v.prevValidUntil)
			if v.errMsg != "" && v.token == "" && !hasActiveGrace {
				if victimKey == "" || v.expiresAt.Before(victimExp) {
					victimKey = k
					victimExp = v.expiresAt
				}
			}
		}
		// Tier 2: Evict entries without an active 10-minute rotation grace window (oldest expiresAt first).
		if victimKey == "" {
			for k, v := range m {
				hasActiveGrace := v.prevToken != "" && now.Before(v.prevValidUntil)
				if !hasActiveGrace {
					if victimKey == "" || v.expiresAt.Before(victimExp) {
						victimKey = k
						victimExp = v.expiresAt
					}
				}
			}
		}
		// Tier 3: All remaining entries hold active rotation grace windows; evict earliest prevValidUntil.
		if victimKey == "" {
			var oldestGrace time.Time
			for k, v := range m {
				if victimKey == "" || v.prevValidUntil.Before(oldestGrace) ||
					(v.prevValidUntil.Equal(oldestGrace) && v.expiresAt.Before(victimExp)) {
					victimKey = k
					oldestGrace = v.prevValidUntil
					victimExp = v.expiresAt
				}
			}
		}
		if victimKey == "" {
			break
		}
		delete(m, victimKey)
	}
}

func invalidateSecretCacheOnly(reader client.Reader, namespace string, ref *xxljobv1alpha1.SecretKeyReference) {
	if ref == nil || ref.Name == "" || ref.Key == "" {
		return
	}
	cacheKey := fmt.Sprintf("%s|%s/%s/%s", readerCacheID(reader), namespace, ref.Name, ref.Key)
	secretCacheMu.Lock()
	if prev, ok := secretCache[cacheKey]; ok {
		prev.expiresAt = time.Time{}
		prev.errMsg = ""
		secretCache[cacheKey] = prev
	}
	secretCacheMu.Unlock()
}

func invalidateSecretCache(reader client.Reader, namespace string, ref *xxljobv1alpha1.SecretKeyReference) {
	if ref == nil || ref.Name == "" || ref.Key == "" {
		return
	}
	invalidateSecretCacheOnly(reader, namespace, ref)

	prefix := fmt.Sprintf("%s|%s/", readerCacheID(reader), namespace)
	tokenCacheMu.Lock()
	for k, prev := range tokenCache {
		if strings.HasPrefix(k, prefix) {
			prev.expiresAt = time.Time{}
			prev.lastForcedAt = time.Time{}
			prev.errMsg = ""
			tokenCache[k] = prev
		}
	}
	tokenCacheMu.Unlock()
}

func purgeExecutorTokenCache(namespace, executorName string) {
	suffix := fmt.Sprintf("|%s/%s", namespace, executorName)
	tokenCacheMu.Lock()
	for k := range tokenCache {
		if strings.HasSuffix(k, suffix) {
			delete(tokenCache, k)
		}
	}
	tokenCacheMu.Unlock()
}

func evictClientCacheByAdminURL(rawURL string) {
	adminURL := strings.TrimRight(strings.TrimSpace(rawURL), "/")
	if adminURL == "" {
		return
	}
	prefix := adminURL + "|"
	clientCacheMu.Lock()
	var keysToRemove []string
	for adminID, key := range clientCacheByAdmin {
		if strings.HasPrefix(adminID, prefix) {
			keysToRemove = append(keysToRemove, key)
		}
	}
	for _, k := range keysToRemove {
		removeClientCacheKeyLocked(k)
	}
	clientCacheMu.Unlock()
}

func purgeAdminCaches(ctx context.Context, k8sClient client.Client, reader client.Reader, admin *xxljobv1alpha1.XxlJobAdmin) {
	if admin == nil {
		return
	}
	sharedURLs := make(map[string]bool)
	sharedSecrets := make(map[string]bool)
	if k8sClient != nil {
		var adminList xxljobv1alpha1.XxlJobAdminList
		if err := k8sClient.List(ctx, &adminList, client.InNamespace(admin.Namespace)); err == nil {
			for i := range adminList.Items {
				other := &adminList.Items[i]
				if other.Name == admin.Name || !other.DeletionTimestamp.IsZero() {
					continue
				}
				if ou := strings.TrimRight(ResolveAdminURL(other), "/"); ou != "" {
					sharedURLs[ou] = true
				}
				if osu := strings.TrimRight(strings.TrimSpace(other.Status.AdminURL), "/"); osu != "" {
					sharedURLs[osu] = true
				}
				for _, oRef := range []*xxljobv1alpha1.SecretKeyReference{
					other.Spec.Datasource.PasswordSecretRef,
					other.Spec.Auth.PasswordSecretRef,
					other.Spec.Auth.AccessTokenSecretRef,
				} {
					if oRef != nil && oRef.Name != "" && oRef.Key != "" {
						sharedSecrets[oRef.Name+"/"+oRef.Key] = true
					}
				}
			}
		}
	}

	if u := strings.TrimRight(ResolveAdminURL(admin), "/"); u != "" && !sharedURLs[u] {
		evictClientCacheByAdminURL(u)
	}
	if su := strings.TrimRight(strings.TrimSpace(admin.Status.AdminURL), "/"); su != "" && !sharedURLs[su] {
		evictClientCacheByAdminURL(su)
	}
	for _, pu := range extractPreviousAdminURLs(admin) {
		if pu != "" && !sharedURLs[pu] {
			evictClientCacheByAdminURL(pu)
		}
	}

	endpointAuthMu.Lock()
	nsEndpointPrefix := admin.Namespace + "|"
	var nextEndpointOrder []string
	for _, k := range endpointAuthOrder {
		snap, exists := endpointAuthCache[k]
		if !exists {
			continue
		}
		urlPart := strings.TrimPrefix(k, nsEndpointPrefix)
		if strings.HasPrefix(k, nsEndpointPrefix) && snap.adminName == admin.Name && !sharedURLs[urlPart] {
			delete(endpointAuthCache, k)
			continue
		}
		nextEndpointOrder = append(nextEndpointOrder, k)
	}
	endpointAuthOrder = nextEndpointOrder
	endpointAuthMu.Unlock()

	refs := []*xxljobv1alpha1.SecretKeyReference{
		admin.Spec.Datasource.PasswordSecretRef,
		admin.Spec.Auth.PasswordSecretRef,
		admin.Spec.Auth.AccessTokenSecretRef,
	}
	secretCacheMu.Lock()
	for _, ref := range refs {
		if ref == nil || ref.Name == "" || ref.Key == "" {
			continue
		}
		if sharedSecrets[ref.Name+"/"+ref.Key] {
			continue
		}
		suffix := fmt.Sprintf("|%s/%s/%s", admin.Namespace, ref.Name, ref.Key)
		for k := range secretCache {
			if strings.HasSuffix(k, suffix) {
				delete(secretCache, k)
			}
		}
	}
	secretCacheMu.Unlock()

	now := time.Now()
	nsTokenInfix := fmt.Sprintf("|%s/", admin.Namespace)
	tokenCacheMu.Lock()
	for k, entry := range tokenCache {
		if !strings.Contains(k, nsTokenInfix) {
			continue
		}
		if entry.adminName == admin.Name {
			delete(tokenCache, k)
		} else if entry.adminName == "" && now.After(entry.expiresAt) && (entry.prevToken == "" || !now.Before(entry.prevValidUntil)) {
			delete(tokenCache, k)
		}
	}
	tokenCacheMu.Unlock()
}

func deletionTimestampChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			return e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(event.GenericEvent) bool {
			return false
		},
	}
}

// ResolveAdminURL computes the cluster-internal or external URL of an XxlJobAdmin instance purely from Spec
// so updates to Spec.Port, Spec.ContextPath, or Spec.ExternalURL take effect immediately without being shadowed
// by a stale Status.AdminURL (fixes R6-P1-8).
func ResolveAdminURL(admin *xxljobv1alpha1.XxlJobAdmin) string {
	if admin == nil {
		return ""
	}
	if strings.TrimSpace(admin.Spec.ExternalURL) != "" {
		return strings.TrimRight(strings.TrimSpace(admin.Spec.ExternalURL), "/")
	}
	port := admin.Spec.Port
	if port <= 0 {
		port = 8080
	}
	ctxPath := strings.TrimSpace(admin.Spec.ContextPath)
	if ctxPath == "" {
		ctxPath = "/xxl-job-admin"
	}
	if !strings.HasPrefix(ctxPath, "/") {
		ctxPath = "/" + ctxPath
	}
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d%s", admin.Name, admin.Namespace, port, strings.TrimRight(ctxPath, "/"))
}

func sanitizeTokenValue(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if !strings.ContainsAny(trimmed, "\r\n") {
		return trimmed
	}
	return strings.NewReplacer("\r", "", "\n", "").Replace(trimmed)
}

func isPureExternalAdmin(admin *xxljobv1alpha1.XxlJobAdmin) bool {
	if admin == nil {
		return false
	}
	return strings.TrimSpace(admin.Spec.ExternalURL) != "" && strings.TrimSpace(admin.Spec.Image) == ""
}

// isSameManagedAdminDBUpdate reports whether prevAdminURL belonged to the same live, managed in-cluster
// XxlJobAdmin CR backed by the same Datasource.URL (including in-place Spec.Port / Spec.ContextPath edits
// on a cluster-local Service URL as well as Hybrid mode ExternalURL + Image updates over the same Deployment/DB).
func isSameManagedAdminDBUpdate(
	admin *xxljobv1alpha1.XxlJobAdmin,
	prevAdminURL string,
	savedMeta *syncedAdminEndpointMeta,
) bool {
	prevAdminURL = strings.TrimRight(strings.TrimSpace(prevAdminURL), "/")
	if admin == nil || !admin.DeletionTimestamp.IsZero() || prevAdminURL == "" {
		return false
	}
	if isPureExternalAdmin(admin) {
		return false
	}
	currentDBHash := proxyexecutor.DatasourceURLHash(admin.Spec.Datasource.URL)
	if currentDBHash == "" {
		return false
	}
	if savedMeta != nil {
		if sName := strings.TrimSpace(savedMeta.AdminName); sName != "" && sName != admin.Name {
			return false
		}
		if sUID := strings.TrimSpace(savedMeta.AdminUID); sUID != "" && admin.UID != "" && sUID != string(admin.UID) {
			return false
		}
	}
	isClusterLocal := proxyexecutor.IsClusterLocalAdminServiceURL(prevAdminURL, admin.Name, admin.Namespace)
	if savedMeta != nil && (savedMeta.AdminURL == "" || strings.TrimRight(strings.TrimSpace(savedMeta.AdminURL), "/") == prevAdminURL) {
		if savedMeta.DatasourceHash != "" {
			return savedMeta.DatasourceHash == currentDBHash
		}
		if savedMeta.AuthCaptured && !savedMeta.ManagedInCluster {
			return false
		}
	}
	if adminMeta := lookupAdminRecordedEndpointMeta(admin, prevAdminURL); adminMeta != nil {
		if sUID := strings.TrimSpace(adminMeta.AdminUID); sUID != "" && admin.UID != "" && sUID != string(admin.UID) {
			return false
		}
		if adminMeta.DatasourceHash != "" {
			return adminMeta.DatasourceHash == currentDBHash
		}
		if adminMeta.AuthCaptured && !adminMeta.ManagedInCluster {
			return false
		}
	}
	return isClusterLocal
}

// isAdminInstanceOrDBReplaced reports whether a dependent resource whose ConditionAdminEndpointSynced recorded
// savedMeta is now facing a recreated Admin CR (different UID) or a managed Admin whose Datasource.URL changed
// in place (even when ResolveAdminURL did not change).
func isAdminInstanceOrDBReplaced(admin *xxljobv1alpha1.XxlJobAdmin, savedMeta *syncedAdminEndpointMeta) bool {
	if admin == nil || savedMeta == nil {
		return false
	}
	if sName := strings.TrimSpace(savedMeta.AdminName); sName != "" && sName != admin.Name {
		return false
	}
	if sUID := strings.TrimSpace(savedMeta.AdminUID); sUID != "" && admin.UID != "" && sUID != string(admin.UID) {
		return true
	}
	if !isPureExternalAdmin(admin) && strings.TrimSpace(savedMeta.DatasourceHash) != "" {
		currentDBHash := proxyexecutor.DatasourceURLHash(admin.Spec.Datasource.URL)
		if currentDBHash != "" && savedMeta.DatasourceHash != currentDBHash {
			return true
		}
	}
	return false
}

// isRetiredManagedAdminEndpoint reports whether prevAdminURL was served by matchedAdmin's owned in-cluster
// Deployment/Service (either via <name>.<ns>.svc.cluster.local or Hybrid ExternalURL + Image) and that
// in-cluster endpoint has since been retired or overwritten (e.g., switching to pure ExternalURL mode where
// cleanupOwnedInClusterResources deletes the Service/Deployment, or changing URL alongside Datasource.URL).
func isRetiredManagedAdminEndpoint(
	matchedAdmin *xxljobv1alpha1.XxlJobAdmin,
	prevAdminURL string,
	savedMeta ...*syncedAdminEndpointMeta,
) bool {
	prevAdminURL = strings.TrimRight(strings.TrimSpace(prevAdminURL), "/")
	if matchedAdmin == nil || prevAdminURL == "" {
		return false
	}
	wasManagedInCluster := proxyexecutor.IsClusterLocalAdminServiceURL(prevAdminURL, matchedAdmin.Name, matchedAdmin.Namespace)
	if !wasManagedInCluster && len(savedMeta) > 0 && savedMeta[0] != nil {
		sm := savedMeta[0]
		if (sm.AdminURL == "" || strings.TrimRight(strings.TrimSpace(sm.AdminURL), "/") == prevAdminURL) &&
			sameAdminIdentity(sm.AdminName, types.UID(sm.AdminUID), matchedAdmin) &&
			(sm.ManagedInCluster || sm.DatasourceHash != "") {
			wasManagedInCluster = true
		}
	}
	if !wasManagedInCluster {
		if adminMeta := lookupAdminRecordedEndpointMeta(matchedAdmin, prevAdminURL); adminMeta != nil &&
			(adminMeta.ManagedInCluster || adminMeta.DatasourceHash != "") {
			wasManagedInCluster = true
		}
	}
	if !wasManagedInCluster {
		return false
	}
	if isPureExternalAdmin(matchedAdmin) {
		return true
	}
	currentURL := strings.TrimRight(ResolveAdminURL(matchedAdmin), "/")
	return currentURL != prevAdminURL
}

// isMigratingFromPreviousAdmin reports whether a resource whose Status.AdminURL was recorded against prevAdminURL
// is now targeting a different XxlJobAdmin in namespace, even when the new target XxlJobAdmin is not yet created,
// missing its Secret, or not yet ready.
func isMigratingFromPreviousAdmin(
	ctx context.Context,
	reader client.Reader,
	namespace, targetAdminName, prevAdminURL string,
	savedMeta ...*syncedAdminEndpointMeta,
) (bool, error) {
	prevAdminURL = strings.TrimRight(strings.TrimSpace(prevAdminURL), "/")
	targetAdminName = strings.TrimSpace(targetAdminName)
	if prevAdminURL == "" || targetAdminName == "" {
		return false, nil
	}
	var metaArg *syncedAdminEndpointMeta
	if len(savedMeta) > 0 {
		metaArg = savedMeta[0]
	}
	var targetAdmin xxljobv1alpha1.XxlJobAdmin
	err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: targetAdminName}, &targetAdmin)
	if err == nil {
		targetURL := strings.TrimRight(ResolveAdminURL(&targetAdmin), "/")
		if targetURL == prevAdminURL {
			return false, nil
		}
		if isSameManagedAdminDBUpdate(&targetAdmin, prevAdminURL, metaArg) {
			return false, nil
		}
		targetStatusURL := strings.TrimRight(strings.TrimSpace(targetAdmin.Status.AdminURL), "/")
		if targetStatusURL == prevAdminURL {
			if strings.TrimSpace(targetAdmin.Spec.ExternalURL) != "" {
				if validateExternalAdminURL(targetAdmin.Spec.ExternalURL) != nil {
					return false, nil
				}
			} else if _, pathErr := normalizeAndValidateContextPath(targetAdmin.Spec.ContextPath); pathErr != nil {
				return false, nil
			}
		}
		return true, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, err
	}
	var adminList xxljobv1alpha1.XxlJobAdminList
	if listErr := reader.List(ctx, &adminList, client.InNamespace(namespace)); listErr != nil {
		return false, listErr
	}
	for i := range adminList.Items {
		item := &adminList.Items[i]
		if item.Name == targetAdminName {
			continue
		}
		if strings.TrimRight(ResolveAdminURL(item), "/") == prevAdminURL ||
			adminHasRecordedPreviousURL(item, prevAdminURL) {
			return true, nil
		}
	}
	return false, nil
}

// isPreviousAdminDeleting reports whether prevAdminURL belongs exclusively to a terminating XxlJobAdmin in namespace.
func isPreviousAdminDeleting(ctx context.Context, reader client.Reader, namespace, prevAdminURL string) (bool, error) {
	prevAdminURL = strings.TrimRight(strings.TrimSpace(prevAdminURL), "/")
	if prevAdminURL == "" {
		return false, nil
	}
	var adminList xxljobv1alpha1.XxlJobAdminList
	if err := reader.List(ctx, &adminList, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	hasDeletingMatch := false
	for i := range adminList.Items {
		item := &adminList.Items[i]
		matches := strings.TrimRight(ResolveAdminURL(item), "/") == prevAdminURL ||
			adminHasRecordedPreviousURL(item, prevAdminURL)
		if !matches {
			continue
		}
		if item.DeletionTimestamp.IsZero() {
			return false, nil
		}
		hasDeletingMatch = true
	}
	return hasDeletingMatch, nil
}

func resolveSecretOrValue(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	ref *xxljobv1alpha1.SecretKeyReference,
	plainValue string,
	defaultValue string,
) (string, error) {
	val, _, err := resolveSecretOrValueWithPrevious(ctx, reader, namespace, ref, plainValue, defaultValue)
	return val, err
}

func resolveSecretOrValueWithPrevious(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	ref *xxljobv1alpha1.SecretKeyReference,
	plainValue string,
	defaultValue string,
) (string, string, error) {
	val, prev, _, _, err := resolveSecretOrValueWithPreviousAndFreshness(ctx, reader, namespace, ref, plainValue, defaultValue)
	return val, prev, err
}

func resolveSecretOrValueWithPreviousAndFreshness(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	ref *xxljobv1alpha1.SecretKeyReference,
	plainValue string,
	defaultValue string,
) (string, string, time.Time, bool, error) {
	if ref != nil && ref.Name != "" && ref.Key != "" {
		cacheKey := fmt.Sprintf("%s|%s/%s/%s", readerCacheID(reader), namespace, ref.Name, ref.Key)
		now := time.Now()
		secretCacheMu.Lock()
		if entry, ok := secretCache[cacheKey]; ok && now.Before(entry.expiresAt) {
			if entry.errMsg != "" {
				secretCacheMu.Unlock()
				return "", "", time.Time{}, false, fmt.Errorf("%s", entry.errMsg)
			}
			prevTok := ""
			var prevValid time.Time
			if entry.prevToken != "" && now.Before(entry.prevValidUntil) {
				prevTok = entry.prevToken
				prevValid = entry.prevValidUntil
			}
			secretCacheMu.Unlock()
			return entry.token, prevTok, prevValid, false, nil
		}
		secretCacheMu.Unlock()

		var secret corev1.Secret
		if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &secret); err != nil {
			errMsg := fmt.Sprintf("get secret %s/%s: %v", namespace, ref.Name, err)
			secretCacheMu.Lock()
			prev := secretCache[cacheKey]
			pruneTokenCacheMapLocked(secretCache, now)
			secretCache[cacheKey] = cachedTokenEntry{
				token:          prev.token,
				expiresAt:      now.Add(forceRefreshCooldown),
				prevToken:      prev.prevToken,
				prevValidUntil: prev.prevValidUntil,
				errMsg:         errMsg,
			}
			secretCacheMu.Unlock()
			return "", "", time.Time{}, true, fmt.Errorf("get secret %s/%s: %w", namespace, ref.Name, err)
		}
		val, ok := secret.Data[ref.Key]
		if !ok {
			errMsg := fmt.Sprintf("key %q not found in secret %s/%s", ref.Key, namespace, ref.Name)
			secretCacheMu.Lock()
			prev := secretCache[cacheKey]
			pruneTokenCacheMapLocked(secretCache, now)
			secretCache[cacheKey] = cachedTokenEntry{
				token:          prev.token,
				expiresAt:      now.Add(forceRefreshCooldown),
				prevToken:      prev.prevToken,
				prevValidUntil: prev.prevValidUntil,
				errMsg:         errMsg,
			}
			secretCacheMu.Unlock()
			return "", "", time.Time{}, true, fmt.Errorf("%s", errMsg)
		}
		strVal := strings.TrimSpace(string(val))
		secretCacheMu.Lock()
		prev, hadPrev := secretCache[cacheKey]
		pruneTokenCacheMapLocked(secretCache, now)
		updated := cachedTokenEntry{
			token:          strVal,
			expiresAt:      now.Add(tokenCacheTTL),
			prevToken:      prev.prevToken,
			prevValidUntil: prev.prevValidUntil,
		}
		if hadPrev && prev.token != "" && prev.token != strVal {
			updated.prevToken = prev.token
			updated.prevValidUntil = now.Add(tokenRotationGracePeriod)
		}
		secretCache[cacheKey] = updated
		prevTok := ""
		var prevValid time.Time
		if updated.prevToken != "" && now.Before(updated.prevValidUntil) {
			prevTok = updated.prevToken
			prevValid = updated.prevValidUntil
		}
		secretCacheMu.Unlock()
		return strVal, prevTok, prevValid, true, nil
	}
	if strings.TrimSpace(plainValue) != "" {
		return strings.TrimSpace(plainValue), "", time.Time{}, true, nil
	}
	return defaultValue, "", time.Time{}, true, nil
}

// isAdminBusinessRejection returns true when XXL-JOB Admin was reachable and explicitly rejected
// the operation (HTTP 200 with ReturnT.Code != 200, e.g., JobGroup still has manual tasks or is the last group),
// as opposed to a network/transport/5xx unavailability error (fixes R5-P2-6 & R6-P1-5).
func isAdminBusinessRejection(err error) bool {
	return xxlclient.IsBusinessRejection(err)
}

func hasForceDeleteAnnotation(meta map[string]string) bool {
	return meta != nil && strings.EqualFold(strings.TrimSpace(meta[AnnotationForceDelete]), "true")
}

func hasAllowGlueTypeRecreateAnnotation(meta map[string]string) bool {
	return meta != nil && strings.EqualFold(strings.TrimSpace(meta[AnnotationAllowGlueTypeRecreate]), "true")
}

// shouldBypassDependencyCheck returns true ONLY when:
//  1. Annotation xxljob.io/force-delete=true is explicitly set on the CR, OR
//  2. The enclosing Kubernetes Namespace itself is being deleted (DeletionTimestamp != nil).
//
// Unlike shouldBypassRemoteCleanup, it does NOT return true after 10 minutes, ensuring that
// accidental out-of-order deletion (e.g., deleting XxlJobExecutor while XxlJobTasks still exist)
// never bypasses dependency protection or leaves orphan resources in Admin (fixes R4-P1-3).
func shouldBypassDependencyCheck(ctx context.Context, reader client.Reader, obj metav1.Object) bool {
	if hasForceDeleteAnnotation(obj.GetAnnotations()) {
		return true
	}
	if nsName := obj.GetNamespace(); nsName != "" && reader != nil {
		var ns corev1.Namespace
		if err := reader.Get(ctx, types.NamespacedName{Name: nsName}, &ns); err == nil {
			if !ns.DeletionTimestamp.IsZero() {
				return true
			}
		}
	}
	return false
}

// shouldBypassRemoteCleanup returns true when shouldBypassDependencyCheck is true OR when the CR
// has been stuck attempting remote Admin cleanup for >= MaxFinalizerRetryDuration (10 minutes).
// This is used ONLY to forgive remote Admin API failures when all child CRs have already been removed (fixes P1-2 & R4-P1-3).
func shouldBypassRemoteCleanup(ctx context.Context, reader client.Reader, obj metav1.Object) bool {
	if shouldBypassDependencyCheck(ctx, reader, obj) {
		return true
	}
	if dt := obj.GetDeletionTimestamp(); dt != nil && !dt.IsZero() {
		if time.Since(dt.Time) >= MaxFinalizerRetryDuration {
			return true
		}
	}
	return false
}

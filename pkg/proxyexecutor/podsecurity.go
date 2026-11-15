package proxyexecutor

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var baselineAllowedCapabilities = map[corev1.Capability]bool{
	"AUDIT_WRITE":      true,
	"CHOWN":            true,
	"DAC_OVERRIDE":     true,
	"FOWNER":           true,
	"FSETID":           true,
	"KILL":             true,
	"MKNOD":            true,
	"NET_BIND_SERVICE": true,
	"SETFCAP":          true,
	"SETGID":           true,
	"SETPCAP":          true,
	"SETUID":           true,
	"SYS_CHROOT":       true,
}

var safeSysctls = map[string]bool{
	"kernel.shm_rmid_forced":              true,
	"net.ipv4.ip_local_port_range":        true,
	"net.ipv4.ip_unprivileged_port_start": true,
	"net.ipv4.tcp_syncookies":             true,
	"net.ipv4.ping_group_range":           true,
}

// ApplyAndValidatePodSecurity enforces Pod Security Standards (privileged, baseline, restricted)
// and blocks mounting service-account-token Secrets on a K8sJobProxy PodSpec before the Operator
// creates the batch/v1.Job (fixes R6-P1-14 / N0).
func ApplyAndValidatePodSecurity(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	level string,
	podSpec *corev1.PodSpec,
	podAnnotations map[string]string,
) error {
	if podSpec == nil {
		return nil
	}

	// Default AutomountServiceAccountToken to false when nil (fixes R5-P2-10(b) / R6-P1-14).
	if podSpec.AutomountServiceAccountToken == nil {
		autoMount := false
		podSpec.AutomountServiceAccountToken = &autoMount
	}

	// Verify that no Secret volume or env SecretKeyRef mounts a kubernetes.io/service-account-token Secret,
	// which would otherwise bypass AllowedServiceAccounts (fixes R5-P2-10(a) / R6-P1-14).
	if err := validateNoServiceAccountTokenSecrets(ctx, reader, namespace, podSpec); err != nil {
		return err
	}

	normalizedLevel := strings.ToLower(strings.TrimSpace(level))
	if normalizedLevel == "" {
		normalizedLevel = "restricted"
	}

	switch normalizedLevel {
	case "privileged":
		return nil
	case "baseline":
		return validateBaselinePodSecurity(podSpec, podAnnotations)
	case "restricted":
		applyRestrictedDefaults(podSpec)
		if err := validateBaselinePodSecurity(podSpec, podAnnotations); err != nil {
			return err
		}
		return validateRestrictedPodSecurity(podSpec)
	default:
		return fmt.Errorf("unsupported podSecurityLevel %q (valid values: privileged, baseline, restricted)", level)
	}
}

func validateNoServiceAccountTokenSecrets(ctx context.Context, reader client.Reader, namespace string, podSpec *corev1.PodSpec) error {
	automountEnabled := podSpec.AutomountServiceAccountToken != nil && *podSpec.AutomountServiceAccountToken
	secretNames := make(map[string]bool)
	for _, vol := range podSpec.Volumes {
		if vol.Secret != nil {
			if strings.TrimSpace(vol.Secret.SecretName) == "" {
				return fmt.Errorf("volume %q references a Secret with an empty secretName", vol.Name)
			}
			secretNames[vol.Secret.SecretName] = true
		}
		if vol.Projected != nil {
			for _, src := range vol.Projected.Sources {
				if src.ServiceAccountToken != nil && !automountEnabled {
					return fmt.Errorf("projected serviceAccountToken in volume %q is forbidden when automountServiceAccountToken is false", vol.Name)
				}
				if src.Secret != nil {
					if strings.TrimSpace(src.Secret.Name) == "" {
						return fmt.Errorf("projected volume %q references a Secret with an empty name", vol.Name)
					}
					secretNames[src.Secret.Name] = true
				}
			}
		}
	}
	collectContainerSecrets := func(cName string, envs []corev1.EnvVar, envFroms []corev1.EnvFromSource) error {
		for _, env := range envs {
			if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
				if strings.TrimSpace(env.ValueFrom.SecretKeyRef.Name) == "" {
					return fmt.Errorf("container %q env %q references a SecretKeyRef with an empty name", cName, env.Name)
				}
				secretNames[env.ValueFrom.SecretKeyRef.Name] = true
			}
		}
		for _, ef := range envFroms {
			if ef.SecretRef != nil {
				if strings.TrimSpace(ef.SecretRef.Name) == "" {
					return fmt.Errorf("container %q envFrom references a SecretRef with an empty name", cName)
				}
				secretNames[ef.SecretRef.Name] = true
			}
		}
		return nil
	}
	for _, c := range podSpec.Containers {
		if err := collectContainerSecrets(c.Name, c.Env, c.EnvFrom); err != nil {
			return err
		}
	}
	for _, c := range podSpec.InitContainers {
		if err := collectContainerSecrets(c.Name, c.Env, c.EnvFrom); err != nil {
			return err
		}
	}
	for _, c := range podSpec.EphemeralContainers {
		if err := collectContainerSecrets(c.Name, c.Env, c.EnvFrom); err != nil {
			return err
		}
	}

	if len(secretNames) == 0 || reader == nil {
		return nil
	}
	for name := range secretNames {
		var sec corev1.Secret
		if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &sec); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("referenced Secret %s/%s not found (all Secrets referenced in k8sJobTemplate must exist and be validated)", namespace, name)
			}
			return fmt.Errorf("inspect referenced Secret %s/%s failed: %w", namespace, name, err)
		}
		if sec.Type == corev1.SecretTypeServiceAccountToken {
			return fmt.Errorf("referencing service-account-token Secret %s/%s in k8sJobTemplate is forbidden (use spec.k8sJobTemplate.serviceAccountName with spec.allowedServiceAccounts)", namespace, name)
		}
	}
	return nil
}

var baselineAllowedSELinuxTypes = map[string]bool{
	"":                 true,
	"container_t":      true,
	"container_init_t": true,
	"container_kvm_t":  true,
}

func normalizeCapabilityName(cap corev1.Capability) corev1.Capability {
	trimmed := strings.ToUpper(strings.TrimSpace(string(cap)))
	trimmed = strings.TrimPrefix(trimmed, "CAP_")
	return corev1.Capability(trimmed)
}

func validateBaselineSELinux(se *corev1.SELinuxOptions) error {
	if se == nil {
		return nil
	}
	if !baselineAllowedSELinuxTypes[se.Type] {
		return fmt.Errorf("podSecurityLevel baseline forbids SELinux type %q", se.Type)
	}
	if se.User != "" || se.Role != "" {
		return fmt.Errorf("podSecurityLevel baseline forbids custom SELinux user/role")
	}
	return nil
}

func validateBaselineContainer(cName string, ports []corev1.ContainerPort, sc *corev1.SecurityContext) error {
	for _, p := range ports {
		if p.HostPort != 0 {
			return fmt.Errorf("podSecurityLevel baseline forbids hostPort %d on container %q", p.HostPort, cName)
		}
	}
	if sc == nil {
		return nil
	}
	if sc.Privileged != nil && *sc.Privileged {
		return fmt.Errorf("podSecurityLevel baseline forbids privileged=true on container %q", cName)
	}
	if sc.WindowsOptions != nil && sc.WindowsOptions.HostProcess != nil && *sc.WindowsOptions.HostProcess {
		return fmt.Errorf("podSecurityLevel baseline forbids windowsOptions.hostProcess=true on container %q", cName)
	}
	if sc.ProcMount != nil && *sc.ProcMount != corev1.DefaultProcMount {
		return fmt.Errorf("podSecurityLevel baseline forbids non-default procMount on container %q", cName)
	}
	if sc.SeccompProfile != nil {
		if sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault &&
			sc.SeccompProfile.Type != corev1.SeccompProfileTypeLocalhost {
			return fmt.Errorf("podSecurityLevel baseline forbids seccompProfile.type=%s on container %q", sc.SeccompProfile.Type, cName)
		}
		if sc.SeccompProfile.Type == corev1.SeccompProfileTypeLocalhost &&
			(sc.SeccompProfile.LocalhostProfile == nil || strings.TrimSpace(*sc.SeccompProfile.LocalhostProfile) == "") {
			return fmt.Errorf("podSecurityLevel baseline requires non-empty seccompProfile.localhostProfile when type=Localhost on container %q", cName)
		}
	}
	if sc.AppArmorProfile != nil {
		if sc.AppArmorProfile.Type != corev1.AppArmorProfileTypeRuntimeDefault &&
			sc.AppArmorProfile.Type != corev1.AppArmorProfileTypeLocalhost {
			return fmt.Errorf("podSecurityLevel baseline forbids appArmorProfile.type=%s on container %q", sc.AppArmorProfile.Type, cName)
		}
		if sc.AppArmorProfile.Type == corev1.AppArmorProfileTypeLocalhost &&
			(sc.AppArmorProfile.LocalhostProfile == nil || strings.TrimSpace(*sc.AppArmorProfile.LocalhostProfile) == "") {
			return fmt.Errorf("podSecurityLevel baseline requires non-empty appArmorProfile.localhostProfile when type=Localhost on container %q", cName)
		}
	}
	if err := validateBaselineSELinux(sc.SELinuxOptions); err != nil {
		return err
	}
	if sc.Capabilities != nil {
		for _, capAdd := range sc.Capabilities.Add {
			if !baselineAllowedCapabilities[normalizeCapabilityName(capAdd)] {
				return fmt.Errorf("podSecurityLevel baseline forbids adding capability %q on container %q", capAdd, cName)
			}
		}
	}
	return nil
}

func validateBaselinePodSecurity(podSpec *corev1.PodSpec, podAnnotations map[string]string) error {
	if podSpec.HostNetwork || podSpec.HostPID || podSpec.HostIPC {
		return fmt.Errorf("podSecurityLevel baseline forbids hostNetwork, hostPID, and hostIPC")
	}
	for _, vol := range podSpec.Volumes {
		if vol.HostPath != nil {
			return fmt.Errorf("podSecurityLevel baseline forbids hostPath volume %q", vol.Name)
		}
		if vol.GitRepo != nil {
			return fmt.Errorf("podSecurityLevel baseline forbids gitRepo volume %q", vol.Name)
		}
	}
	for k, v := range podAnnotations {
		lowerK := strings.ToLower(strings.TrimSpace(k))
		trimmedV := strings.TrimSpace(v)
		lowerV := strings.ToLower(trimmedV)
		if strings.HasPrefix(lowerK, "container.apparmor.security.beta.kubernetes.io/") ||
			strings.HasPrefix(lowerK, "apparmor.security.beta.kubernetes.io/") {
			if lowerV != "runtime/default" &&
				(!strings.HasPrefix(lowerV, "localhost/") || strings.TrimSpace(trimmedV[len("localhost/"):]) == "") {
				return fmt.Errorf("podSecurityLevel baseline forbids appArmor annotation %s=%s", k, v)
			}
		}
		if strings.HasPrefix(lowerK, "seccomp.security.alpha.kubernetes.io/") ||
			strings.HasPrefix(lowerK, "container.seccomp.security.alpha.kubernetes.io/") {
			if lowerV != "runtime/default" && lowerV != "docker/default" &&
				(!strings.HasPrefix(lowerV, "localhost/") || strings.TrimSpace(trimmedV[len("localhost/"):]) == "") {
				return fmt.Errorf("podSecurityLevel baseline forbids seccomp annotation %s=%s", k, v)
			}
		}
		if lowerK == "security.alpha.kubernetes.io/unsafe-sysctls" && trimmedV != "" {
			return fmt.Errorf("podSecurityLevel baseline forbids unsafe-sysctls annotation %s=%s", k, v)
		}
		if lowerK == "security.alpha.kubernetes.io/sysctls" && trimmedV != "" {
			for _, entry := range strings.Split(trimmedV, ",") {
				entry = strings.TrimSpace(entry)
				if entry == "" {
					continue
				}
				sysctlName := strings.TrimSpace(strings.SplitN(entry, "=", 2)[0])
				if !safeSysctls[sysctlName] {
					return fmt.Errorf("podSecurityLevel baseline forbids unsafe sysctl %q in annotation %s", sysctlName, k)
				}
			}
		}
	}
	if podSpec.SecurityContext != nil {
		if podSpec.SecurityContext.WindowsOptions != nil &&
			podSpec.SecurityContext.WindowsOptions.HostProcess != nil &&
			*podSpec.SecurityContext.WindowsOptions.HostProcess {
			return fmt.Errorf("podSecurityLevel baseline forbids pod windowsOptions.hostProcess=true")
		}
		if podSpec.SecurityContext.SeccompProfile != nil {
			if podSpec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault &&
				podSpec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeLocalhost {
				return fmt.Errorf("podSecurityLevel baseline forbids pod seccompProfile.type=%s", podSpec.SecurityContext.SeccompProfile.Type)
			}
			if podSpec.SecurityContext.SeccompProfile.Type == corev1.SeccompProfileTypeLocalhost &&
				(podSpec.SecurityContext.SeccompProfile.LocalhostProfile == nil || strings.TrimSpace(*podSpec.SecurityContext.SeccompProfile.LocalhostProfile) == "") {
				return fmt.Errorf("podSecurityLevel baseline requires non-empty pod seccompProfile.localhostProfile when type=Localhost")
			}
		}
		if podSpec.SecurityContext.AppArmorProfile != nil {
			if podSpec.SecurityContext.AppArmorProfile.Type != corev1.AppArmorProfileTypeRuntimeDefault &&
				podSpec.SecurityContext.AppArmorProfile.Type != corev1.AppArmorProfileTypeLocalhost {
				return fmt.Errorf("podSecurityLevel baseline forbids pod appArmorProfile.type=%s", podSpec.SecurityContext.AppArmorProfile.Type)
			}
			if podSpec.SecurityContext.AppArmorProfile.Type == corev1.AppArmorProfileTypeLocalhost &&
				(podSpec.SecurityContext.AppArmorProfile.LocalhostProfile == nil || strings.TrimSpace(*podSpec.SecurityContext.AppArmorProfile.LocalhostProfile) == "") {
				return fmt.Errorf("podSecurityLevel baseline requires non-empty pod appArmorProfile.localhostProfile when type=Localhost")
			}
		}
		if err := validateBaselineSELinux(podSpec.SecurityContext.SELinuxOptions); err != nil {
			return err
		}
		for _, sysctl := range podSpec.SecurityContext.Sysctls {
			if !safeSysctls[sysctl.Name] {
				return fmt.Errorf("podSecurityLevel baseline forbids unsafe sysctl %q", sysctl.Name)
			}
		}
	}
	for _, c := range podSpec.Containers {
		if err := validateBaselineContainer(c.Name, c.Ports, c.SecurityContext); err != nil {
			return err
		}
	}
	for _, c := range podSpec.InitContainers {
		if err := validateBaselineContainer(c.Name, c.Ports, c.SecurityContext); err != nil {
			return err
		}
	}
	for _, c := range podSpec.EphemeralContainers {
		if err := validateBaselineContainer(c.Name, c.Ports, c.SecurityContext); err != nil {
			return err
		}
	}
	return nil
}

func applyRestrictedDefaultsToContainer(sc **corev1.SecurityContext) {
	if *sc == nil {
		*sc = &corev1.SecurityContext{}
	}
	if (*sc).AllowPrivilegeEscalation == nil {
		fl := false
		(*sc).AllowPrivilegeEscalation = &fl
	}
	if (*sc).Capabilities == nil {
		(*sc).Capabilities = &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		}
	} else if len((*sc).Capabilities.Drop) == 0 {
		(*sc).Capabilities.Drop = []corev1.Capability{"ALL"}
	}
}

func applyRestrictedDefaults(podSpec *corev1.PodSpec) {
	if podSpec.SecurityContext == nil {
		podSpec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if podSpec.SecurityContext.RunAsNonRoot == nil {
		allContainersExplicit := len(podSpec.Containers)+len(podSpec.InitContainers)+len(podSpec.EphemeralContainers) > 0
		for _, c := range podSpec.Containers {
			if c.SecurityContext == nil || c.SecurityContext.RunAsNonRoot == nil {
				allContainersExplicit = false
				break
			}
		}
		for _, c := range podSpec.InitContainers {
			if c.SecurityContext == nil || c.SecurityContext.RunAsNonRoot == nil {
				allContainersExplicit = false
				break
			}
		}
		for _, ec := range podSpec.EphemeralContainers {
			if ec.SecurityContext == nil || ec.SecurityContext.RunAsNonRoot == nil {
				allContainersExplicit = false
				break
			}
		}
		if !allContainersExplicit {
			tr := true
			podSpec.SecurityContext.RunAsNonRoot = &tr
		}
	}
	if podSpec.SecurityContext.SeccompProfile == nil {
		allContainersExplicitSeccomp := len(podSpec.Containers)+len(podSpec.InitContainers)+len(podSpec.EphemeralContainers) > 0
		for _, c := range podSpec.Containers {
			if c.SecurityContext == nil || c.SecurityContext.SeccompProfile == nil {
				allContainersExplicitSeccomp = false
				break
			}
		}
		for _, c := range podSpec.InitContainers {
			if c.SecurityContext == nil || c.SecurityContext.SeccompProfile == nil {
				allContainersExplicitSeccomp = false
				break
			}
		}
		for _, ec := range podSpec.EphemeralContainers {
			if ec.SecurityContext == nil || ec.SecurityContext.SeccompProfile == nil {
				allContainersExplicitSeccomp = false
				break
			}
		}
		if !allContainersExplicitSeccomp {
			podSpec.SecurityContext.SeccompProfile = &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			}
		}
	}
	for i := range podSpec.Containers {
		applyRestrictedDefaultsToContainer(&podSpec.Containers[i].SecurityContext)
	}
	for i := range podSpec.InitContainers {
		applyRestrictedDefaultsToContainer(&podSpec.InitContainers[i].SecurityContext)
	}
	for i := range podSpec.EphemeralContainers {
		applyRestrictedDefaultsToContainer(&podSpec.EphemeralContainers[i].SecurityContext)
	}
}

func validateRestrictedContainer(cName string, sc *corev1.SecurityContext, podRunAsNonRoot, podSeccompOK bool) error {
	if sc == nil {
		return fmt.Errorf("podSecurityLevel restricted requires securityContext on container %q", cName)
	}
	if sc.WindowsOptions != nil && sc.WindowsOptions.RunAsUserName != nil &&
		strings.EqualFold(strings.TrimSpace(*sc.WindowsOptions.RunAsUserName), "ContainerAdministrator") {
		return fmt.Errorf("podSecurityLevel restricted forbids windowsOptions.runAsUserName=ContainerAdministrator on container %q", cName)
	}
	if sc.RunAsNonRoot != nil && !*sc.RunAsNonRoot {
		return fmt.Errorf("podSecurityLevel restricted forbids runAsNonRoot=false on container %q", cName)
	}
	if !podRunAsNonRoot && (sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot) {
		return fmt.Errorf("podSecurityLevel restricted requires runAsNonRoot=true on pod or container %q", cName)
	}
	if sc.RunAsUser != nil && *sc.RunAsUser == 0 {
		return fmt.Errorf("podSecurityLevel restricted forbids runAsUser=0 on container %q", cName)
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		return fmt.Errorf("podSecurityLevel restricted requires allowPrivilegeEscalation=false on container %q", cName)
	}
	if sc.Capabilities == nil {
		return fmt.Errorf("podSecurityLevel restricted requires capabilities.drop=[\"ALL\"] on container %q", cName)
	}
	droppedAll := false
	for _, d := range sc.Capabilities.Drop {
		if normalizeCapabilityName(d) == "ALL" {
			droppedAll = true
			break
		}
	}
	if !droppedAll {
		return fmt.Errorf("podSecurityLevel restricted requires capabilities.drop=[\"ALL\"] on container %q", cName)
	}
	for _, a := range sc.Capabilities.Add {
		if normalizeCapabilityName(a) != "NET_BIND_SERVICE" {
			return fmt.Errorf("podSecurityLevel restricted forbids adding capability %q on container %q", a, cName)
		}
	}
	if sc.SeccompProfile != nil {
		if sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault &&
			sc.SeccompProfile.Type != corev1.SeccompProfileTypeLocalhost {
			return fmt.Errorf("podSecurityLevel restricted requires seccompProfile RuntimeDefault or Localhost on container %q (got %s)", cName, sc.SeccompProfile.Type)
		}
		if sc.SeccompProfile.Type == corev1.SeccompProfileTypeLocalhost &&
			(sc.SeccompProfile.LocalhostProfile == nil || strings.TrimSpace(*sc.SeccompProfile.LocalhostProfile) == "") {
			return fmt.Errorf("podSecurityLevel restricted requires non-empty seccompProfile.localhostProfile when type=Localhost on container %q", cName)
		}
	} else if !podSeccompOK {
		return fmt.Errorf("podSecurityLevel restricted requires seccompProfile RuntimeDefault or Localhost on container %q", cName)
	}
	return nil
}

func validateRestrictedPodSecurity(podSpec *corev1.PodSpec) error {
	for _, vol := range podSpec.Volumes {
		allowed := vol.ConfigMap != nil ||
			vol.DownwardAPI != nil ||
			vol.EmptyDir != nil ||
			vol.Projected != nil ||
			vol.Secret != nil ||
			vol.CSI != nil ||
			vol.Ephemeral != nil ||
			vol.PersistentVolumeClaim != nil
		if !allowed {
			return fmt.Errorf("podSecurityLevel restricted forbids volume %q of non-restricted type", vol.Name)
		}
	}

	if podSpec.SecurityContext != nil && podSpec.SecurityContext.WindowsOptions != nil &&
		podSpec.SecurityContext.WindowsOptions.RunAsUserName != nil &&
		strings.EqualFold(strings.TrimSpace(*podSpec.SecurityContext.WindowsOptions.RunAsUserName), "ContainerAdministrator") {
		return fmt.Errorf("podSecurityLevel restricted forbids pod windowsOptions.runAsUserName=ContainerAdministrator")
	}

	podRunAsNonRoot := podSpec.SecurityContext != nil && podSpec.SecurityContext.RunAsNonRoot != nil && *podSpec.SecurityContext.RunAsNonRoot
	totalContainers := len(podSpec.Containers) + len(podSpec.InitContainers) + len(podSpec.EphemeralContainers)
	if podSpec.SecurityContext != nil && podSpec.SecurityContext.RunAsNonRoot != nil && !*podSpec.SecurityContext.RunAsNonRoot && totalContainers == 0 {
		return fmt.Errorf("podSecurityLevel restricted forbids pod runAsNonRoot=false without container runAsNonRoot=true overrides")
	}
	if podSpec.SecurityContext != nil && podSpec.SecurityContext.RunAsUser != nil && *podSpec.SecurityContext.RunAsUser == 0 {
		return fmt.Errorf("podSecurityLevel restricted forbids pod runAsUser=0")
	}
	podSeccompOK := false
	if podSpec.SecurityContext != nil && podSpec.SecurityContext.SeccompProfile != nil {
		switch podSpec.SecurityContext.SeccompProfile.Type {
		case corev1.SeccompProfileTypeRuntimeDefault:
			podSeccompOK = true
		case corev1.SeccompProfileTypeLocalhost:
			if podSpec.SecurityContext.SeccompProfile.LocalhostProfile == nil ||
				strings.TrimSpace(*podSpec.SecurityContext.SeccompProfile.LocalhostProfile) == "" {
				return fmt.Errorf("podSecurityLevel restricted requires non-empty pod seccompProfile.localhostProfile when type=Localhost")
			}
			podSeccompOK = true
		default:
			return fmt.Errorf("podSecurityLevel restricted forbids pod seccompProfile.type=%s", podSpec.SecurityContext.SeccompProfile.Type)
		}
	}

	for _, c := range podSpec.Containers {
		if err := validateRestrictedContainer(c.Name, c.SecurityContext, podRunAsNonRoot, podSeccompOK); err != nil {
			return err
		}
	}
	for _, c := range podSpec.InitContainers {
		if err := validateRestrictedContainer(c.Name, c.SecurityContext, podRunAsNonRoot, podSeccompOK); err != nil {
			return err
		}
	}
	for _, c := range podSpec.EphemeralContainers {
		if err := validateRestrictedContainer(c.Name, c.SecurityContext, podRunAsNonRoot, podSeccompOK); err != nil {
			return err
		}
	}
	return nil
}

package main

import (
	"testing"
)

func TestResolveSidecarTokensFromEnvSanitizesCRLFBeforeValidation(t *testing.T) {
	rejectCases := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "empty",
			env:  map[string]string{},
		},
		{
			name: "only_crlf_and_spaces",
			env:  map[string]string{"XXL_JOB_ACCESS_TOKEN": " \r\n\t "},
		},
		{
			name: "default_token_trailing_crlf",
			env:  map[string]string{"XXL_JOB_ACCESS_TOKEN": "default_token\r\n"},
		},
		{
			name: "default_token_embedded_crlf",
			env:  map[string]string{"XXL_JOB_ACCESS_TOKEN": "default_\r\ntoken"},
		},
		{
			name: "fallback_also_default_token_crlf",
			env: map[string]string{
				"XXL_JOB_ACCESS_TOKEN": " \r\n ",
				"XXL_JOB_ACCESSTOKEN":  "\r\ndefault_token\n",
			},
		},
	}

	for _, tc := range rejectCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := resolveSidecarTokensFromEnv(func(k string) string {
				return tc.env[k]
			})
			if err == nil {
				t.Fatalf("expected error for env %+v, got nil", tc.env)
			}
		})
	}

	// Valid token with CRLF/whitespace, fallback when primary is whitespace/CRLF, and sanitized prev/shared tokens
	tok, prev, secret, err := resolveSidecarTokensFromEnv(func(k string) string {
		switch k {
		case "XXL_JOB_ACCESS_TOKEN":
			return " \r\n "
		case "XXL_JOB_ACCESSTOKEN":
			return "  prod-\r\ntoken-123\r\n "
		case "XXL_JOB_PREVIOUS_ACCESS_TOKEN":
			return " prev-tok\r\n"
		case "XXL_JOB_SIDECAR_SHARED_SECRET":
			return "\r\nshared-sec\n"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("expected valid fallback token, got err=%v", err)
	}
	if tok != "prod-token-123" || prev != "prev-tok" || secret != "shared-sec" {
		t.Fatalf("unexpected sanitized tokens: tok=%q prev=%q secret=%q", tok, prev, secret)
	}

	// Previous token sanitizing to "default_token" must be cleared to ""
	_, prevCleared, _, err := resolveSidecarTokensFromEnv(func(k string) string {
		switch k {
		case "XXL_JOB_ACCESS_TOKEN":
			return "valid-tok"
		case "XXL_JOB_PREVIOUS_ACCESS_TOKEN":
			return "default_\r\ntoken\r\n"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if prevCleared != "" {
		t.Fatalf("expected previous token sanitizing to default_token to be cleared, got %q", prevCleared)
	}
}

package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJobRoutesRequireBearerCredential(t *testing.T) {
	s := &Service{Token: strings.Repeat("x", 24)}
	h := s.Handler()
	for _, header := range []string{"", s.Token, "Bearer wrong"} {
		r := httptest.NewRequest(http.MethodPost, "/internal/v1/archive-jobs", strings.NewReader("{}"))
		r.Header.Set("Authorization", header)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("unauthorized request returned %d", w.Code)
		}
	}
}

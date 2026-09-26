package archive_test

import (
	"context"
	"github.com/aero-arc/aero-arc-archive-worker/internal/archive"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSnapshotBoundedAndNoRedirect(t *testing.T) {
	j, data := fixture()
	part := j.Sources[0]
	redirect := false
	oversize := false
	token := strings.Repeat("t", 24)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("missing credential")
		}
		if redirect {
			http.Redirect(w, r, "/other", http.StatusFound)
			return
		}
		b := data[part.ID]
		if oversize {
			b = append(append([]byte(nil), b...), byte(' '))
		}
		_, _ = w.Write(b)
	}))
	defer server.Close()
	source, err := archive.NewHTTPSource(server.URL, token, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = source.Read(context.Background(), j, part); err != nil {
		t.Fatal(err)
	}
	oversize = true
	if _, err = source.Read(context.Background(), j, part); err == nil {
		t.Fatal("oversize accepted")
	}
	redirect = true
	if _, err = source.Read(context.Background(), j, part); err == nil {
		t.Fatal("redirect followed")
	}
}

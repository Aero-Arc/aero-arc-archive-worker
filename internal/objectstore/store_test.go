package objectstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalImmutableVerification(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(context.Background(), "local", dir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = s.PutVerified(context.Background(), "flight/chunk", []byte("original"), "application/json"); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.PutVerified(context.Background(), "flight/chunk", []byte("replacement"), "application/json"); err == nil {
		t.Fatal("overwrote immutable evidence")
	}
	if err = os.WriteFile(filepath.Join(dir, "flight/chunk"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.PutVerified(context.Background(), "flight/chunk", []byte("original"), "application/json"); err == nil {
		t.Fatal("corruption not detected")
	}
	if err = s.PutVerified(context.Background(), "../escape", []byte("x"), ""); err == nil {
		t.Fatal("path escaped root")
	}
}

package service

import "testing"

func TestRegister(t *testing.T) {
	s := New()

	user, err := s.Register(" alice ")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if user.ID != "alice" || user.Username != "alice" {
		t.Fatalf("unexpected user: %+v", user)
	}

	if _, err := s.Register("alice"); err != ErrUserExists {
		t.Fatalf("expected duplicate user error, got %v", err)
	}
}

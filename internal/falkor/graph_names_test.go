package falkor

import "testing"

func TestUserGraphNameIsStableAndDoesNotContainIdentity(t *testing.T) {
	first := UserGraphName("tenant-a", "user@example.com")
	second := UserGraphName("tenant-a", "user@example.com")
	if first != second {
		t.Fatalf("graph name is not stable: %q != %q", first, second)
	}
	if first[:13] != "nightshift_u_" {
		t.Fatalf("unexpected graph name prefix: %q", first)
	}
	if first == "nightshift_u_user@example.com" || first == "nightshift_u_tenant-a" {
		t.Fatal("graph name leaked an identity value")
	}
}

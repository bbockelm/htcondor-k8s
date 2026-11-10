package main

import htcondor "github.com/bbockelm/golang-htcondor"

// newDirectSchedd skips the collector. An operator who already has the
// schedd's sinful string should not have to run a collector query to use it,
// and in a test harness the address is the only thing that is certain.
func newDirectSchedd(name, addr string) *htcondor.Schedd {
	if name == "" {
		name = "schedd"
	}
	return htcondor.NewSchedd(name, addr)
}

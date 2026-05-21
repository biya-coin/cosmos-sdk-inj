//go:build ignore

// This file exists because some of the DBs e.g CLevelDB
// require gcc as the compiler before they can ran otherwise
// we'll encounter crashes such as in https://github.com/tendermint/merkleeyes/issues/39
//
// Re-enable with: go test -tags=gcc (after regenerating mock for cosmos-db).

package iavl

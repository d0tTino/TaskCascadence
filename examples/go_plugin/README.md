# Go Cronyx Plugin

This directory shows how a Go based plugin might expose tasks through a
CronyxServer compatible API. The `go.mod` file defines a simple module and the
`main.go` implements a stub HTTP server returning task metadata.

## Building

Use the standard Go toolchain to create a binary:

```bash
go build -o go_cronyx_plugin .
```

## Serving

Run the compiled binary and point Cascadence at the CronyxServer by
setting the ``CRONYX_BASE_URL`` environment variable before starting
Cascadence:

```bash
./go_cronyx_plugin &
export CRONYX_BASE_URL=http://localhost:8000
# now run Cascadence normally
```

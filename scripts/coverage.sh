#!/usr/bin/env bash

set -euo pipefail

coverage_file="${1:-coverage.out}"
mapfile -t packages < <(
  go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./... |
    sed '/^$/d'
)

if [ "${#packages[@]}" -eq 0 ]; then
  echo "Не найдены Go-пакеты с тестами." >&2
  exit 1
fi

go test -race -covermode=atomic -coverprofile="${coverage_file}" "${packages[@]}"
go tool cover -func="${coverage_file}"

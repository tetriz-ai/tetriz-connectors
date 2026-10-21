#!/bin/sh
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT

# Lints the protobuf sources (buf lint), regenerates the committed generated
# code and license folder, then fails if
# the working tree differs: protobuf stubs (buf generate), enum SQL helpers
# (go generate ./gen/...), the gomock mocks (every //go:generate mockgen line),
# file headers, third_party/licenses and NOTICE's module list.
set -eu
cd "$(dirname "$0")/.."
buf lint
buf generate
(cd gen && go generate ./...)
git grep -l '^//go:generate go tool mockgen' -- '*.go' | while read -r f; do
	go generate "./$f"
done
go run ./internal/tools/headers .
go run ./internal/tools/headers -check
scripts/third-party-licenses.sh --check
set -- gen internal/gen proto internal/mocks ':(glob)**/mock_*_test.go'
if [ -n "$(git status --porcelain -- "$@")" ]; then
	git status --short -- "$@"
	git diff --stat -- "$@"
	echo "generated code is out of date; run scripts/check-generated.sh and commit the result" >&2
	exit 1
fi

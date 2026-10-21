#!/bin/sh
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT

# Regenerates third_party/licenses: the license (and, for MPL-2.0 modules,
# the source) of every module compiled into the release binary, for each
# release GOOS, plus the Go toolchain's own LICENSE. It also rewrites NOTICE's
# list of compiled-in modules from the same go-licenses data, so a module the
# binary gains or a version it bumps cannot be missing from NOTICE, and NOTICE's
# Go toolchain line from the Dockerfile's golang build stage.
#
#   scripts/third-party-licenses.sh          rewrite third_party/licenses and NOTICE's list
#   scripts/third-party-licenses.sh --check  fail if either differs from a fresh run
#
# Needs go-licenses (go install github.com/google/go-licenses@v1.6.0).
set -eu
cd "$(dirname "$0")/.."
MODULE=github.com/tetriz-ai/tetriz-connectors
OUT=third_party/licenses
GOROOT_DIR="$(go env GOROOT)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/merged"
for goos in linux darwin windows; do
	GOROOT="$GOROOT_DIR" GOOS="$goos" GOARCH=amd64 go-licenses save ./cmd/tetriz-connectors \
		--ignore "$MODULE" --save_path="$tmp/$goos" 2>/dev/null
	chmod -R u+w "$tmp/$goos"
	cp -Rf "$tmp/$goos/." "$tmp/merged/"
done
# go-licenses copies one license file per module. go.yaml.in/yaml/v2 carries a
# second one, LICENSE.libyaml (MIT), for the files ported from libyaml; it is
# copied only while the binary compiles that module in.
if [ -d "$tmp/merged/go.yaml.in/yaml/v2" ]; then
	yamlv2="$(go list -m -f '{{.Dir}}' go.yaml.in/yaml/v2)"
	cp "$yamlv2/LICENSE.libyaml" "$tmp/merged/go.yaml.in/yaml/v2/LICENSE.libyaml"
fi
# Some modules (golang.org/x/*, google.golang.org/protobuf) also carry a PATENTS
# grant beside their LICENSE; ship it next to the LICENSE go-licenses copied.
for dir in $(cd "$tmp/merged" && find . -name 'LICENSE*' -type f -exec dirname {} \; | sed 's|^\./||' | sort -u); do
	# The saved directory can be a package inside its module (golang.org/x/sys/unix):
	# walk up to the module root.
	mod="$dir" src=""
	while [ -z "$src" ] && [ "$mod" != "." ] && [ -n "$mod" ]; do
		src="$(go list -m -f '{{.Dir}}' "$mod" 2>/dev/null || true)"
		[ -n "$src" ] || mod="$(dirname "$mod")"
	done
	if [ -n "$src" ] && [ -f "$src/PATENTS" ]; then
		cp "$src/PATENTS" "$tmp/merged/$dir/PATENTS"
	fi
done
mkdir -p "$tmp/merged/go"
# Some distributions (Homebrew) keep the toolchain LICENSE one level above GOROOT.
golicense="$GOROOT_DIR/LICENSE"
[ -f "$golicense" ] || golicense="$GOROOT_DIR/../LICENSE"
cp "$golicense" "$tmp/merged/go/LICENSE"
# The toolchain's PATENTS grant sits in GOROOT (Homebrew keeps it there even
# when it moves the LICENSE up a level).
gopatents="$GOROOT_DIR/PATENTS"
[ -f "$gopatents" ] || gopatents="$GOROOT_DIR/../PATENTS"
cp "$gopatents" "$tmp/merged/go/PATENTS"
chmod -R u+w,go-w "$tmp/merged"


# notice_list prints one NOTICE line per compiled-in package: the package, its
# module's version, its license, the module's pkg.go.dev page and the license
# folder. Licenses go-licenses reads as one identifier where the module ships
# files under a second license are given in full here.
notice_list() {
	for goos in linux darwin windows; do
		GOROOT="$GOROOT_DIR" GOOS="$goos" GOARCH=amd64 go-licenses report ./cmd/tetriz-connectors \
			--ignore "$MODULE" 2>/dev/null
	done | sort -u -t, -k1,1 | while IFS=, read -r pkg _url lic; do
		mod="$pkg" ver=""
		while [ -z "$ver" ] && [ "$mod" != "." ] && [ -n "$mod" ]; do
			ver="$(go list -m -f '{{.Version}}' "$mod" 2>/dev/null || true)"
			[ -n "$ver" ] || mod="$(dirname "$mod")"
		done
		case "$pkg" in
		go.opentelemetry.io/otel | go.opentelemetry.io/otel/trace) lic="Apache-2.0 AND BSD-3-Clause" ;;
		go.yaml.in/yaml/v2) lic="Apache-2.0 AND MIT" ;;
		gopkg.in/yaml.v3) lic="MIT AND Apache-2.0" ;;
		esac
		echo "- $pkg $ver, $lic, https://pkg.go.dev/$mod@$ver, third_party/licenses/$pkg"
	done
}

# The toolchain line names the Go the release image is built with: the
# Dockerfile's build stage, not go.mod's go line, which is the floor.
gover="$(sed -n 's#^FROM docker.io/library/golang:\([0-9][0-9.]*\)-.*#\1#p' Dockerfile | head -1)"
if [ -z "$gover" ]; then
	echo "cannot read the Go version from the Dockerfile's golang build stage" >&2
	exit 1
fi

# NOTICE's list is the lines after the Go toolchain line, up to the next blank line.
notice_list >"$tmp/list"
awk -v list="$tmp/list" -v gover="go$gover" '
	skip && /^$/ { skip = 0 }
	skip { next }
	/^- The Go toolchain and standard library,/ {
		sub(/standard library, go[0-9][0-9.]*,/, "standard library, " gover ",")
		print
		while ((getline l < list) > 0) print l
		skip = 1
		next
	}
	{ print }
' NOTICE >"$tmp/NOTICE"

if [ "${1:-}" = "--check" ]; then
	if ! diff -r "$tmp/merged" "$OUT" >/dev/null; then
		diff -r "$tmp/merged" "$OUT" | head -20
		echo "third_party/licenses is out of date; run scripts/third-party-licenses.sh" >&2
		exit 1
	fi
	if ! diff "$tmp/NOTICE" NOTICE >/dev/null; then
		diff "$tmp/NOTICE" NOTICE | head -20
		echo "NOTICE's toolchain line or module list is out of date; run scripts/third-party-licenses.sh" >&2
		exit 1
	fi
	exit 0
fi
cp "$tmp/NOTICE" NOTICE
rm -rf "$OUT"
mkdir -p "$(dirname "$OUT")"
cp -R "$tmp/merged" "$OUT"

#!/bin/sh
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT

# Runs the project's CI on this machine. There is no hosted CI: run this before
# every push, or install the pre-push hook with scripts/install-hooks.sh.
#
#   scripts/ci.sh              every job
#   scripts/ci.sh test lint    only the named jobs
#
# Jobs: test lint vulncheck examples generated image compose helm release_checks.
#
# The test and compose jobs start their own Postgres, Redis and compose stack
# under a unique tetriz-ci-<pid> project, on ports beside the quick start's, and
# remove them afterwards; a local quick-start stack is left alone. They need
# docker or podman. Set TETRIZ_TEST_POSTGRES_DSN and TETRIZ_TEST_REDIS_ADDR to
# test against databases of your own instead.
#
# golangci-lint v2.14.0, go-licenses v1.6.0 and govulncheck v1.8.0 are
# installed into ~/.cache/tetriz-ci (CI_TOOLS overrides it), not over the
# copies on PATH. The other pinned tools are checked before their job runs:
# buf 1.71.0, helm v4.3.0 and kubeconform v0.8.0, plus hadolint and trivy for
# the image job. The test job runs Go 1.26.0, the
# latest 1.26 and the latest 1.27 through GOTOOLCHAIN; set CI_GO_VERSIONS
# (for example "go1.26.0 go1.26.10 go1.27.3") to skip looking those up.
set -eu
cd "$(dirname "$0")/.."

all_jobs="test lint vulncheck examples generated image compose helm release_checks"
jobs=${*:-$all_jobs}
for j in $jobs; do
	case " $all_jobs " in
	*" $j "*) ;;
	*)
		echo "unknown job $j; jobs are: $all_jobs" >&2
		exit 2
		;;
	esac
done

if command -v docker >/dev/null 2>&1; then
	engine=docker
elif command -v podman >/dev/null 2>&1; then
	engine=podman
else
	engine=
fi

# The CI stack: its own compose project and ports, so it never touches a
# quick-start stack someone has up.
export TETRIZ_PG_PORT="${CI_PG_PORT:-55433}"
export TETRIZ_REDIS_PORT="${CI_REDIS_PORT:-56380}"
export TETRIZ_RUNNER_PORT="${CI_RUNNER_PORT:-9091}"
export TETRIZ_IMAGE=tetriz-connectors:ci
COMPOSE="$engine compose -p tetriz-ci-$$"
stack_owned=false
export COMPOSE

need() {
	command -v "$1" >/dev/null 2>&1 || {
		echo "$1 is not installed" >&2
		return 1
	}
}

need_engine() {
	[ -n "$engine" ] || {
		echo "this job needs docker or podman" >&2
		return 1
	}
}

# need_version tool want cmd...: cmd's output must contain want.
need_version() {
	tool=$1 want=$2
	shift 2
	need "$tool" || return 1
	got=$("$@" 2>&1 | head -1)
	case "$got" in
	*"$want"*) ;;
	*)
		echo "$tool must be $want; found: $got" >&2
		return 1
		;;
	esac
}

# latest_go 1.27 prints the newest released go1.27.x.
latest_go() {
	curl -fsSL 'https://go.dev/dl/?mode=json&include=all' |
		grep -o "\"version\": *\"go$1\.[0-9]*\"" | grep -o "go$1\.[0-9]*" |
		sort -t. -k3 -n | tail -1
}

go127() {
	v=$(latest_go 1.27)
	[ -n "$v" ] || {
		echo "could not look up the latest Go 1.27" >&2
		return 1
	}
	echo "$v"
}

tools=${CI_TOOLS:-${XDG_CACHE_HOME:-$HOME/.cache}/tetriz-ci}

# tool name module version go: prints the path of name, built from
# module@version with Go go into $tools/name-version.
tool() {
	bin="$tools/$1-$3/$1"
	if [ ! -x "$bin" ]; then
		GOTOOLCHAIN=$4 GOBIN="$tools/$1-$3" go install "$2@$3" >&2 || return 1
	fi
	echo "$bin"
}

stack_down() {
	if [ "$stack_owned" = true ]; then
		$COMPOSE down -v >/dev/null 2>&1 || true
	fi
}
trap stack_down EXIT

job_test() {
	versions=${CI_GO_VERSIONS:-}
	if [ -z "$versions" ]; then
		v126=$(latest_go 1.26)
		v127=$(go127)
		[ -n "$v126" ] || {
			echo "could not look up the latest Go 1.26" >&2
			return 1
		}
		versions="go1.26.0 $v126 $v127"
	fi
	if [ -z "${TETRIZ_TEST_POSTGRES_DSN:-}" ] || [ -z "${TETRIZ_TEST_REDIS_ADDR:-}" ]; then
		need_engine || return 1
		stack_owned=true
		$COMPOSE up -d --wait postgres redis || return 1
	fi
	dsn=${TETRIZ_TEST_POSTGRES_DSN:-"host=127.0.0.1 port=$TETRIZ_PG_PORT user=tetriz password=tetriz-local-only dbname=postgres sslmode=disable"}
	redis=${TETRIZ_TEST_REDIS_ADDR:-"127.0.0.1:$TETRIZ_REDIS_PORT"}
	for v in $versions; do
		echo "--- Go $v"
		GOTOOLCHAIN=$v TETRIZ_REQUIRE_TEST_DB=1 TETRIZ_TEST_POSTGRES_DSN=$dsn TETRIZ_TEST_REDIS_ADDR=$redis \
			sh -c '
				go version &&
				go build ./... && go vet ./... && go test -race -count=1 ./... &&
				go test -race -count=1 -tags=stress -run TestRateLimitStormSimulation ./connectors/bitbucket/ &&
				go run ./cmd/tetriz-connectors version &&
				go run ./cmd/tetriz-connectors connectors | head -3
			' || return 1
	done
}

job_lint() {
	# golangci-lint v2.14.0 is built with Go 1.27; raise both together.
	v=$(go127) || return 1
	lint=$(tool golangci-lint github.com/golangci/golangci-lint/v2/cmd/golangci-lint v2.14.0 "$v") || return 1
	GOTOOLCHAIN=$v "$lint" run ./...
}

job_vulncheck() {
	# govulncheck must be built with the Go it scans, so it runs under Go 1.27.
	v=$(go127) || return 1
	vuln=$(tool govulncheck golang.org/x/vuln/cmd/govulncheck v1.8.0 "$v") || return 1
	GOTOOLCHAIN=$v "$vuln" ./...
}

job_examples() {
	v=$(go127) || return 1
	GOTOOLCHAIN=$v scripts/run-examples.sh
}

job_generated() {
	need_version buf 1.71.0 buf --version || return 1
	licenses=$(tool go-licenses github.com/google/go-licenses v1.6.0 local) || return 1
	PATH="$(dirname "$licenses"):$PATH" scripts/check-generated.sh
}

job_image() {
	need_engine && need hadolint && need trivy || return 1
	hadolint Dockerfile || return 1
	$engine build -t "$TETRIZ_IMAGE" . || return 1
	# trivy scans a saved archive, which works the same for docker and podman.
	tar=$(mktemp)
	status=0
	$engine save -o "$tar" "$TETRIZ_IMAGE" &&
		trivy image --input "$tar" --exit-code 1 --severity CRITICAL,HIGH --ignore-unfixed || status=1
	rm -f "$tar"
	return "$status"
}

job_compose() {
	need_engine || return 1
	status=0
	stack_owned=true
	$COMPOSE up --build --wait && scripts/acceptance.sh </dev/null || status=1
	[ "$status" = 0 ] || $COMPOSE logs --no-color 2>&1 | tail -200
	$COMPOSE down -v >/dev/null 2>&1 || true
	stack_owned=false
	return "$status"
}

job_release_checks() {
	need python3 || return 1
	python3 -m unittest discover -s scripts/tests -v
}

job_helm() {
	need_version helm v4.3.0 helm version --short || return 1
	need_version kubeconform v0.8.0 kubeconform -v || return 1
	chart=deploy/helm/tetriz-connectors
	# The Kubernetes schema version and the CRD catalog commit kubeconform checks against.
	k8s=1.37.0
	crds=4c8dc296d32b06d15ccde9668ff136c951f4d539
	out=$(mktemp -d)
	# The default values need the three required settings to render.
	set -- --set config.redisAddr=valkey:6379 --set secrets.postgresDsn=dsn --set secrets.credentialsKey=key
	status=0
	{
		helm lint --strict "$chart" "$@" &&
			helm lint --strict "$chart" -f "$chart/values-demo.yaml" &&
			helm lint --strict "$chart" -f "$chart/ci/full-values.yaml" &&
			helm template ci "$chart" "$@" >"$out/default.yaml" &&
			helm template ci "$chart" -f "$chart/values-demo.yaml" >"$out/demo.yaml" &&
			helm template ci "$chart" -f "$chart/ci/full-values.yaml" >"$out/full.yaml" &&
			kubeconform -strict -summary -kubernetes-version "$k8s" \
				-schema-location default \
				-schema-location "https://raw.githubusercontent.com/datreeio/CRDs-catalog/$crds/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json" \
				"$out"/*.yaml
	} || status=1
	rm -rf "$out"
	return "$status"
}

failed=
for j in $jobs; do
	echo "=== $j"
	if "job_$j"; then
		echo "=== $j: ok"
	else
		echo "=== $j: FAILED" >&2
		failed="$failed $j"
	fi
done

if [ -n "$failed" ]; then
	echo "CI failed:$failed" >&2
	exit 1
fi
echo "CI passed: $jobs"

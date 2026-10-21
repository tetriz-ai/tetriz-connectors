#!/bin/sh
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT

# Installs a git pre-push hook that runs scripts/ci.sh, so a push goes out only
# after the local CI passes. `git push --no-verify` skips it once. Set
# CI_JOBS to run a subset, for example CI_JOBS="test lint" git push.
set -eu
cd "$(dirname "$0")/.."

hook=$(git rev-parse --git-path hooks)/pre-push
if [ -e "$hook" ] && ! grep -q 'scripts/ci.sh' "$hook"; then
	echo "$hook exists and is not this project's hook; leaving it alone" >&2
	exit 1
fi
mkdir -p "$(dirname "$hook")"
cat >"$hook" <<'EOF'
#!/bin/sh
# Installed by scripts/install-hooks.sh: run the local CI before pushing.
exec "$(git rev-parse --show-toplevel)/scripts/ci.sh" ${CI_JOBS:-}
EOF
chmod +x "$hook"
echo "installed $hook"

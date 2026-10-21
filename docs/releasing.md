# Releasing without GitHub Actions

GitHub Actions stays disabled. A Git tag alone does not upload binaries or publish an image.
Run the local gate, prepare the assets, then publish them explicitly. Do not advertise the
Releases download or GHCR image until each exists and can be downloaded anonymously.

## Prepare on the release machine

Use a clean checkout on `main`. The repository's default branch must also be `main` (set it
when creating the destination). Required tools: Python 3.9+, Git, Go with toolchain download
support, Docker or Podman, and the pinned tools listed in `CONTRIBUTING.md`.

1. Replace the pending date in `CHANGELOG.md` with the actual release date and commit it.
2. Run `scripts/ci.sh` in full, retaining its output with the commit ID. This includes the
   database-required tests, examples, generated files, license regeneration, vulnerability
   check, image scan, compose acceptance, Helm checks, and release-helper safety tests.
3. Run `python3 scripts/prepare-release.py v0.1.0`. It builds Linux, macOS and Windows for
   amd64 and arm64 with Go 1.27.3 and CGO disabled. `dist/v0.1.0/` contains six archives,
   SHA-256 checksums, and `SOURCE.json` with the exact commit/toolchain. Every archive includes
   `LICENSE`, `NOTICE`, `third_party/`, `README.md` and `SOURCE.json`. The helper refuses a
   dirty checkout, a branch other than `main`, invalid versions and an existing output folder.
   A failed build can leave an incomplete folder; inspect it before removing it yourself.
4. Verify `checksums.txt`, inspect the archive contents, and smoke-test the native executable's
   `version`, `connectors` and `schema` commands. Test a Linux archive in a container too.
   The reported version must be the tag. Cross-compilation does not replace these smoke tests.

The helper prepares assets only. It neither runs CI nor creates a tag or release. The older
`.goreleaser.yaml` remains available for optional GoReleaser users; do not mix its outputs with
this helper's output folder. Never publish artifacts from an unvalidated or changed commit.

## Publish the source and binary assets

After the local gate passes and the destination exists, compare `SOURCE.json`'s commit to
`git rev-parse HEAD`. Set `version` and `source` from that file, then:

```sh
set -eu
version=v0.1.0
source=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["commit"])' "dist/$version/SOURCE.json")
test "$(git branch --show-current)" = main
test -z "$(git status --porcelain)"
test "$source" = "$(git rev-parse HEAD)"
if git grep -n CHANGELOG_DATE_PLACEHOLDER -- CHANGELOG.md; then exit 1; fi
git push origin main
git tag -a "$version" "$source" -m "Release $version"
git push origin "refs/tags/$version"
gh release create "$version" "dist/$version/"* \
  --repo tetriz-ai/tetriz-connectors --verify-tag --draft \
  --title "$version" --notes-file /absolute/path/to/release-notes.txt
```

Write the release notes outside this clean checkout before running these commands. Keep the
release as a draft while checking all six archive attachments and `checksums.txt`. Publish the
draft explicitly with `gh release edit "$version" --repo tetriz-ai/tetriz-connectors --draft=false`.
The automatically generated source archives alone are not binary assets. Do not enable Actions.
If a release already exists, inspect its tag and contents before explicitly uploading missing
assets with `gh release upload`; never overwrite an existing release's artifacts blindly.

## Build and publish the container separately

From the same validated commit, build a local image and inspect its embedded legal notices:

```sh
version=v0.1.0
docker build --build-arg VERSION="$version" -t "tetriz-connectors:$version" .
docker run --rm "tetriz-connectors:$version" version
docker run --rm "tetriz-connectors:$version" connectors
docker run --rm --entrypoint sh "tetriz-connectors:$version" \
  -c 'test -s /usr/share/doc/tetriz-connectors/LICENSE && test -s /usr/share/doc/tetriz-connectors/NOTICE'
```

After scanning that image and passing compose acceptance, an authenticated maintainer with
GHCR package write permission can publish both architectures explicitly:

```sh
# Authenticate with docker login ghcr.io; supply a token through --password-stdin.
# The token needs write:packages; do not commit it or put it in the command line.
docker buildx build --platform linux/amd64,linux/arm64 \
  --build-arg VERSION="$version" \
  --tag "ghcr.io/tetriz-ai/tetriz-connectors:$version" \
  --tag ghcr.io/tetriz-ai/tetriz-connectors:latest --push .
```

This last command uploads to GHCR. Inspect the published manifest for both platforms, set the
GHCR package visibility to public in its package settings, and verify an anonymous pull of the
versioned image on each platform. A public repository does not make its GHCR package public.
Retain the pushed digest with the validation evidence. Publishing to GHCR does not require
GitHub Actions.

#!/usr/bin/env bash
# Bump the release version everywhere it is pinned.
#
#   ./scripts/bump_version.sh 8.1.0
#
# Updates:
#   - stac_fastapi/*/stac_fastapi/*/version.py   (__version__)
#   - stac_fastapi/*/stac_fastapi/*/app.py       (STAC_FASTAPI_VERSION fallback)
#   - stac_fastapi/*/pyproject.toml              (inter-package ==X.Y.Z pins,
#     including extras like stac-fastapi-core[catalogs]==X.Y.Z)
#   - pyproject.toml                             (workspace root version)
#   - helm-chart/stac-fastapi/values.yaml        (STAC_FASTAPI_VERSION)
#   - CHANGELOG.md                               ([Unreleased] -> [vX.Y.Z] and
#     the compare links at the bottom)
#
# Still manual:
#   - README.md "Latest News" headline
#   - helm-chart/stac-fastapi/Chart.yaml         (appVersion is "latest";
#     `version` is the chart's own semver)
#   - uv.lock                                    (run `uv lock` afterwards)

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

NEW="${1:?usage: $0 X.Y.Z}"
[[ "$NEW" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
    echo "error: version must be X.Y.Z, got '$NEW'" >&2
    exit 1
}
TODAY="$(date +%F)"
PKGS="stac-fastapi-core|stac-fastapi-elasticsearch|stac-fastapi-opensearch|sfeos-helpers"

# 1. package __version__ modules (source of truth for hatch builds)
for f in stac_fastapi/*/stac_fastapi/*/version.py; do
    sed -i -E "s/__version__ = \"[0-9]+\.[0-9]+\.[0-9]+\"/__version__ = \"$NEW\"/" "$f"
    echo "bumped $f"
done

# 2. STAC_FASTAPI_VERSION fallback in each app's instantiate_api
for f in stac_fastapi/*/stac_fastapi/*/app.py; do
    sed -i -E "s/os\.getenv\(\"STAC_FASTAPI_VERSION\", \"[0-9]+\.[0-9]+\.[0-9]+\"\)/os.getenv(\"STAC_FASTAPI_VERSION\", \"$NEW\")/" "$f"
    echo "bumped $f"
done

# 3. inter-package pins inside each package's pyproject.toml
for f in stac_fastapi/*/pyproject.toml; do
    sed -i -E "s/(${PKGS})(\[[^]]*\])?==[0-9]+\.[0-9]+\.[0-9]+/\1\2==${NEW}/g" "$f"
    echo "bumped pins in $f"
done

# 4. workspace root version
sed -i -E "s/^version = \"[0-9]+\.[0-9]+\.[0-9]+\"/version = \"$NEW\"/" pyproject.toml
echo "bumped pyproject.toml"

# 5. helm runtime default
sed -i -E "s/STAC_FASTAPI_VERSION: \"[0-9]+\.[0-9]+\.[0-9]+\"/STAC_FASTAPI_VERSION: \"$NEW\"/" \
    helm-chart/stac-fastapi/values.yaml
echo "bumped helm-chart/stac-fastapi/values.yaml"

# 6. CHANGELOG: move the Unreleased section under the new version and update
#    the compare links. Skipped if Unreleased is empty.
PREV="$(grep -oE '^\[v[0-9]+\.[0-9]+\.[0-9]+\]' CHANGELOG.md | head -1 | tr -d '[]v')"
if grep -A3 '^## \[Unreleased\]$' CHANGELOG.md | grep -qE '^(### |-)'; then
    sed -i -E "0,/^## \[Unreleased\]$/s//## [Unreleased]\n\n## [v${NEW}] - ${TODAY}/" CHANGELOG.md
    sed -i -E "s|^\[Unreleased\]: (.*)/compare/v${PREV}\.\.\.main|[Unreleased]: \1/compare/v${NEW}...main\n[v${NEW}]: \1/compare/v${PREV}...v${NEW}|" \
        CHANGELOG.md
    echo "bumped CHANGELOG.md (previous release: v${PREV})"
else
    echo "CHANGELOG.md Unreleased section is empty; left unchanged"
fi

echo
echo "Done. Remaining manual steps:"
echo "  - uv lock"
echo "  - README.md Latest News headline"
echo "  - review 'git diff' and commit"

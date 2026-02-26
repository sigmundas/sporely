#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
main_py="$root/main.py"

if [[ ! -f "$main_py" ]]; then
    echo "Error: main.py not found at $main_py" >&2
    exit 1
fi

version=$(python3 - "$main_py" << 'PYEOF'
import re, sys
text = open(sys.argv[1]).read()
m = re.search(r"APP_VERSION\s*=\s*[\"']([\w.]+)", text)
if not m:
    sys.exit("APP_VERSION not found in main.py")
print(m.group(1))
PYEOF
)

if [[ -z "$version" ]]; then
    echo "Error: APP_VERSION is empty" >&2
    exit 1
fi

tag="v$version"

if git -C "$root" tag -l "$tag" | grep -q "^${tag}$"; then
    echo "Error: Tag $tag already exists" >&2
    exit 1
fi

git -C "$root" tag -a "$tag" -m "$tag"
echo "Created tag $tag"

echo "Pushing commits..."
git -C "$root" push
echo "Pushing tags..."
git -C "$root" push --tags

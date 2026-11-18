#!/bin/bash
# Offline verification of the feed: see SKILL.md. Run from anywhere in the repository.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 1
scheduler=no
[ "${1:-}" = "--scheduler" ] && scheduler=yes
git status --porcelain -- scheduler | grep -q . && scheduler=yes
git diff --name-only origin/main...HEAD 2>/dev/null | grep -q '^scheduler/' && scheduler=yes

step() { printf '%-46s' "$1"; }
fail() { echo "FAILED"; echo "$1" | tail -25; rm -rf __pycache__ tests/__pycache__ scheduler/__pycache__; exit 1; }

step "Python files compile"
out=$(python3 -m py_compile ./*.py tests/*.py scheduler/*.py .claude/skills/verify/*.py 2>&1) || fail "$out"
echo "ok"

step "Tests of tests/"
out=$(python3 -m unittest discover -s tests 2>&1) || fail "$out"
echo "ok ($(echo "$out" | grep -E '^Ran ' | tail -1))"

if [ "$scheduler" = yes ]; then
  step "Tests of the Worker (scheduler/)"
  out=$(cd scheduler && npm test 2>&1) || fail "$out"
  echo "ok ($(echo "$out" | grep -E '^ℹ pass' | tail -1))"
else
  step "Tests of the Worker (scheduler/)"; echo "not run: scheduler/ untouched"
fi

step "Workflows are valid YAML"
out=$(ruby -ryaml -e 'Dir[".github/workflows/*.yml"].sort.each { |f| d = YAML.load_file(f); abort("#{f}: no trigger or no job") unless d.is_a?(Hash) && d.key?(true) && d["jobs"].is_a?(Hash) && !d["jobs"].empty? }; puts Dir[".github/workflows/*.yml"].size' 2>&1) || fail "$out"
echo "ok ($out files)"

step "JSON files of data/ parse"
out=$(python3 - <<'PY' 2>&1
import json, pathlib, sys
bad, count = [], 0
for path in sorted(pathlib.Path('data').rglob('*.json')):
    count += 1
    try:
        json.loads(path.read_text(encoding='utf-8'))
    except (ValueError, OSError) as e:
        bad.append(f'{path}: {e}')
print('\n'.join(bad[:20]) if bad else count)
sys.exit(1 if bad else 0)
PY
) || fail "$out"
echo "ok ($out files)"

rm -rf __pycache__ tests/__pycache__ scheduler/__pycache__ .claude/skills/verify/__pycache__
echo "OFFLINE OK"

---
description: 'Trader: prepare and publish a new GitHub release end-to-end (version bump, docs, QA, build, tag, GitHub release)'
---

Perform the full `QS-Trader` release workflow end-to-end for a new version.

## Required inputs

Collect or confirm these before doing anything destructive:

- **Target version** (required), e.g. `0.2.0-beta.7` or `0.2.0`
- **Release scope**: what changed since the last release
- **Release type**: prerelease vs stable
- **Permission to publish** if the request is ambiguous or the user only asked for preparation

If the target version is missing, inspect the current version and recent changes, propose the next sensible version, and ask for confirmation before tagging or publishing.

## Repo-specific rules to follow

- Follow `.github/instructions/copilot-instructions.md`
- Use the existing Makefile workflow instead of inventing a parallel release process
- Treat a failed QA/build/test step as a hard stop
- Do **not** create or push a tag if:
  - the working tree contains unrelated/unreviewed changes
  - versioned files are inconsistent
  - QA/build/test checks fail
  - release notes or docs are clearly outdated

## Release workflow

### 1. Baseline checks

1. Inspect git status and confirm you are in `QS-Trader`
1. Read the current version from `pyproject.toml` (or use `make version`)
1. Review `CHANGELOG.md`, `README.md`, and relevant `docs/**`
1. Search the repo for hardcoded references to the previous version and outdated release notes/examples
1. Review the diff since the last tag, or current uncommitted changes, to understand what is actually being released

### 2. Update release metadata

1. Update `pyproject.toml` to the target version
1. Update `CHANGELOG.md`
   - add a new dated section for the target version
   - summarize notable changes clearly
   - keep the existing Keep a Changelog structure (`Added`, `Changed`, `Fixed`, etc.)
1. Update version references in docs where needed
   - `README.md`
   - install examples pinned to older tags
   - affected docs under `docs/**`
   - scaffold docs/templates if release-visible behavior changed
1. If the version bump or dependency metadata causes lock/build metadata changes, update the relevant generated file(s) as well

### 3. Validate docs and version consistency

Verify that:

- `pyproject.toml` version matches the intended release version
- changelog heading/version/date are correct
- README install examples are not pointing at stale versions
- docs reflect newly added features, renamed commands, or changed output formats
- no obvious release notes are missing for user-facing changes

### 4. Run the full quality gate

Run the repo’s standard checks and fix issues until they pass:

1. `make qa`
1. `make build`
1. Verify the package version from the built environment, e.g. `uv run qs-trader --version`
1. Confirm the build artifacts in `dist/` match the target version

If any command fails, debug and fix the underlying issue before continuing.

### 5. Final pre-release review

Before publishing, confirm all of the following:

- git diff contains only intentional release-related changes
- generated artifacts match the target version
- tests passed
- lint/format/type checks passed
- changelog is ready to become release notes
- the repo is in a clean releasable state

Then create a release commit using a conventional message such as:

- `chore(release): prepare vX.Y.Z`

If pre-commit hooks or formatters modify files, restage and re-run checks as needed.

### 6. Publish the release

Use the repository’s existing release flow:

1. Run `make release VERSION=X.Y.Z`
1. If GitHub CLI is available and authenticated, prefer completing the GitHub release as well (attach `dist/*`, use the changelog as release notes, and mark prerelease appropriately when relevant)
1. If GitHub CLI is unavailable, complete everything up to tag push and clearly report the exact remaining GitHub UI step

Never claim the release is published unless the tag push and GitHub release creation both succeeded, or you explicitly state the remaining manual step.

## Extra checks you should perform

- Search for stale version strings like prior beta tags in docs/examples and update them only where appropriate
- Make sure release notes describe the actual shipped changes, not unrelated work-in-progress
- Ensure the release does not leave behind accidental local-only files or noisy generated artifacts unless they are intentionally versioned
- If the release changes CLI behavior, package layout, outputs, or scaffolded files, update documentation accordingly

## Expected response back to the user

Return a concise release report containing:

- target version
- files updated
- checks run and their outcomes
- whether build artifacts were produced successfully
- whether the tag was created and pushed
- whether the GitHub release was fully published or what manual step remains
- any follow-up recommendations

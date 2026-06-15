# Releasing duratiq

Releases are published to [PyPI](https://pypi.org/p/duratiq) by the
[`release`](.github/workflows/release.yml) workflow when a `v*` tag is pushed. It
uses **PyPI Trusted Publishing** (OIDC) — there is no API token stored in the repo.

## One-time setup (PyPI side)

Trusted Publishing must be configured once before the first automated release.

1. On PyPI, go to the `duratiq` project → **Settings → Publishing** (or, for the very
   first release, add a **pending publisher** under your account → Publishing).
2. Add a GitHub Actions publisher with:
   - **Owner**: `ivancrneto`
   - **Repository**: `duratiq`
   - **Workflow name**: `release.yml`
   - **Environment**: `pypi`
3. (Recommended) In the GitHub repo, create an **Environment** named `pypi` under
   Settings → Environments and add protection rules (e.g. required reviewers) so a
   publish can't happen without approval.

## Cutting a release

1. Make sure `main` is green (test / pre-commit / build / admin / coverage).
2. Bump `version` in [`pyproject.toml`](pyproject.toml) and move the
   [`CHANGELOG.md`](CHANGELOG.md) `[Unreleased]` entries under a new
   `## [X.Y.Z] — YYYY-MM-DD` heading (update the compare/link refs at the bottom).
3. Merge that to `main`.
4. Tag and push:

   ```bash
   git checkout main && git pull
   git tag vX.Y.Z          # must equal the pyproject version
   git push origin vX.Y.Z
   ```

The workflow then runs the test suite, builds the sdist + wheel, `twine check`s them,
verifies the tag matches the package version, and publishes to PyPI. Watch it under
the repo's **Actions → release**.

> The tag must match the version in `pyproject.toml` — the `build` job fails the
> release if `vX.Y.Z` and the built `duratiq-X.Y.Z` disagree.

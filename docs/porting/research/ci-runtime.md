# CI runtime review (S22a)

Measured 2026-11-11 from `gh run view <id> --json jobs` over the eight latest successful `ci` and `security` runs (S19 to S22),
then per step for the S22 pull request run. Decision: [D-56](../decisions.md#d-56).

Python parity: not applicable (CI only; no Python behaviour is replaced).

## Before

Wall time per job in minutes (pull request runs and pushes to `main`):

| Job | Range | Where the time went (S22 PR run) |
|---|---|---|
| `dotnet` | 6.9 to 10.7 | `dotnet test` 304 s, of which `MicroDCS.IntegrationTests` 4:58 (every unit assembly under 6 s); format 57 s; build 52 s; Sonar end 33 s; audit 17 s |
| `integration` | 5.1 to 10.4 | test image build 70 s (SDK pull 17 s, restore 15 s, build 29 s; no cache), the same integration suite again 4:51, replica tests 142 s (with their own runtime AOT build) |
| `container (linux-x64)` | 3.0 to 7.7 | image build 264 s |
| `container (linux-arm64)` | 3.3 to 6.3 | image build 224 s |
| `kubernetes` | 3.5 | its own image build 112 s, kind 83 s |
| `trivy-image` (`security`) | 2.6 to 3.9 | a fourth x64 image build, scan |
| `gitleaks`, `workflows`, `trivy-fs` | 0.1 to 0.8 | |

Critical path of a PR: 8 to 11 min (`dotnet` or `integration`). The runtime image was built five times per PR
(two container builds, kubernetes, trivy-image, replica tests).

### Why the integration suite took 5 minutes

Per class (local TRX, 335 test-seconds in all): `SfcHostTests` 295 s for 28 tests, nine of them about 30 s each. The time sat
in `MicroDcsHostedService.StopAsync` (29.7 s, every other hosted service under 20 ms): shutdown phase 5 of D-37 waits for the
expiry timers of open commands until the grace period ends, the recipes in those tests leave commands with
`TimeoutSeconds: 30` open, and the rig stopped the host with `CancellationToken.None`. Production is not affected (grace
period 30 s, `terminationGracePeriodSeconds: 45`). With a 1 s grace period in the rig the suite takes 35 s locally instead of
6:26; the whole solution with coverage takes 48 s (1035 tests, 0 skipped).

### `scripts/aot-publish` and the lock file

`dotnet publish -r <rid>` restores with the RID as a global property, which reaches every referenced project and writes the
RID into `src/MicroDCS.Generators/packages.lock.json` (netstandard2.0, no RIDs on purpose); the next locked restore (Docker
build) then fails. Reproduced on `main`. The script now restores without a RID first and publishes with `--no-restore`, as
the Dockerfile does; the `src/` projects declare both RIDs, so the runtime packs are restored.

## Options per job

| Job | Option | Taken |
|---|---|---|
| all | Fix the 30 s waits in the SFC rig | yes |
| `dotnet` + `integration` | Run the integration suite once (host, with coverage) | yes (D-15 revised) |
| docs-only PRs | Skip tests and images, keep the Sonar analysis | yes |
| `container (linux-arm64)` | PRs only for image inputs; `main` and daily always | yes (D-32 revised) |
| `kubernetes` | PRs only for manifest or image inputs; reuse the x64 image | yes |
| `integration` | Reuse the x64 image of `container` | no: two builds in a row on the critical path |
| `trivy-image` | Move into `ci.yml` to reuse the image | no: belongs to the daily security scan; off the critical path |
| `dotnet` | Split format and audit into a job | no: after the test fix `dotnet` is under the container build |
| compose test image | buildx `type=gha` cache in compose | no: needs the runtime token exposed (marketplace action) for about 30 s |

## After

Filled from the S22a pull request run and a documentation-only probe PR; see the [log](../log.md) entry.

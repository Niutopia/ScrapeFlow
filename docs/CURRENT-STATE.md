# ScrapeFlow Current State

> Baseline snapshot audited: `9fc2aa0` on branch `codex/scrapeflow-transactional-convergence`.
> This file describes implementation facts for that snapshot. It is not the long-lived product contract.

## 1. Git baseline

The supplied evidence shows:

- clean work tree;
- HEAD `9fc2aa0`;
- branch `codex/scrapeflow-transactional-convergence`;
- no uncommitted diff.

The recent commit sequence is heavily weighted toward release/acceptance/preflight/readiness hardening plus the Quark sidecar transition.

## 2. Docker topology: already aligned

`docker-compose.yml` defines exactly three services:

- `alist`
- `api`
- `quark-helper`

Confirmed characteristics:

- Compose project name is `scrapeflow`.
- AList and API publish only loopback host ports.
- `quark-helper` uses `network_mode: service:api` and shares the API network namespace.
- The Helper is not a separate product/business service; it is a narrow sidecar using the same API image.
- The API container is the modular monolith.

This topology should be preserved. There is no current need to add worker/scheduler/audit/provider containers.

## 3. Ordinary intake: current implementation conflicts with the target product flow

Current discovery path:

`local/simple_server.py::_scan_inbound_once()`

- reads `<SCRAPEFLOW_MEDIA_ROOT>/待刮削`;
- registers direct child directories;
- calls `SimpleEngineRunner.create_pending_job()`;
- intentionally performs passive discovery even while globally paused.

Current durable registration:

`local/scrapeflow_api/simple_engine_runner.py::create_pending_job()`

- creates a job with phase `awaiting_target_shelf`;
- stores no plan;
- stores no target shelf/root;
- does not run archive/TMDB/planner/writer work.

Current start gate:

`SimpleEngineRunner.start_automatic_job()`

- is documented in code as the only ordinary-root transition that may enter `queued`;
- requires `movie`, `anime`, or `us_tv`;
- derives the fixed first-level target root;
- only then allows the scheduler to proceed.

Current scheduler:

`local/simple_server.py::_run_automatic_job()`

- returns immediately for `awaiting_target_shelf` and `target_policy_conflict`;
- also refuses ordinary jobs without all three persisted selection fields;
- calls `plan_automatic_job()` only after the shelf gate.

Therefore the current source implements:

```text
intake -> awaiting_target_shelf -> user shelf -> identity/planning -> write
```

It does **not** yet implement the desired:

```text
intake -> read-only identity + three-library reconciliation
       -> duplicate / existing-gap / merge-existing / new-work
       -> shelf confirmation only for genuine new work
```

## 4. Existing identity implementation should be reused, not replaced

`SimpleEngineRunner.resolve_automatic_request()` currently imports and calls the existing Engine identity path in `engine.scraper`, including:

- `_query_from_source`
- `_media_context_from_source_and_target`
- `auto_match_tmdb`
- `_season_from_source`

The underlying identity implementation lives in `engine/scrapeflow/identity_matching.py` and already contains substantial title normalization, query variants, aliases, media-context handling, TMDB matching and season inference.

Important implication for the reconciliation work:

- a new reconciliation coordinator may be needed;
- a new TMDB/title identity engine is not needed;
- move/refactor the shelf dependency out of the read-only identity path instead of creating another matcher.

## 5. Existing formal-library read-only machinery is reusable for reconciliation

`local/scrapeflow_api/simple_library_audit.py` already knows the three formal roots:

- `/quark/影视/电影`
- `/quark/影视/番剧`
- `/quark/影视/美剧`

It already contains read-only facilities that should be evaluated for reuse before adding a new scanner, including:

- complete inventory scanning;
- NFO identity parsing;
- `bootstrap_automatic_works_from_library()`;
- Engine-job work projection;
- identity keys and merge helpers;
- semantic gap construction;
- subtitle evidence handling.

`bootstrap_automatic_works_from_library()` explicitly projects formal-library NFO identities without creating Engine jobs or writing AList state.

This does not prove it alone is sufficient for the new intake reconciliation feature, but it is the first existing implementation Codex should reuse/extend.

## 6. Target-shelf module is useful; its placement in the workflow is the problem

`engine/scrapeflow/target_shelf.py` is a small, coherent module that provides:

- closed enum: `movie`, `anime`, `us_tv`;
- mapping from configured library root to `电影`, `番剧`, `美剧`;
- compatibility checks between selected shelf and Engine media type.

There is no reason to delete this module.

The obsolete behavior is the universal **start gate**, not the enum/root mapping itself.

The expected convergence is:

- existing work: derive shelf/root from the matched formal work;
- genuine new work: use this module for the user's bounded shelf confirmation.

## 7. Existing formal writer is real and should remain the only one

`SimpleEngineRunner` uses a cross-process worker lock (`worker_lock()`).

`execute_job()`:

- acquires that lock;
- reconstructs the persisted Engine plan;
- checks plan/problem/cleanup constraints;
- invokes the existing executor;
- persists the execution result.

`SimplePlanExecutor.execute()` performs the media moves/artifact writes and exact readback work.

Provider subtitle installation also calls `SimpleEngineRunner.install_subtitle_sidecar()`, which acquires the same worker lock.

No second formal writer is required for reconciliation.

## 8. Provider video delivery already returns through an Engine child

`local/scrapeflow_api/automatic_replenishment.py` already implements the desired broad topology:

```text
Provider acquisition
  -> task staging
  -> _plan_internal_child()
  -> engine_runner.plan_job(... internal_child_of=root_job_id)
  -> engine_runner.execute_automatic(child.id)
  -> same Engine runner / writer
```

The runtime also checks that an ordinary Provider child inherits the root job's selected shelf/root in the current target-shelf-first model.

Therefore the Provider ingest architecture should be preserved. The upcoming reconciliation change should adapt how the root obtains its authoritative shelf/work identity; it should not add another Provider ingest path.

## 9. Archive subsystem is already the lightweight version, not the old transaction framework

`engine/scrapeflow/archive.py` explicitly states that it does not own an AList transaction, receipt/digest/journal, remote decompression, or formal writer.

Confirmed current capabilities include:

- ZIP / 7z / RAR magic detection;
- disguised/self-extracting archive recognition without executing the file;
- 7-Zip based listing/extraction boundary;
- path traversal/absolute/drive/dot-segment validation;
- link rejection;
- collision checks;
- bounded member count/depth/expanded bytes/member bytes/archive bytes/expansion ratio/disk reserve;
- bounded password candidates and in-memory password handling;
- task-owned staging through `archive_preprocessing.py`.

This is already aligned with the lightweight product direction. Do not rewrite it during intake convergence.

## 10. Pause/restart semantics are not aligned yet

`docker-compose.yml` supplies `SCRAPEFLOW_START_PAUSED=1`, but runtime source does not use that environment variable to initialize control state.

`PersistentControlState` in `local/scrapeflow_api/control_state.py`:

- persists both pause and resume;
- treats a missing/invalid file as paused;
- returns an existing valid `paused=false` document as resumed.

`SimpleApplication.__init__()`:

- creates `PersistentControlState`;
- recovers persisted Engine records;
- starts `_resume_automatic_jobs()` in a startup thread.

`_resume_automatic_jobs()` will requeue resumable jobs when persisted control says unpaused.

Therefore a container/process restart can inherit a previous resumed control state. This conflicts with the desired local rule that every fresh API process starts effectively paused until the user explicitly resumes.

There is also a pause-semantics mismatch:

- ordinary automatic dispatch checks global pause before entering `_run_automatic_job()`;
- the formal executor's cooperative callback is based on the job cancel-request marker, not the global pause bit;
- Provider runtime has explicit cooperative global-pause checks at stage boundaries.

The target should be one simple definition: do not start the next external side effect while paused.

## 11. Quark external task recovery has most required pieces, but the top-level state blocks them

The current Quark magnet bridge already supports:

- `magnet-submit`;
- `magnet-status`;
- passing an existing `task_id` into `QuarkMagnetOfflineBridge.execute()` so it queries instead of submitting;
- persisting a returned task id in the attempt workspace.

However `AutomaticReplenishmentRuntime._run_request()` checks `_waiting_reconcile_gap_states()` before loading/reusing the active attempt. If any gap is `waiting_reconcile`, it immediately returns another `waiting_reconcile` result and does not re-enter the existing task-id-aware materializer path.

So the current defect is narrower than "Quark recovery is missing":

> the existing query/reconcile capability is present, but the `waiting_reconcile` orchestration branch prevents it from being reached.

Preferred fix: connect the durable known task id back into the existing query path; if no reliable id exists, expose a simple `needs_attention`/manual state. Do not create a new transaction framework.

## 12. Isolated acceptance root currently conflicts with Engine placement rules

`local/scrapeflow_api/provider_staging.py` explicitly permits an acceptance media root of:

```text
/quark/影视/ScrapeFlow/验收/<run-id>
```

and derives Provider staging below that root.

But `engine/scrapeflow/placement.py` hard-codes:

```text
MEDIA_ROOT = /quark/影视
UNSCRAPED_ROOT = /quark/影视/待刮削
REPLENISHMENT_ROOT = /quark/影视/ScrapeFlow/补源
CATEGORY_ROOTS = /quark/影视/{电影,番剧,美剧}
```

`validate_routing()` treats a path as production as soon as either source or target is anywhere under `/quark/影视`. Therefore an acceptance run nested below `/quark/影视/ScrapeFlow/验收/...` is classified as production but does not use the hard-coded production intake/category roots.

This confirms the previously reported isolation/path conflict.

This is a real current bug, but it should be fixed by making placement use the configured library-root contract rather than by adding another acceptance-specific validation layer.

## 13. One confirmed Compose/config mismatch remains

`engine/scrapeflow/media_quality.py` reads `SCRAPEFLOW_MIN_VIDEO_BYTES`.

`.env.local.example` documents that variable.

`docker-compose.yml` does not currently pass `SCRAPEFLOW_MIN_VIDEO_BYTES` into the API service.

Therefore changing it in `.env.local` does not affect the container unless Compose is corrected.

This is a small configuration alignment issue, not a reason to add a settings framework.

## 14. Local API protection is sufficient for the current product boundary; do not escalate it into enterprise auth

Current Compose publishes the API only on host loopback.

Inside the container the API listens on `0.0.0.0`, which is normal for Docker port publishing.

`SimpleHandler` currently applies loopback Host/Origin/Sec-Fetch checks to POST writes. GET endpoints are not protected by that same check.

For the declared single-user, loopback-only product this is not a P0 architecture problem. Keep the simple local-write protection. Revisit real authentication only if the product is intentionally exposed beyond localhost.

## 15. Acceptance/release machinery is real current code, not dead code

The repository currently contains active scripts/modules for:

- release checks;
- release evidence;
- isolated preflight;
- runtime readiness;
- acceptance package generation;
- offline backup verification.

The README and release checks actively reference several of these. Some also use SHA-512 for release/preflight file evidence, while active media SHA-256 calls are explicitly banned/scanned.

Given the new product priorities, this area is probably overbuilt relative to a local single-user tool, but it should **not** be deleted blindly during the intake refactor.

Recommended treatment:

1. freeze new acceptance/preflight/readiness layers;
2. fix the core product flow first;
3. later run a dedicated simplification task and remove only machinery that no longer protects a real local failure mode or release operation.

## 16. Tests currently encode the obsolete product order

There are 39 `local/tests/test_*.py` files in the supplied snapshot.

In particular, `local/tests/test_target_shelf_start_gate.py` explicitly asserts that:

- pending registration performs no Engine/TMDB/planner work;
- every ordinary root remains `awaiting_target_shelf` until `/start` selects a shelf;
- legacy automatic roots cannot execute without that selection.

`test_simple_server.py`, `test_phase4_golden_path.py`, and recovery tests also call `start_automatic_job()` before normal planning.

These tests are valid evidence of the current implementation but conflict with the new product contract. When intake reconciliation is implemented, the affected tests must be rewritten to assert the new order rather than used as a reason to preserve the obsolete gate.

## 17. Documentation authority was stale in the audited baseline

At the audited baseline, `README.md` and `ARCHITECTURE.md` both described target-shelf-first as the main flow.

At that time, `docs/scrapeflow-final-convergence-plan-v1.md` also declared itself the frozen/only contract and listed `awaiting_target_shelf -> /start` as a reusable baseline.

At the same time, the older `docs/scrapeflow-single-user-local-convergence-plan.md` contains several lightweight principles that are still useful and already implemented, including:

- single user/local/single writer;
- no application-level media SHA-256;
- no receipt/digest/nonce/epoch transaction system;
- reuse old business/safety kernels rather than restoring old runtimes;
- archive/path/no-overwrite/ffprobe safety;
- Python unittest instead of another test framework.

This documentation-authority convergence has now been completed: root `AGENTS.md` is the long-lived contract, this file remains the implementation-fact snapshot, and target-shelf-first plans/RC/audits are historical references. The current source described in sections 3 and 16 still has the obsolete target-shelf-first implementation gap.

## 18. Recommended convergence order from this baseline

Do not start with a repository-wide rewrite.

### Task 1 - documentation authority only (completed)

- root `AGENTS.md` is the long-lived contract;
- this file is the current implementation-fact snapshot;
- `ARCHITECTURE.md` and README state the reconciliation-first target unambiguously;
- old target-shelf-first contract sections are historical/superseded;
- no business-code change was made for this task.

### Task 2 - read-only reconciliation entry

- keep passive intake discovery;
- replace the universal `awaiting_target_shelf` business gate with a read-only reconciliation stage;
- reuse Engine identity and formal-library audit/NFO identity facilities;
- produce bounded outcomes: duplicate-complete / existing-gap / merge-existing / new-work / uncertain;
- do not write the formal library or submit Providers in this stage.

### Task 3 - move shelf confirmation to new-work only

- retain `target_shelf.py` as the closed shelf policy;
- existing matched works inherit their formal shelf/root;
- only new works wait for `movie/anime/us_tv` confirmation when needed;
- adapt/update target-shelf tests accordingly.

### Task 4 - converge execution branches

- duplicate: safe task-owned input cleanup only when proven;
- existing gap: Provider against the existing work;
- merge-existing: existing Engine plan + same writer;
- new work: existing Engine + same writer;
- uncertain: stop for user attention.

### Task 5 - pause/restart simplification

- every process start effectively paused;
- keep job persistence, do not persist authorization to resume across a new process;
- use one pause boundary meaning across ordinary and Provider flows.

### Task 6 - Quark waiting-reconcile connection

- known task id re-enters the existing status/query path;
- no duplicate submit;
- missing/ambiguous id becomes simple user attention.

### Task 7 - path/config cleanup

- make Engine placement honor configured library roots, fixing isolated-root conflict without another acceptance layer;
- pass `SCRAPEFLOW_MIN_VIDEO_BYTES` through Compose;
- then reassess whether isolated acceptance tooling can be simplified.

### Task 8 - dedicated complexity deletion

Only after the core flow works:

- inventory release/preflight/readiness/acceptance code by real caller and real local value;
- remove redundant layers/dead scripts/tests;
- do not touch archive, Engine, writer, or Provider code merely because it looks large.

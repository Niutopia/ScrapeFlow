# ScrapeFlow Codex Working Contract

## 1. Authority

This file defines ScrapeFlow's long-lived product and engineering rules.
Before changing code, also read `docs/CURRENT-STATE.md` if it exists.

- `AGENTS.md` = target behavior and Codex constraints.
- `docs/CURRENT-STATE.md` = facts about the current HEAD and known gaps.
- Current code/tests are implementation evidence, not product authority when they conflict with this file.
- Historical RC, audit, acceptance, convergence, or target-shelf documents do not override this file merely because they call themselves "final" or "only" contracts.

If the user's latest explicit instruction conflicts with this file, report the conflict instead of silently inventing a new architecture.

## 2. Product boundary

ScrapeFlow is a personal media-ingest and replenishment tool:

- one user;
- one local machine;
- Docker Compose deployment;
- one AList instance;
- one ScrapeFlow API application;
- one formal-library writer;
- lightweight local state.

It is not a multi-user, multi-tenant, distributed, enterprise, or public-cloud platform.

Priority order:

1. correct media behavior;
2. no accidental formal-library writes/overwrites;
3. reuse existing mature code;
4. simple control flow and manual recovery;
5. maintainability;
6. only then abstraction/extensibility.

Do not add complexity for hypothetical future scale.

## 3. Target product flow

Ordinary intake must converge to:

```text
待刮削输入
  -> read-only identity + reconciliation against 电影 / 番剧 / 美剧
  -> one result:
       duplicate_complete
       existing_gap
       merge_existing
       new_work
       uncertain
```

Required behavior:

- `duplicate_complete`: do not create another formal work; clean only task-owned input after identity/completeness is sufficiently certain.
- `existing_gap`: the existing formal work determines shelf/work root; replenish only the confirmed gap; do not ask for a new shelf.
- `merge_existing`: merge new valid media through the existing Engine and the same writer; do not create a duplicate work.
- `new_work`: reuse the original Engine for TMDB identity, media type, season/episode, naming and concrete work path; ask for `movie / anime / us_tv` only when a genuine new work still needs first-level shelf confirmation.
- `uncertain`: stop safely and expose a blocked/needs-attention state; do not guess identity, shelf, target path, deletion, or Provider fallback.

After a formal write:

```text
single writer
  -> exact AList readback
  -> NFO / artwork / subtitle handling
  -> current-work residual/audit checks
```

After intake is cleared/settled, run the full-library review read-only. Confirmed gaps may then enter Provider replenishment.

## 4. Read-only work is allowed before shelf confirmation

Shelf confirmation is not a gate for harmless analysis.
Before any formal write or new Provider submission, ScrapeFlow may:

- browse intake and formal shelves;
- read AList metadata/NFO data;
- run existing media inspection/ffprobe logic;
- query TMDB through existing Engine identity code;
- compare intake identity with formal-library works;
- build a reconciliation result or plan.

The hard boundary is **external side effects**, not "no Engine call before target shelf".
Do not preserve or introduce a rule requiring every ordinary input to choose a shelf before read-only identity/reconciliation.

## 5. Existing authorities: reuse, do not clone

### Identity and planning

Reuse:

- `engine/scrapeflow/identity_matching.py`
- `engine/scraper.py`
- existing planning modules under `engine/scrapeflow/`

Do not create a second title normalizer, TMDB matcher, season/episode identity engine, or naming engine for reconciliation.
A reconciliation coordinator may organize the three formal shelves, but identity remains Engine-owned.

### Formal-library read-only inventory/audit

Before adding a new scanner, inspect/reuse `local/scrapeflow_api/simple_library_audit.py`, especially its formal-root inventory, NFO identity projection, work projection and gap helpers.
Reconciliation must not become a second full-library audit implementation.

### Target shelf

Keep `engine/scrapeflow/target_shelf.py` for:

- the closed `movie / anime / us_tv` enum;
- mapping a configured library root to the three first-level shelves;
- media-type/shelf compatibility.

Do not use it as the universal gate that blocks all ordinary identity/reconciliation.

### Formal writer / Engine runner

Reuse `local/scrapeflow_api/simple_engine_runner.py`, its existing executor, worker lock, readback and subtitle-write boundary.
All formal-library writes must continue through this single writer.
Do not add a reconciliation writer, Provider writer, archive writer, recovery writer, or audit writer.

### Archive

Reuse:

- `engine/scrapeflow/archive.py`
- `engine/scrapeflow/archive_preprocessing.py`

Do not create another archive framework and do not restore historical transaction/receipt/digest wrappers around the current archive safety kernel.

### Provider

Reuse:

- `local/scrapeflow_api/automatic_replenishment.py`
- `local/scrapeflow_api/provider_staging.py`
- existing Engine-child execution in `simple_engine_runner.py`.

Provider video must remain:

```text
Provider -> task staging -> existing Engine child -> same writer -> formal library
```

Do not add a Provider-specific formal ingest engine.

### Quark Helper

Keep the current narrow Docker sidecar and existing bridge/helper code.
The Helper is a Quark adapter, not a business planner and never decides formal-library paths.

## 6. Provider contract

Tier order is fixed:

```text
Quark share -> Quark magnet offline -> local Torrent
```

- Advance to a lower tier only after the previous tier has genuinely failed/exhausted as a resource candidate.
- Infrastructure/auth/helper/network failure is not "resource absent" and must not silently advance the tier.
- Provider output lands in task-owned staging first.
- Provider never directly writes or chooses the formal-library path.

## 7. Quark external task recovery

```text
known external_task_id -> query/reconcile that task
no task id             -> submit only when state permits a new submission
submit outcome unknown -> needs_attention/manual recovery
```

Never resubmit merely because the API restarted.
The repository already has task-id-aware Quark status/query paths; connect and reuse them.
Do not build an exactly-once/distributed-transaction framework for this edge case.

## 8. Pause and restart

Target behavior:

- every API process/container start begins effectively paused;
- job records survive restart;
- a previously persisted `resumed` value does not authorize new formal work after a fresh process start;
- the user explicitly resumes after startup.

Pause means:

> Do not start the next external side effect.

An already submitted non-atomically-cancellable operation may finish, but check pause again before the next move/upload/delete/Provider submit/child write boundary.
Use one pause meaning for ordinary and Provider jobs.
Do not add another control-state layer, lease, journal, or scheduler framework.

## 9. Docker contract

Normal Compose topology remains exactly:

1. `alist`
2. `api`
3. `quark-helper`

The API is a modular monolith. Reconciliation, Engine, archive, writer, audit, Provider and state are code modules, not new containers.

Do not add worker/scheduler/auditor/provider/auth/gateway/state/reconciliation services without an explicit user-approved runtime-isolation reason.

Normal host exposure stays loopback-only. Do not introduce enterprise API authentication/RBAC while that product boundary remains true. Keep the existing minimal local/same-origin write protection.

## 10. Local safety that must remain

Keep:

- source/staging/formal-library path boundaries and non-overlap;
- target exists => stop, no overwrite by default;
- archive path traversal and link rejection;
- bounded archive member count/depth/expanded size/ratio/disk reserve;
- disguised executable recognition without executing it;
- minimum video-size admission;
- existing ffprobe/media readability checks;
- task-owned cleanup only;
- secret/password/cookie redaction from normal logs/job state;
- exact AList path/type/size readback after formal writes.

Do not weaken these to make the code smaller.

## 11. Complexity that must not be reintroduced

Unless the user explicitly changes product scope, do not add/restore:

- application-level whole-media SHA-256 verification;
- plan/content receipts or approval digests;
- nonce/epoch/lease transaction proof;
- two-phase commit or formal rollback stores;
- distributed locks/leader election;
- message queues for local orchestration;
- multi-writer/multi-instance coordination;
- generic Provider plugin platforms;
- a database/state platform solely to replace working lightweight local state;
- enterprise audit trails or RBAC;
- continuous production E2E/smoke infrastructure.

Do not mechanically remove every `hash`, `manifest`, `infohash`, `CRC`, or `digest` occurrence. Protocol-native identifiers, 7-Zip CRC, HMAC comparison, Git hashes and small release-file hashes are different concerns. The prohibited design is application-level media transaction proof.

## 12. Rare failures: stop instead of building another framework

Preferred model:

```text
normal path -> automate
rare/ambiguous path -> stop safely -> tell the user
```

Use existing/simple states (`failed`, `blocked`, `retry_wait`, `needs_attention`) where practical.
Do not create a new state-machine framework just to remove manual handling of a low-probability local failure.

## 13. Codex runtime safety

Unless the user explicitly asks otherwise:

- do not read `.env.local` or real secrets;
- do not inspect/modify `.runtime/`, `.scrapeflow/`, `state/`, `backups/`, or real media content for routine source changes;
- do not start Docker Compose;
- do not contact real AList/TMDB/Quark/PanSou/aria2 services;
- do not run destructive Git commands;
- do not modify runtime state or media files.

Use source inspection, fake/in-memory clients and focused tests by default.

## 14. Tests

Use the existing Python `unittest` approach unless explicitly told otherwise.
Tests are guards, not product authority.

When tests encode the obsolete target-shelf-first flow, update them with the product change rather than preserving obsolete behavior just to keep tests green.

Prioritize tests for:

- the five reconciliation outcomes;
- identity reuse/no duplicate work creation;
- shelf confirmation only for genuine new works;
- path/no-overwrite boundaries;
- single-writer execution;
- pause at external-side-effect boundaries;
- Provider tier order;
- known external task IDs queried, not resubmitted;
- archive safety regressions.

Do not add another acceptance/preflight/readiness/evidence layer while the core product flow is being corrected.
Existing acceptance/release tooling may remain until a dedicated simplification task proves what can be removed safely.

Passing unit tests, `docker compose config`, or image build is not real AList/Quark/media end-to-end acceptance.

## 15. Required Codex workflow

Before editing:

1. read this file and `docs/CURRENT-STATE.md`;
2. search the whole repository for the requested capability;
3. trace the current API -> Engine/Provider/writer/state call path;
4. identify the existing authority that should win;
5. choose the smallest change toward this contract.

During editing:

- prefer extending/calling existing code;
- do not create a parallel manager/service/framework when an existing module can own the behavior;
- keep diffs scoped;
- do not opportunistically rewrite unrelated modules;
- do not split the API into new services;
- do not rewrite the original Engine to implement reconciliation.

Before adding any new abstraction, class, service, validator, state machine, scheduler, persistence format, or Docker service, explicitly verify why an existing implementation cannot be extended.

After editing:

- run focused tests, plus broader local unit tests only when a shared core contract changed;
- do not use real credentials/media for routine verification;
- report: root cause, files changed, existing code reused, duplicate implementation avoided/removed, tests run, and unresolved task-specific issues.

## 16. Documentation

After this contract is adopted:

- `AGENTS.md` is the Codex product/engineering instruction authority.
- `ARCHITECTURE.md` describes the target architecture and must not preserve the obsolete universal target-shelf-first entry flow.
- `docs/CURRENT-STATE.md` records current HEAD facts/gaps and is updated when they materially change.
- older convergence/RC/acceptance documents are historical unless explicitly reconciled with this contract.

Web UI is not a current backend completion requirement unless the user explicitly starts a Web task.

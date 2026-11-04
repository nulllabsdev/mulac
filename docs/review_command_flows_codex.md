# Command flow architecture review

## Scope and conclusion

Review date: 2026-11-04. Reviewed commit:
`01f523be7e5a716e7c05a5e3b6f81baec25f83d3` (`wip`), including its changes
relative to its parent and their effect on the Poem, Axum, and application-worker
flows. Three independent reviewers each examined all three flows; this report
consolidates their findings and the primary reviewer's verification.

**The commit resolves the main over-layering concern.** It removes the
synchronous `CommandingService` object and its always-successful outer `Result`.
The HTTP paths now have one execution bridge. The shared execution core remains
appropriately separated. No new runtime correctness regression was identified
in the reviewed change or the targeted checks below.

The earlier recommendation to remove or simplify `CommandingService` is
**resolved**, not an outstanding action. A separate helper is not necessary
merely to replace the deleted object: the new captured closure is small and
coherent.

## Commit message

**`wip`** does not follow the repository's Conventional Commits convention or
describe the change. A suitable replacement when finalizing history is:

```text
refactor(commanding): merge http execution bridges
```

The public Rust API break also needs a clear breaking-change description in the
release/commit body. ADR-013 and the changelog already acknowledge it. This
review does not rewrite the commit.

## Content review

### What changed

- `AsyncCommandingService` captures `Arc<CommandConsumer<R>>` directly instead
  of capturing a synchronous service. Its closure constructs the reservation
  specification, calls the consumer, and converts the result to
  `RunCommandsResponse`.
- The closure returns `RunCommandsResponse`, not an always-`Ok` nested result.
  The async method's `Result` represents the actual blocking-task failure.
- The execution module is gated by `api-tokio`; `api` retains contracts and
  errors, while `api-testing` supplies consumers without a Tokio dependency.
- Public fixtures change from `*_service` to `*_consumer`. Examples, adapter
  tests, documentation, diagrams, and architectural decisions are updated.
- The worker, consumer, dispatcher, registry, and typed handler wrapper are
  unchanged.

Sources: [execution bridge](../libs/commanding/src/http/execution.rs#L39),
[feature boundary](../libs/commanding/src/http/mod.rs#L27),
[ADR-013](adr/013-merge-commanding-execution-bridges.md), and
[changelog](../CHANGELOG.md).

### What is done well

The change removes an object without removing its necessary behavior. Blocking
execution, repository-type erasure, response formatting, and sanitized task
failures remain in the HTTP bridge. Request validation remains at the HTTP
boundary. Queue lifecycle policy does not acquire HTTP dependencies.

Poem and Axum still report consumer failures as HTTP 200 batch responses and
task failures as sanitized HTTP 500 errors. Synchronous consumers still support
borrowed repositories and do not need a runtime. The worker correctly continues
to consume directly rather than going through HTTP request/response types.

### Commit and migration findings

1. **Medium: provide an explicit synchronous migration example.** The documented
   direction to use `CommandConsumer::consume` is correct, but it is not a
   drop-in equivalent to the removed HTTP-shaped service. The consumer accepts
   a reservation specification and returns raw batch errors. A caller wishing
   to preserve the previous validation and response policy should use:

   ```rust
   let request = RunCommandsRequest::new(limit)?;
   let result = consumer.consume(&ReservableCommandSpec::new(request.limit()));
   let response = RunCommandsResponse::from_result(result);
   ```

   These types are available through `commanding::prelude`. Using
   `consumer.consume(...)?` instead would propagate batch errors rather than
   encode them in the response. Using an arbitrary specification also does not
   preserve the HTTP limit validation automatically. These are downstream
   migration risks, not defects in the new bridge.

2. **Low: replace the nonconforming commit subject when finalizing history.**
   See the commit-message section above.

The removed `CommandingService`, changed async constructor, and renamed public
fixtures are intentional source-breaking changes, not accidental regressions.
They require downstream Rust migration; passing HTTP tests do not imply Rust
source compatibility. Commanding now differs from the other queues' service
construction, as ADR-013 explicitly records. Do not restore the redundant layer
solely for symmetry.

## Updated assessment of all three flows

| Flow   | Current assessment                                                                                                                                                               |
| ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Poem   | Endpoint and adapter form one framework boundary. The adapter performs real validation, schema conversion, and error translation. One async execution bridge calls the consumer. |
| Axum   | Extractor and response/error wrappers own framework behavior. The adapter itself remains a thin unwrap/await/wrap convenience. One async execution bridge calls the consumer.    |
| Worker | Poll/cancel, execute a blocking batch, log, repeat. Already a lean driver; keep it independent of HTTP contracts.                                                                |

Request, closure, resume, and return boxes in the diagrams represent execution
phases, not necessarily separate objects. The closure boxes now belong to
`AsyncCommandingService`; they do not represent a surviving synchronous service.

### Boundaries to preserve

- **Consumer:** reservation, sequential batch processing, reservation identity,
  completion/failure updates, and error collection.
- **Dispatcher:** execution of a command and dispatch of its resulting events.
  The [direct gateway](../libs/commanding/src/application/command_gateway.rs#L26)
  also uses it without durable consumption. Folding it into the consumer would
  couple direct execution to queue mechanics or duplicate orchestration.
- **Registry:** command-type selection and missing-handler detection.
- **Typed wrapper:** command deserialization, event serialization, sequence
  validation, event identity, and correlation/causation metadata.
- **Async boundary:** offloading synchronous storage/handler work and handling
  task failure. Its responsibility remains necessary after the merge.

### Remaining concerns not introduced by this commit

1. **Worker lifecycle ownership deserves priority.** The sample application
   discards its worker handles, while `PersistentKernelHandle::wait()` cancels
   its token without joining those tasks. If graceful batch draining is
   intended, the application must retain and await the handles, or the
   spawning component must own them. Do not add a generic executor merely to
   address ownership. See [application startup](../test_apps/twitter/src/assembly/bin/twitterapp.rs#L62)
   and [kernel wait](../kernel/src/lib.rs#L536).

2. **Axum's adapter remains optional convenience rather than substantive
   execution policy.** Its [two-line method](../libs/commanding/src/http/axum/adapter.rs#L80)
   could live in a route using the async service directly as state. However,
   custom OpenAPI routes already reuse the adapter. Reviewers differed on
   whether removing the object is worth repeating those conversions in callers.
   Retaining it initially is reasonable; it is not an urgent correctness fix.

3. **Poem's adapter is more useful than Axum's.** It performs framework-owned
   request/response/error conversions and supports host-composed endpoints.
   Treat endpoint and adapter as one conceptual framework boundary rather than
   forcing both frameworks into identical object structures.

4. **Duplicate contract policy can drift.** Poem's shared request and the
   commanding request both validate the limit, and related response shapes are
   maintained separately. Centralize ownership before removing checks: Rust
   callers can construct wire types without going through HTTP extraction.
   Preserve schema identity when considering consolidation.

5. **The blocking-pool test does not establish its named guarantee.**
   `runs_batches_on_blocking_pool` checks the returned count, not which thread
   executed the work. A thread-observing fixture would strengthen regression
   protection. This weakness predates the commit; the current implementation
   explicitly uses `spawn_blocking`.

Handler effects, event dispatch, and outcome updates remain separate operations.
Removing forwarding calls does not make them transactional or eliminate
duplicate effects on retry. No performance improvement is claimed without
measurement.

## Verification

The following targeted checks passed offline at the reviewed commit:

| Check                                            | Result                                |
| ------------------------------------------------ | ------------------------------------- |
| Commanding, no defaults, `api`                   | Build passed                          |
| Commanding, no defaults, `api-testing`           | Build passed; 35 library tests passed |
| Commanding, no defaults, `api-tokio,api-testing` | 41 library tests passed               |
| Axum commanding adapter integration              | 4 tests passed                        |
| Poem commanding adapter integration              | 8 tests passed                        |
| Axum shared OpenAPI schemas                      | 5 tests passed                        |
| Normal `api-testing` dependency tree             | No Tokio, Poem, or Axum dependency    |

The two library test configurations overlap; their counts are not distinct test
cases. Existing warnings in eventing/inbox were observed. This was not a full
workspace, database-backed, shutdown, or performance test run.

Primary test commands:

```bash
cargo test --offline --manifest-path libs/Cargo.toml \
  -p mulac-commanding --no-default-features \
  --features api-tokio,api-testing --lib

cargo test --offline --manifest-path test_apps/http_adapters/Cargo.toml \
  --test commanding_axum --test commanding_poem --test axum_schema
```

## Overall

**Keep this simplification.** The strongest earlier recommendation is now
implemented, and the tested HTTP contracts remain intact. Next, document the
synchronous migration precisely and address worker lifecycle ownership. Further
adapter flattening is an API ergonomics decision, not a reason to collapse the
shared execution core. No implementation changes are made by this review.

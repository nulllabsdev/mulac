# ADR-011: API and HTTP features on core crates

**Status:** Accepted; execution bridges amended by ADR-013 (commanding) and ADR-014 (other queues)

Supersedes the crate layout in ADR-009 and ADR-010, not their HTTP behavior.

## Decision

The commanding, eventing, inbox and outbox crates own their services, contracts,
errors, async wrappers, fixtures and framework adapters. The four `*_api`
crates and `mulac_axum`/`mulac_poem` are removed without compatibility shims.

All four cores use this feature matrix:

| Feature        | Capability                                              |
| -------------- | ------------------------------------------------------- |
| default        | `diesel`, `api`                                         |
| `api`          | synchronous services, contracts and API errors          |
| `api-tokio`    | `api` plus caller-runtime `spawn_blocking` wrappers     |
| `api-testing`  | `api` plus fixtures and port-based doubles              |
| `axum`         | `api-tokio` plus this queue's Axum adapter              |
| `axum-openapi` | `axum` plus Utoipa schemas                              |
| `poem`         | `api-tokio` plus this queue's Poem/poem-openapi adapter |

Existing `diesel` and `amqp` features retain their semantics. HTTP features do
not enable either. Disabling defaults is required for a database-free build.
Frameworks are additive and independent, and no queue forwards its adapter
features to another queue. Tokio used by AMQP does not imply `api-tokio`.

Implementation modules are private. Services are exported through
`crate::prelude`, adapters through `crate::prelude::axum` and
`crate::prelude::poem`, and optional fixtures through `crate::prelude::testing`.

Within commanding, the private `http` module groups the shared contracts,
errors, execution bridge, fixtures, and private `axum`/`poem` adapters.
`http::execution` owns request validation, response conversion, and optional
Tokio blocking adaptation; the application layer's `CommandConsumer` remains
the command execution use case. The synchronous `CommandingService` and asynchronous
`AsyncCommandingService` remain available through the public prelude for Rust
callers. This internal organization does not change the `api*` feature names,
public imports, or HTTP behavior and does not add another application layer.

`mulac_http` has no queue, kernel, database or broker dependencies. Its default
feature set is empty; `axum`, `axum-openapi`, and `poem` activate only framework
support. It owns shared run schemas/body types and Axum body/rejection helpers,
not services or queue adapters. This single schema owner prevents Poem from
panicking when multiple distinct Rust types claim the same OpenAPI name.
Inbox-specific Poem submission types remain in inbox. Queue-specific API
contracts remain in their cores; adapters convert between these and shared
HTTP types. Shared Poem types are re-exported by every core's Poem prelude.

## Compatibility and migration

This is a breaking Rust API change. For example:

```toml
commanding = { package = "mulac-commanding", version = "0.3.0", registry = "margo", default-features = false, features = ["axum-openapi"] }
```

```rust
use commanding::prelude::{CommandingService, AsyncCommandingService};
use commanding::prelude::axum::CommandingAdapter;
```

Replace `*_api::prelude` with the matching core prelude. Replace
`mulac_axum::prelude` or `mulac_poem::prelude` adapter imports with the matching
core's framework prelude. Replace old API features `tokio` and `testing` with
`api-tokio` and `api-testing`. Replace adapter-crate queue features with a
framework feature on each queue dependency. Both frameworks may coexist.
Poem run wrappers now contain neutral `RunRequestData`/`RunResponseData`, not
inbox API contracts; these data types are also available via the Poem prelude.

HTTP paths, JSON bodies, validation text, statuses, and the shared
`RunRequest`/`RunResponse` OpenAPI names remain unchanged. Runtime and publisher
ownership remain with the application. No migrations are required. The example
version is the current development version, not a promise of registry release;
publishing and the breaking-change version bump are separate release actions.

## Verification

`make ci` runs core/kernel tests and the database-free HTTP integration harness,
including both frameworks' four-queue schema composition tests. It compiles each
core's feature matrix and checks normal dependency trees for framework, runtime,
database and broker leakage. `test_apps/http_adapters` contains the migrated
adapter behavior tests. Database and broker integration tests remain opt-in.

# Agent Guidelines

These guidelines apply to all AI code assistants working in this repository.

## Feature Module Structure

Every feature module throughout the Mulac project must expose a single `pub mod prelude` as its only public interface. All internal implementation sub-modules (`commanding`, `eventing`, `http`, `infra_sqlx_pg`, etc.) must be declared without `pub` so they remain private to the feature.

```rust
mod commanding { ... }    // private: command structs and handlers
mod eventing { ... }      // private: event structs and subscribers
mod infra_sqlx_pg { ... } // private: database queries
mod http { ... }          // private: HTTP API handlers and request types

pub mod prelude {         // public: re-exports only
    pub use super::commanding::{MyCommand, MyHandler};
    pub use super::eventing::{MyEvent, MySubscriber};
    pub use super::infra_sqlx_pg::my_query;
    pub use super::http::{Api, MyRequest};
}
```

Callers must import through `feature::prelude::*`, never through internal sub-modules like `feature::commanding::*` or `feature::http::*`. This convention applies across libraries, kernel, and test applications, including outbox and outbox_api. Standard-library and third-party I/O modules (such as `std::io`) are unaffected.

## Conventional Commits

All commits must follow the [Conventional Commits](https://www.conventionalcommits.org/) format.

Format: `<type>(<scope>): <subject>`

- **type**: feat, fix, docs, style, refactor, perf, test, chore, ci
- **scope**: optional, e.g., library name or component
- **subject**: lowercase, imperative, no period

Example: `feat(kernel): add event handler initialization`

## LLM Attribution

If an LLM only committed code (did not write the code itself), it should only be attributed for the commit message, not as a co-author. Omit the `Co-authored-by` trailer in such cases.

If an LLM is attributed as `Author`, `CoAuthor`, or `Committer`, use the format `{toolname}::{model}::{effort}`.

Do not include an email address in LLM attribution.

Never use the `Co-authored-by:` trailer for LLM attribution. Use only `Author:`, `CoAuthor:`, or `Committer:`.

Use the following rules to choose the correct trailer:

| Situation                                                     | Required trailer(s)                        | Notes                                                                            |
|---------------------------------------------------------------|--------------------------------------------|----------------------------------------------------------------------------------|
| LLM wrote the change and created the commit                   | `Author: {toolname}::{model}::{effort}`    | Use when the LLM produced the substantive code or documentation being committed. |
| LLM helped write the change and a human is the primary author | `CoAuthor: {toolname}::{model}::{effort}`  | Use for material contribution without making the LLM the primary author.         |
| LLM only created the commit for human-written code            | `Committer: {toolname}::{model}::{effort}` | Use only for commit mechanics.                                                   |
| LLM only suggested the commit message text                    | no trailer                                 | No attribution is required.                                                      |

Use the model ID in the `model` field, for example `gpt-5.4`.

Use one of the following values in the `effort` field: `low`, `medium`, `high`.

Examples:

```text
docs(dx): clarify llm attribution format

CoAuthor: copilot-cli::gpt-5.4::medium
```

```text
chore(repo): add cargo make targets

Author: copilot-cli::gpt-5.4::medium
```

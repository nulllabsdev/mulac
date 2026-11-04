# TODO

- [ ] `ports.rs:18`: Move the traits declared at the module root into a private submodule and re-export them through `prelude`, making `prelude` the module's only public interface.
- [ ] `libs/commanding/src/execution/command_dispatcher.rs:65`, `libs/inbox/src/application/conversions.rs:57`: Stop generating new event and command IDs on retry, which defeats dedupe. Event identity needs an explicit design, not a positional UUID. See finding 2 in `docs/review_libraries_unified.md`.

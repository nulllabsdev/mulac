//! Item-local unwind containment. Never place the whole batch inside this boundary.
pub mod prelude {
    #[cfg(feature = "diesel")]
    pub(crate) use super::guard;
    pub(crate) use super::run_item;
}

use std::fmt::Display;
use std::panic::{AssertUnwindSafe, catch_unwind};
use uuid::Uuid;

/// The supplied closure owns its per-call state. Shared collaborators must remain
/// usable after unwinding; catching a panic does not undo their side effects.
pub(crate) fn guard<T, E>(
    operation: impl FnOnce() -> Result<T, E>,
    panic_error: impl FnOnce() -> E,
) -> Result<T, E> {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(result) => result,
        Err(payload) => {
            // A user-provided panic payload may itself panic when dropped.
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                std::mem::forget(secondary);
            }
            Err(panic_error())
        }
    }
}

pub(crate) fn run_item<E: Display>(
    identity: (Uuid, Uuid),
    work: impl FnOnce() -> Result<(), E>,
    complete: impl FnOnce() -> Result<(), E>,
    fail: impl FnOnce(String) -> Result<(), E>,
    panic_error: impl Fn(String) -> E,
    errors: &mut Vec<E>,
) {
    let (id, reservation_id) = identity;
    let context = |stage: &str| format!("item {id}, reservation {reservation_id}, stage {stage}");
    let mut stage = "processing";
    let result = guard(work, || {
        panic_error(format!("{}: panicked", context(stage)))
    });
    let result = result.and_then(|()| {
        stage = "completion";
        guard(complete, || {
            panic_error(format!("{}: panicked", context(stage)))
        })
    });
    if let Err(error) = result {
        let reason = format!("{}: {error}", context(stage));
        // Completion may have committed despite an acknowledgement error. The
        // repository's ownership predicate must never overwrite that outcome.
        if let Err(finalization_error) = guard(
            || fail(reason),
            || panic_error(format!("{}: panicked", context("failure finalization"))),
        ) {
            errors.push(finalization_error);
        }
        errors.push(error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_panicking_payload_destructor_without_exposing_payload() {
        struct BadPayload;
        impl Drop for BadPayload {
            fn drop(&mut self) {
                std::panic::panic_any("private destructor details");
            }
        }
        let result: Result<(), &str> =
            guard(|| std::panic::panic_any(BadPayload), || "item panicked");
        assert_eq!(result, Err("item panicked"));
        assert_eq!(guard(|| Ok::<_, &str>(42), || "unexpected"), Ok(42));
    }
}

//! Reports failures to HTTP clients by ID and logs the real error under that ID.
//!
//! Responses therefore reveal nothing about storage, brokers or internal types,
//! and operators can find the full error by searching their logs for the ID. The
//! host must install a `tracing` subscriber, or the IDs lead nowhere.
pub mod prelude {
    pub use super::{ReportedError, report_batch_error};
}

use std::error::Error;
use std::fmt::{self, Display};
use uuid::Uuid;

type Cause = Box<dyn Error + Send + Sync>;

/// A failure shown to HTTP clients only by its ID.
///
/// The cause is logged once, on creation, and stays available as
/// [`Error::source`] for Rust callers. `Display` writes only the ID.
#[derive(Debug)]
pub struct ReportedError {
    id: Uuid,
    cause: Cause,
}

impl ReportedError {
    /// Report a server failure, logged at `error`.
    pub fn server(queue: &'static str, cause: impl Into<Cause>) -> Self {
        let report = Self::new(cause.into());
        tracing::error!(error_id = %report.id, queue, error = %report.cause, "HTTP request failed");
        report
    }

    /// Report a request the client must correct, logged at `warn`.
    pub fn client(queue: &'static str, cause: impl Into<Cause>) -> Self {
        let report = Self::new(cause.into());
        tracing::warn!(error_id = %report.id, queue, error = %report.cause, "HTTP request rejected");
        report
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    fn new(cause: Cause) -> Self {
        Self {
            id: Uuid::now_v7(),
            cause,
        }
    }
}

impl Display for ReportedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}

impl Error for ReportedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

/// Log one failed batch item at `error` and return the ID for the response.
pub fn report_batch_error(queue: &'static str, error: impl Display) -> String {
    let id = Uuid::now_v7();
    tracing::error!(error_id = %id, queue, error = %error, "HTTP batch item failed");
    id.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing::Level;

    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);

    impl Write for Logs {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Run `report` with logging captured, returning its log output.
    fn captured(report: impl FnOnce()) -> String {
        let logs = Logs::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, report);
        String::from_utf8(logs.0.lock().unwrap().clone()).unwrap()
    }

    #[test]
    fn display_shows_only_the_id_and_source_keeps_the_cause() {
        let report = ReportedError::server("test", "secret dsn");
        assert_eq!(report.to_string(), report.id().to_string());
        assert!(!report.to_string().contains("secret"));
        assert_eq!(report.source().unwrap().to_string(), "secret dsn");
    }

    #[test]
    fn server_failures_are_logged_at_error_under_their_id() {
        let mut id = String::new();
        let logs = captured(|| id = ReportedError::server("test", "secret dsn").to_string());
        assert!(logs.contains("ERROR"), "{logs}");
        assert!(logs.contains(&id), "{logs}");
        assert!(logs.contains("secret dsn"), "{logs}");
        assert!(logs.contains("queue=\"test\""), "{logs}");
    }

    #[test]
    fn client_failures_are_logged_at_warn_under_their_id() {
        let mut id = String::new();
        let logs = captured(|| id = ReportedError::client("test", "bad body").to_string());
        assert!(logs.contains("WARN"), "{logs}");
        assert!(logs.contains(&id), "{logs}");
        assert!(logs.contains("bad body"), "{logs}");
    }

    #[test]
    fn batch_errors_get_distinct_logged_ids() {
        let mut ids = vec![];
        let logs = captured(|| {
            ids = ["first", "second"]
                .map(|error| report_batch_error("test", error))
                .to_vec();
        });
        assert_ne!(ids[0], ids[1]);
        for (id, error) in ids.iter().zip(["first", "second"]) {
            assert!(Uuid::parse_str(id).is_ok(), "{id}");
            let line = logs.lines().find(|line| line.contains(id.as_str()));
            assert!(line.is_some_and(|line| line.contains(error)), "{logs}");
        }
    }
}

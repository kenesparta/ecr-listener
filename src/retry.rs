//! The two retry policies: how long a message that could not be settled stays invisible
//! before SQS delivers it again, and how long the poll loop waits after a failed receive.

use std::num::NonZeroU32;
use std::time::Duration;

use crate::config::DeployTimeout;

const FIRST_RETRY_SECS: u16 = 60;
const MAX_RETRY_SECS: u16 = 900;
/// Extra time a message stays hidden beyond the deploy timeout, so a deploy that is cut off at
/// the timeout still settles its messages before they reappear.
const HOLD_MARGIN_SECS: u16 = 60;

/// A visibility timeout in whole seconds, always within SQS's 0..=43200 range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VisibilityTimeout(u16);

impl VisibilityTimeout {
    /// How long a message stays hidden after a failed deploy or an unexpected body:
    /// `min(60 * 2^(receive_count - 1), 900)` seconds.
    pub(crate) fn for_retry(receive_count: NonZeroU32) -> Self {
        let doublings = receive_count.get() - 1;
        let secs = 2_u16
            .checked_pow(doublings)
            .and_then(|factor| factor.checked_mul(FIRST_RETRY_SECS))
            .map_or(MAX_RETRY_SECS, |secs| secs.min(MAX_RETRY_SECS));
        Self(secs)
    }

    /// How long a message stays hidden while the deploy it asked for runs.
    pub(crate) fn hold_for(timeout: DeployTimeout) -> Self {
        Self(timeout.as_secs().saturating_add(HOLD_MARGIN_SECS))
    }

    pub(crate) fn as_secs(self) -> i32 {
        i32::from(self.0)
    }
}

/// The delay between failed `ReceiveMessage` calls: 1 s, doubling up to 60 s, back to 1 s
/// after a receive succeeds.
#[derive(Debug)]
pub(crate) struct PollBackoff {
    next: Duration,
}

impl PollBackoff {
    const FIRST: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(60);

    /// The delay to wait now; the one after it is twice as long, up to the cap.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = delay.saturating_mul(2).min(Self::MAX);
        delay
    }

    pub(crate) fn reset(&mut self) {
        self.next = Self::FIRST;
    }
}

impl Default for PollBackoff {
    fn default() -> Self {
        Self { next: Self::FIRST }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry_secs(receive_count: u32) -> i32 {
        VisibilityTimeout::for_retry(NonZeroU32::new(receive_count).unwrap()).as_secs()
    }

    #[test]
    fn retry_delay_doubles_from_a_minute_per_delivery() {
        assert_eq!(retry_secs(1), 60);
        assert_eq!(retry_secs(2), 120);
        assert_eq!(retry_secs(3), 240);
        assert_eq!(retry_secs(4), 480);
    }

    #[test]
    fn retry_delay_caps_at_fifteen_minutes() {
        assert_eq!(retry_secs(5), 900);
        assert_eq!(retry_secs(8), 900);
        assert_eq!(retry_secs(17), 900);
        assert_eq!(retry_secs(u32::MAX), 900);
    }

    #[test]
    fn a_hold_covers_the_deploy_timeout_plus_a_minute() {
        let timeout = DeployTimeout::try_from(300).unwrap();
        assert_eq!(VisibilityTimeout::hold_for(timeout).as_secs(), 360);
        let longest = DeployTimeout::try_from(3600).unwrap();
        assert_eq!(VisibilityTimeout::hold_for(longest).as_secs(), 3660);
    }

    #[test]
    fn poll_backoff_doubles_from_one_second_to_a_minute() {
        let mut backoff = PollBackoff::default();
        let delays: Vec<u64> = (0..9).map(|_| backoff.next_delay().as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60, 60]);
    }

    #[test]
    fn poll_backoff_starts_over_after_a_reset() {
        let mut backoff = PollBackoff::default();
        for _ in 0..5 {
            backoff.next_delay();
        }
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
    }
}

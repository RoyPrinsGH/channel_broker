mod std;

#[cfg(feature = "std-mpsc-channel")]
pub use std::StdMpscChannel;
#[cfg(feature = "sync-channel")]
pub use std::SyncChannel;

#[cfg(any(feature = "broadcast-channel", feature = "watch-channel"))]
mod tokio;

#[cfg(feature = "broadcast-channel")]
pub use tokio::BroadcastChannel;
#[cfg(feature = "watch-channel")]
pub use tokio::WatchChannel;

use std::any::{Any, TypeId};
use std::collections::HashMap;

mod channels;

#[cfg(feature = "broadcast-channel")]
pub use channels::BroadcastChannel;
#[cfg(feature = "std-mpsc-channel")]
pub use channels::StdMpscChannel;
#[cfg(feature = "sync-channel")]
pub use channels::SyncChannel;
#[cfg(feature = "watch-channel")]
pub use channels::WatchChannel;

pub trait ChannelDef {
    type Message;
}

pub struct ChannelBroker {
    #[cfg(feature = "std-mpsc-channel")]
    std_mpsc_channels: HashMap<TypeId, Box<dyn Any>>,
    #[cfg(feature = "sync-channel")]
    sync_channels: HashMap<TypeId, Box<dyn Any>>,
    #[cfg(feature = "broadcast-channel")]
    broadcast_channels: HashMap<TypeId, Box<dyn Any>>,
    #[cfg(feature = "watch-channel")]
    watch_channels: HashMap<TypeId, Box<dyn Any>>,
}

impl Default for ChannelBroker {
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
    fn default() -> Self {
        #[cfg(feature = "tracing")]
        tracing::trace!("creating empty channel broker");

        Self {
            #[cfg(feature = "std-mpsc-channel")]
            std_mpsc_channels: HashMap::new(),
            #[cfg(feature = "sync-channel")]
            sync_channels: HashMap::new(),
            #[cfg(feature = "broadcast-channel")]
            broadcast_channels: HashMap::new(),
            #[cfg(feature = "watch-channel")]
            watch_channels: HashMap::new(),
        }
    }
}

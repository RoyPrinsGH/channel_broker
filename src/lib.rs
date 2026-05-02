//! Type-indexed channel registry with feature-gated standard-library and Tokio backends.
//! It maintains the invariant that a channel is either registered as a
//! sender/receiver pair or absent entirely. That, in turn, allows the broker to
//! expose ergonomic channel accessors without partial-state edge cases.
//!
//! `ChannelBroker` stores one channel instance per type key and lets the rest of an
//! application retrieve it without passing sender and receiver handles through every
//! layer. Most channel families are registered under a zero-sized marker type that
//! implements [`ChannelDef`].
//!
//! A common use case is wiring task factories or supervisors. Create the broker
//! once at startup, pass `&ChannelBroker` to each factory, and let each task grab
//! the publisher or reader it needs. This removes most explicit
//! sender/receiver-state management and makes it straightforward to stop and
//! restart tasks without breaking the expected communication paths.
//!
//! # Features
//!
//! - `std-mpsc-channel`: unbounded `std::sync::mpsc` channels via `add_channel`.
//! - `sync-channel`: bounded `std::sync::mpsc::sync_channel` channels via
//!   `add_sync_channel`.
//! - `broadcast-channel`: Tokio broadcast channels via `add_broadcast`.
//! - `watch-channel`: Tokio watch channels via `add_watch`.
//! - `tracing`: `tracing::instrument` spans around broker and channel operations.
//!
//! The `std-mpsc-channel` and `sync-channel` features are enabled by default.
//!
//! # Example
//!
//! This example requires the `broadcast-channel` and `watch-channel` features.
//!
//! ```
//! # #[cfg(all(feature = "broadcast-channel", feature = "watch-channel"))]
//! # {
//! use channel_broker::{ChannelBroker, ChannelDef};
//!
//! struct Events;
//! struct State;
//!
//! impl ChannelDef for Events {
//!     type Message = &'static str;
//! }
//!
//! impl ChannelDef for State {
//!     type Message = String;
//! }
//!
//! fn event_worker_factory(broker: &ChannelBroker) -> impl Future<Output = ()> + Send + 'static {
//!     let mut events = broker
//!         .broadcast::<Events>()
//!         .new_reader();
//!
//!     let state = broker
//!         .watch::<State>()
//!         .new_publisher();
//!
//!     async move {
//!         let event = events
//!             .recv()
//!             .await
//!             .unwrap();
//!
//!         state.send(format!("processed {event}"));
//!     }
//! }
//!
//! tokio::runtime::Builder::new_current_thread()
//!     .enable_all()
//!     .build()
//!     .unwrap()
//!     .block_on(async {
//!         let broker = ChannelBroker::default()
//!             .add_broadcast::<Events>(16)
//!             .add_watch::<State>("idle".to_owned());
//!
//!         tokio::spawn(event_worker_factory(&broker));
//!
//!         broker
//!             .broadcast::<Events>()
//!             .send("job finished");
//!
//!         let mut state_reader = broker
//!             .watch::<State>()
//!             .new_reader();
//!             
//!         state_reader
//!             .wait_for(|state| state == "processed job finished")
//!             .await;
//!     });
//! # }
//! ```
use std::any::{Any, TypeId};
use std::collections::HashMap;

mod channels;
mod codegen;

#[cfg(feature = "broadcast-channel")]
pub use channels::BroadcastChannel;
#[cfg(feature = "std-mpsc-channel")]
pub use channels::StdMpscChannel;
#[cfg(feature = "sync-channel")]
pub use channels::SyncChannel;
#[cfg(feature = "watch-channel")]
pub use channels::WatchChannel;

/// Associates a type-level key with the message payload stored in [`ChannelBroker`].
///
/// Implement this trait on a marker type when you want to register one of the
/// marker-keyed channel families with the broker.
pub trait ChannelDef {
    type Message;
}

/// Heterogeneous registry of channel instances keyed by `TypeId`.
///
/// Each registration method consumes and returns `Self`, which makes it convenient to
/// build a broker fluently during initialization. Registering another channel under the
/// same key replaces the previous entry for that key.
#[derive(Default)]
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

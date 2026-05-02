//! Tokio channel backends for `ChannelBroker`.

use std::any::TypeId;
#[cfg(feature = "tracing")]
use std::any::type_name;

#[cfg(feature = "broadcast-channel")]
use tokio::sync::broadcast;
#[cfg(feature = "watch-channel")]
use tokio::sync::watch;
#[cfg(feature = "watch-channel")]
use tokio::sync::watch::Ref;

#[cfg(any(feature = "broadcast-channel"))]
use crate::ChannelDef;
use crate::{ChannelBroker, impl_accessor_fields};

#[cfg(feature = "broadcast-channel")]
/// Wrapper around `tokio::sync::broadcast`.
///
/// The broker stores one sender and one receiver internally. Use [`Self::new_publisher`]
/// and [`Self::new_reader`] to clone additional handles for other tasks.
pub struct BroadcastChannel<T>
where
    T: Clone,
{
    sender: broadcast::Sender<T>,
    receiver: broadcast::Receiver<T>,
}

#[cfg(feature = "broadcast-channel")]
impl<T> BroadcastChannel<T>
where
    T: Clone,
{
    /// Creates a new broadcast channel with the provided ring-buffer `capacity`.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn new(capacity: usize) -> Self {
        #[cfg(feature = "tracing")]
        tracing::trace!(capacity, message_type = type_name::<T>(), "creating tokio broadcast channel");

        let (sender, receiver) = broadcast::channel::<T>(capacity);
        Self { sender, receiver }
    }

    /// Broadcasts `new` to all active receivers.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self, new),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn send(&self, new: T) {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "sending tokio broadcast message");

        match self
            .sender
            .send(new)
        {
            Ok(_) => (),
            Err(_) => unreachable!("we always hold a linked receiver, so this cannot fail"),
        }
    }

    /// Clones and returns an additional sender handle.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn new_publisher(&self) -> broadcast::Sender<T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "cloning tokio broadcast publisher");

        self.sender
            .clone()
    }

    /// Returns a fresh receiver subscribed to messages sent after this call.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn new_reader(&self) -> broadcast::Receiver<T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "creating tokio broadcast reader");

        self.receiver
            .resubscribe()
    }
}

#[cfg(feature = "broadcast-channel")]
impl ChannelBroker {
    /// Registers a Tokio broadcast channel for `TChannelDef`.
    ///
    /// Retrieve the channel later with the `broadcast*` broker accessors. If a channel
    /// with the same key already exists, it is replaced.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self),
            fields(
                channel_type = type_name::<TChannelDef>(),
                message_type = type_name::<TChannelDef::Message>()
            )
        )
    )]
    pub fn add_broadcast<TChannelDef>(mut self, capacity: usize) -> Self
    where
        TChannelDef: ChannelDef + 'static,
        TChannelDef::Message: Clone + 'static,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            capacity,
            channel_type = type_name::<TChannelDef>(),
            message_type = type_name::<TChannelDef::Message>(),
            "registering tokio broadcast channel"
        );

        self.broadcast_channels
            .insert(
                TypeId::of::<TChannelDef>(),
                Box::new(BroadcastChannel::<TChannelDef::Message>::new(capacity)),
            );

        self
    }

    impl_accessor_fields!(broadcast);
}

#[cfg(feature = "watch-channel")]
/// Wrapper around `tokio::sync::watch`.
///
/// A watch channel always stores a current value. Use [`Self::read`] to borrow the latest
/// state and [`Self::new_reader`] to clone additional receivers.
pub struct WatchChannel<T>
where
    T: Clone,
{
    sender: watch::Sender<T>,
    receiver: watch::Receiver<T>,
}

#[cfg(feature = "watch-channel")]
impl<T> WatchChannel<T>
where
    T: Clone,
{
    /// Creates a new watch channel seeded with `init`.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(init),
			fields(state_type = type_name::<T>())
		)
	)]
    pub fn new(init: T) -> Self {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<T>(), "creating tokio watch channel");

        let (sender, receiver) = watch::channel::<T>(init);
        Self { sender, receiver }
    }

    /// Replaces the current value with `new` and notifies watchers.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self, new),
			fields(state_type = type_name::<T>())
		)
	)]
    pub fn update(&self, new: T) {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<T>(), "updating tokio watch channel");

        self.sender
            .send(new)
            .expect("we always hold a linked receiver, so this cannot fail")
    }

    /// Borrows the most recently published value.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(state_type = type_name::<T>())
		)
	)]
    pub fn read(&self) -> Ref<'_, T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<T>(), "reading tokio watch channel");

        self.receiver
            .borrow()
    }

    /// Waits for the published value to match the given predicate.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self, f),
			fields(state_type = type_name::<T>())
		)
	)]
    pub async fn wait_for(&mut self, f: impl FnMut(&T) -> bool) -> Ref<'_, T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<T>(), "reading tokio watch channel");

        self.receiver
            .wait_for(f)
            .await
            .expect("we always hold a sender, so this cannot fail")
    }

    /// Clones and returns an additional sender handle.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(state_type = type_name::<T>())
		)
	)]
    pub fn new_publisher(&self) -> watch::Sender<T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<T>(), "cloning tokio watch publisher");

        self.sender
            .clone()
    }

    /// Clones and returns an additional receiver handle.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(state_type = type_name::<T>())
		)
	)]
    pub fn new_reader(&self) -> watch::Receiver<T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<T>(), "cloning tokio watch reader");

        self.receiver
            .clone()
    }
}

#[cfg(feature = "watch-channel")]
impl ChannelBroker {
    /// Registers a Tokio watch channel for `TChannelDef`.
    ///
    /// Retrieve the channel later with the `watch*` broker accessors. If a channel
    /// with the same key already exists, it is replaced.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self, init),
            fields(state_type = type_name::<TChannelDef>())
        )
    )]
    pub fn add_watch<TChannelDef>(mut self, init: TChannelDef::Message) -> Self
    where
        TChannelDef: ChannelDef + 'static,
        TChannelDef::Message: Clone,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<TChannelDef>(), "registering tokio watch channel");

        self.watch_channels
            .insert(TypeId::of::<TChannelDef>(), Box::new(WatchChannel::<TChannelDef::Message>::new(init)));

        self
    }

    impl_accessor_fields!(watch);
}

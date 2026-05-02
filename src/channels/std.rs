//! Standard-library channel backends for `ChannelBroker`.

use std::any::TypeId;
#[cfg(feature = "tracing")]
use std::any::type_name;
#[cfg(feature = "std-mpsc-channel")]
use std::sync::mpsc::Sender;
use std::sync::mpsc::{self, Receiver, TryRecvError};
#[cfg(feature = "sync-channel")]
use std::sync::mpsc::{SyncSender, TrySendError};

use crate::{ChannelBroker, ChannelDef, impl_accessor_fields};

#[cfg(feature = "std-mpsc-channel")]
/// Thin wrapper around `std::sync::mpsc::channel`.
///
/// The broker stores one sender and one receiver internally. Use [`Self::new_publisher`]
/// to hand out additional producer handles while keeping the canonical receiver inside
/// the broker.
pub struct StdMpscChannel<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

#[cfg(feature = "std-mpsc-channel")]
impl<T> StdMpscChannel<T> {
    /// Creates a new unbounded multi-producer, single-consumer channel.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn new() -> Self {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "creating std channel");

        let (sender, receiver) = mpsc::channel::<T>();
        Self { sender, receiver }
    }

    /// Sends `new` into the channel.
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
        tracing::trace!(message_type = type_name::<T>(), "sending std channel message");

        self.sender
            .send(new)
            .expect("we always hold a linked receiver, so this cannot fail")
    }

    /// Blocks until a message is available and returns it.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn recv(&self) -> T {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "receiving std channel message");

        self.receiver
            .recv()
            .expect("we always hold a linked sender, so this cannot fail")
    }

    /// Attempts to receive a message without blocking.
    ///
    /// Returns `None` when the channel is currently empty.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn try_recv(&self) -> Option<T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "trying to receive std channel message");

        match self
            .receiver
            .try_recv()
        {
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => unreachable!("we always hold a linked receiver, so this cannot be disconnected"),
            Ok(data) => Some(data),
        }
    }

    /// Clones and returns an additional producer handle.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn new_publisher(&self) -> mpsc::Sender<T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "cloning std channel publisher");

        self.sender
            .clone()
    }
}

#[cfg(feature = "std-mpsc-channel")]
impl<T> Default for StdMpscChannel<T> {
    /// Creates a new unbounded channel.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "std-mpsc-channel")]
impl ChannelBroker {
    /// Registers an unbounded standard-library channel for `TChannelDef`.
    ///
    /// Retrieve the channel later with the `std_mpsc*` broker accessors. If a channel
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
    pub fn add_channel<TChannelDef>(mut self) -> Self
    where
        TChannelDef: ChannelDef + 'static,
        TChannelDef::Message: 'static,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            channel_type = type_name::<TChannelDef>(),
            message_type = type_name::<TChannelDef::Message>(),
            "registering std channel"
        );

        self.std_mpsc_channels
            .insert(TypeId::of::<TChannelDef>(), Box::new(StdMpscChannel::<TChannelDef::Message>::new()));

        self
    }

    impl_accessor_fields!(std_mpsc);
}

#[cfg(feature = "sync-channel")]
/// Thin wrapper around `std::sync::mpsc::sync_channel`.
///
/// The broker keeps one sender and one receiver internally. Use [`Self::new_publisher`]
/// when additional producer handles are needed.
pub struct SyncChannel<T> {
    sender: SyncSender<T>,
    receiver: Receiver<T>,
}

#[cfg(feature = "sync-channel")]
impl<T> SyncChannel<T> {
    /// Creates a new bounded channel with the provided `bound`.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn new(bound: usize) -> Self {
        #[cfg(feature = "tracing")]
        tracing::trace!(bound, message_type = type_name::<T>(), "creating std sync channel");

        let (sender, receiver) = mpsc::sync_channel::<T>(bound);
        Self { sender, receiver }
    }

    /// Sends `new`, blocking until the bounded channel has capacity.
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
        tracing::trace!(message_type = type_name::<T>(), "sending std sync channel message");

        self.sender
            .send(new)
            .expect("we always hold a linked receiver, so this cannot fail")
    }

    /// Attempts to send `new` without blocking.
    ///
    /// Returns the original message in `Err(new)` when the channel is full.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self, new),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn try_send(&self, new: T) -> Result<(), T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "trying to send std sync channel message");

        match self
            .sender
            .try_send(new)
        {
            Err(TrySendError::Full(returned)) => Err(returned),
            Err(TrySendError::Disconnected(_)) => unreachable!("we always hold a linked receiver, so this cannot be disconnected"),
            Ok(()) => Ok(()),
        }
    }

    /// Blocks until a message is available and returns it.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn recv(&self) -> T {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "receiving std sync channel message");

        self.receiver
            .recv()
            .expect("we always hold a linked sender, so this cannot fail")
    }

    /// Attempts to receive a message without blocking.
    ///
    /// Returns `None` when the channel is currently empty.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn try_recv(&self) -> Option<T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "trying to receive std sync channel message");

        match self
            .receiver
            .try_recv()
        {
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => unreachable!("we always hold a linked receiver, so this cannot be disconnected"),
            Ok(data) => Some(data),
        }
    }

    /// Clones and returns an additional producer handle.
    #[cfg_attr(
		feature = "tracing",
		tracing::instrument(
			level = "trace",
			skip(self),
			fields(message_type = type_name::<T>())
		)
	)]
    pub fn new_publisher(&self) -> mpsc::SyncSender<T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(message_type = type_name::<T>(), "cloning std sync channel publisher");

        self.sender
            .clone()
    }
}

#[cfg(feature = "sync-channel")]
impl ChannelBroker {
    /// Registers a bounded standard-library channel for `TChannelDef`.
    ///
    /// Retrieve the channel later with the `sync*` broker accessors. If a channel with
    /// the same key already exists, it is replaced.
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
    pub fn add_sync_channel<TChannelDef>(mut self, bound: usize) -> Self
    where
        TChannelDef: ChannelDef + 'static,
        TChannelDef::Message: 'static,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            bound,
            channel_type = type_name::<TChannelDef>(),
            message_type = type_name::<TChannelDef::Message>(),
            "registering std sync channel"
        );

        self.sync_channels
            .insert(TypeId::of::<TChannelDef>(), Box::new(SyncChannel::<TChannelDef::Message>::new(bound)));

        self
    }

    impl_accessor_fields!(sync);
}

#[cfg(any(feature = "broadcast-channel", feature = "watch-channel"))]
use std::any::TypeId;
#[cfg(feature = "tracing")]
use std::any::type_name;

#[cfg(feature = "broadcast-channel")]
use tokio::sync::broadcast;
#[cfg(feature = "watch-channel")]
use tokio::sync::watch;
#[cfg(feature = "watch-channel")]
use tokio::sync::watch::Ref;

use crate::ChannelBroker;
#[cfg(any(feature = "broadcast-channel"))]
use crate::ChannelDef;

#[cfg(feature = "broadcast-channel")]
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

        _ = self
            .sender
            .send(new);
    }

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

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self),
            fields(channel_type = type_name::<TChannelDef>())
        )
    )]
    pub fn broadcast_maybe<TChannelDef>(&self) -> Option<&BroadcastChannel<TChannelDef::Message>>
    where
        TChannelDef: ChannelDef + 'static,
        TChannelDef::Message: Clone + 'static,
    {
        let maybe_channel = self
            .broadcast_channels
            .get(&TypeId::of::<TChannelDef>())?
            .downcast_ref::<BroadcastChannel<TChannelDef::Message>>();

        #[cfg(feature = "tracing")]
        tracing::trace!(
            channel_type = type_name::<TChannelDef>(),
            found = maybe_channel.is_some(),
            "resolved tokio broadcast channel"
        );

        maybe_channel
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self),
            fields(channel_type = type_name::<TChannelDef>())
        )
    )]
    pub fn broadcast<TChannelDef>(&self) -> &BroadcastChannel<TChannelDef::Message>
    where
        TChannelDef: ChannelDef + 'static,
        TChannelDef::Message: Clone + 'static,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(channel_type = type_name::<TChannelDef>(), "accessing tokio broadcast channel");

        self.broadcast_maybe::<TChannelDef>()
            .expect("requested tokio broadcast channel is not registered in ChannelBroker")
    }
}

#[cfg(feature = "watch-channel")]
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

        _ = self
            .sender
            .send(new);
    }

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
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self, init),
            fields(state_type = type_name::<TState>())
        )
    )]
    pub fn add_watch<TState>(mut self, init: TState) -> Self
    where
        TState: Clone + 'static,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<TState>(), "registering tokio watch channel");

        self.watch_channels
            .insert(TypeId::of::<TState>(), Box::new(WatchChannel::<TState>::new(init)));

        self
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self),
            fields(state_type = type_name::<TState>())
        )
    )]
    pub fn watch_maybe<TState>(&self) -> Option<&WatchChannel<TState>>
    where
        TState: Clone + 'static,
    {
        let maybe_channel = self
            .watch_channels
            .get(&TypeId::of::<TState>())?
            .downcast_ref::<WatchChannel<TState>>();

        #[cfg(feature = "tracing")]
        tracing::trace!(
            state_type = type_name::<TState>(),
            found = maybe_channel.is_some(),
            "resolved tokio watch channel"
        );

        maybe_channel
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self),
            fields(state_type = type_name::<TState>())
        )
    )]
    pub fn watch<TState>(&self) -> &WatchChannel<TState>
    where
        TState: Clone + 'static,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<TState>(), "accessing tokio watch channel");

        self.watch_maybe::<TState>()
            .expect("requested tokio watch channel is not registered in ChannelBroker")
    }
}

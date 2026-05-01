use std::any::{Any, TypeId};
use std::collections::HashMap;

#[cfg(feature = "tracing")]
use std::any::type_name;

use tokio::sync::broadcast;
use tokio::sync::watch::{self, Ref};

pub struct EventChannel<T>
where
    T: Clone,
{
    sender: broadcast::Sender<T>,
    receiver: broadcast::Receiver<T>,
}

impl<T> EventChannel<T>
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
        tracing::trace!(capacity, message_type = type_name::<T>(), "creating event channel");

        let (sender, receiver) = broadcast::channel::<T>(capacity);
        Self { sender, receiver }
    }
}

impl<T> EventChannel<T>
where
    T: Clone,
{
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
        tracing::trace!(message_type = type_name::<T>(), "sending event message");

        // We own receiver, so this never fails
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
        tracing::trace!(message_type = type_name::<T>(), "cloning event publisher");

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
        tracing::trace!(message_type = type_name::<T>(), "creating event reader");

        self.receiver
            .resubscribe()
    }
}

pub struct StateChannel<T>
where
    T: Clone,
{
    sender: watch::Sender<T>,
    receiver: watch::Receiver<T>,
}

impl<T> StateChannel<T>
where
    T: Clone,
{
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
        tracing::trace!(state_type = type_name::<T>(), "updating state channel");

        // We own receiver, so this never fails
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
        tracing::trace!(state_type = type_name::<T>(), "reading state channel");

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
        tracing::trace!(state_type = type_name::<T>(), "cloning state publisher");

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
        tracing::trace!(state_type = type_name::<T>(), "cloning state reader");

        self.receiver
            .clone()
    }
}

impl<T> StateChannel<T>
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
        tracing::trace!(state_type = type_name::<T>(), "creating state channel");

        let (sender, receiver) = watch::channel::<T>(init);
        Self { sender, receiver }
    }
}

pub trait ChannelDef {
    type Message;
}

pub struct ChannelBroker {
    events: HashMap<TypeId, Box<dyn Any>>,
    states: HashMap<TypeId, Box<dyn Any>>,
}

impl Default for ChannelBroker {
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
    fn default() -> Self {
        #[cfg(feature = "tracing")]
        tracing::trace!("creating empty channel broker");

        Self {
            events: HashMap::new(),
            states: HashMap::new(),
        }
    }
}

impl ChannelBroker {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self),
            fields(
                channel_type = type_name::<TEventChannelDef>(),
                message_type = type_name::<TEventChannelDef::Message>()
            )
        )
    )]
    pub fn add_channel<TEventChannelDef>(mut self, capacity: usize) -> Self
    where
        TEventChannelDef: ChannelDef + 'static,
        TEventChannelDef::Message: Clone,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            capacity,
            channel_type = type_name::<TEventChannelDef>(),
            message_type = type_name::<TEventChannelDef::Message>(),
            "registering event channel"
        );

        self.events
            .insert(
                TypeId::of::<TEventChannelDef>(),
                Box::new(EventChannel::<TEventChannelDef::Message>::new(capacity)),
            );

        self
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace", 
            skip(self, init), 
            fields(state_type = type_name::<TState>())
        )
    )]
    pub fn add_state<TState>(mut self, init: TState) -> Self
    where
        TState: Clone + 'static,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<TState>(), "registering state channel");

        self.states
            .insert(TypeId::of::<TState>(), Box::new(StateChannel::<TState>::new(init)));

        self
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace", 
            skip(self), 
            fields(channel_type = type_name::<TEventChannelDef>())
        )
    )]
    pub fn events_maybe<TEventChannelDef>(&self) -> Option<&EventChannel<TEventChannelDef::Message>>
    where
        TEventChannelDef: ChannelDef + 'static,
        TEventChannelDef::Message: Clone,
    {
        let maybe_event_channel = self
            .events
            .get(&TypeId::of::<TEventChannelDef>())?
            .downcast_ref::<EventChannel<TEventChannelDef::Message>>();

        #[cfg(feature = "tracing")]
        tracing::trace!(
            channel_type = type_name::<TEventChannelDef>(),
            found = maybe_event_channel.is_some(),
            "resolved event channel"
        );

        maybe_event_channel
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace", 
            skip(self), 
            fields(channel_type = type_name::<TEventChannelDef>())
        )
    )]
    pub fn events<TEventChannelDef>(&self) -> &EventChannel<TEventChannelDef::Message>
    where
        TEventChannelDef: ChannelDef + 'static,
        TEventChannelDef::Message: Clone,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(channel_type = type_name::<TEventChannelDef>(), "accessing event channel");

        self.events_maybe::<TEventChannelDef>()
            .expect("requested event channel is not registered in ChannelBroker")
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace", 
            skip(self), 
            fields(state_type = type_name::<TState>())
        )
    )]
    pub fn state_maybe<TState>(&self) -> Option<&StateChannel<TState>>
    where
        TState: Clone + 'static,
    {
        let maybe_state_channel = self
            .states
            .get(&TypeId::of::<TState>())?
            .downcast_ref::<StateChannel<TState>>();

        #[cfg(feature = "tracing")]
        tracing::trace!(
            state_type = type_name::<TState>(),
            found = maybe_state_channel.is_some(),
            "resolved state channel"
        );

        maybe_state_channel
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace", 
            skip(self), 
            fields(state_type = type_name::<TState>())
        )
    )]
    pub fn state<TState>(&self) -> &StateChannel<TState>
    where
        TState: Clone + 'static,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!(state_type = type_name::<TState>(), "accessing state channel");

        self.state_maybe::<TState>()
            .expect("requested state channel is not registered in ChannelBroker")
    }
}

#[cfg(test)]
mod tests {
    use crate::{ChannelBroker, ChannelDef, EventChannel, StateChannel};

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct MyState {
        dummy: i32,
    }

    struct MyChannel;

    impl ChannelDef for MyChannel {
        type Message = MyState;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn state_channel_supports_update_publishers_and_readers() {
        let channel = StateChannel::new(MyState { dummy: 1 });

        assert_eq!(
            channel
                .read()
                .dummy,
            1
        );

        let publisher = channel.new_publisher();
        let mut reader = channel.new_reader();

        publisher
            .send(MyState { dummy: 2 })
            .expect("state publisher should deliver while channel receiver is owned");

        reader
            .changed()
            .await
            .expect("state reader should observe publisher update");

        assert_eq!(
            reader
                .borrow_and_update()
                .dummy,
            2
        );

        assert_eq!(
            channel
                .read()
                .dummy,
            2
        );

        channel.update(MyState { dummy: 3 });

        reader
            .changed()
            .await
            .expect("state reader should observe channel update");

        assert_eq!(
            reader
                .borrow_and_update()
                .dummy,
            3
        );

        assert_eq!(
            channel
                .read()
                .dummy,
            3
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn event_channel_supports_send_publishers_and_readers() {
        let channel = EventChannel::new(2);

        let publisher = channel.new_publisher();
        let mut reader = channel.new_reader();

        channel.send(MyState { dummy: 1 });

        assert_eq!(
            reader
                .recv()
                .await
                .expect("event reader should receive sent message")
                .dummy,
            1
        );

        let mut second_reader = channel.new_reader();

        publisher
            .send(MyState { dummy: 2 })
            .expect("event publisher should deliver while channel receiver is owned");

        assert_eq!(
            second_reader
                .recv()
                .await
                .expect("second event reader should receive published message")
                .dummy,
            2
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn broker_returns_registered_channels_and_states() {
        let broker = ChannelBroker::default()
            .add_state(MyState { dummy: 10 })
            .add_channel::<MyChannel>(2);

        assert_eq!(
            broker
                .state_maybe::<MyState>()
                .expect("registered state channel should be available")
                .read()
                .dummy,
            10
        );

        assert!(
            broker
                .events_maybe::<MyChannel>()
                .is_some()
        );

        let mut state_reader = broker
            .state::<MyState>()
            .new_reader();

        broker
            .state::<MyState>()
            .update(MyState { dummy: 11 });

        state_reader
            .changed()
            .await
            .expect("broker state reader should observe state update");

        assert_eq!(
            state_reader
                .borrow_and_update()
                .dummy,
            11
        );

        let mut event_reader = broker
            .events::<MyChannel>()
            .new_reader();

        broker
            .events::<MyChannel>()
            .send(MyState { dummy: 12 });

        assert_eq!(
            event_reader
                .recv()
                .await
                .expect("broker event reader should receive sent event")
                .dummy,
            12
        );
    }
}

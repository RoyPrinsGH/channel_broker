#[doc(hidden)]
#[macro_export]
macro_rules! __impl_accessor_fields {
    (
        name = $name:ident,
        field = $field:ident,
        key = $key:ident,
        channel = $channel:ty,
        bounds = { $($bounds:tt)* }
    ) => {
        paste::paste! {
            #[cfg_attr(
                feature = "tracing",
                tracing::instrument(
                    level = "trace",
                    skip(self),
                    fields(
                        accessor = stringify!($name),
                        key_type = ::std::any::type_name::<$key>()
                    )
                )
            )]
            pub fn [<$name _maybe>]<$key>(&self) -> Option<&$channel>
            where
                $($bounds)*
            {
                let maybe_channel = self
                    .$field
                    .get(&::std::any::TypeId::of::<$key>())?
                    .downcast_ref::<$channel>();

                #[cfg(feature = "tracing")]
                tracing::trace!(
                    accessor = stringify!($name),
                    key_type = ::std::any::type_name::<$key>(),
                    found = maybe_channel.is_some(),
                    "resolved channel broker entry"
                );

                maybe_channel
            }

            #[cfg_attr(
                feature = "tracing",
                tracing::instrument(
                    level = "trace",
                    skip(self),
                    fields(
                        accessor = stringify!($name),
                        key_type = ::std::any::type_name::<$key>()
                    )
                )
            )]
            pub fn $name<$key>(&self) -> &$channel
            where
                $($bounds)*
            {
                #[cfg(feature = "tracing")]
                tracing::trace!(
                    accessor = stringify!($name),
                    key_type = ::std::any::type_name::<$key>(),
                    "accessing channel broker entry"
                );

                self.[<$name _maybe>]::<$key>()
                    .expect(concat!("requested `", stringify!($name), "` channel is not registered in ChannelBroker"))
            }

            #[cfg_attr(
                feature = "tracing",
                tracing::instrument(
                    level = "trace",
                    skip(self),
                    fields(
                        accessor = stringify!($name),
                        key_type = ::std::any::type_name::<$key>()
                    )
                )
            )]
            pub fn [<$name _mut_maybe>]<$key>(&mut self) -> Option<&mut $channel>
            where
                $($bounds)*
            {
                let maybe_channel = self
                    .$field
                    .get_mut(&::std::any::TypeId::of::<$key>())?
                    .downcast_mut::<$channel>();

                #[cfg(feature = "tracing")]
                tracing::trace!(
                    accessor = stringify!($name),
                    key_type = ::std::any::type_name::<$key>(),
                    found = maybe_channel.is_some(),
                    mutable = true,
                    "resolved channel broker entry"
                );

                maybe_channel
            }

            #[cfg_attr(
                feature = "tracing",
                tracing::instrument(
                    level = "trace",
                    skip(self),
                    fields(
                        accessor = stringify!($name),
                        key_type = ::std::any::type_name::<$key>()
                    )
                )
            )]
            pub fn [<$name _mut>]<$key>(&mut self) -> &mut $channel
            where
                $($bounds)*
            {
                #[cfg(feature = "tracing")]
                tracing::trace!(
                    accessor = stringify!($name),
                    key_type = ::std::any::type_name::<$key>(),
                    mutable = true,
                    "accessing channel broker entry"
                );

                self.[<$name _mut_maybe>]::<$key>()
                    .expect(concat!("requested `", stringify!($name), "` channel is not registered in ChannelBroker"))
            }
        }
    };
}

#[macro_export]
macro_rules! impl_accessor_fields {
    (channel) => {
        $crate::__impl_accessor_fields!(
            name = channel,
            field = std_mpsc_channels,
            key = TChannelDef,
            channel = $crate::StdMpscChannel<TChannelDef::Message>,
            bounds = {
                TChannelDef: $crate::ChannelDef + 'static,
                TChannelDef::Message: 'static,
            }
        );
    };
    (sync_channel) => {
        $crate::__impl_accessor_fields!(
            name = sync_channel,
            field = sync_channels,
            key = TChannelDef,
            channel = $crate::SyncChannel<TChannelDef::Message>,
            bounds = {
                TChannelDef: $crate::ChannelDef + 'static,
                TChannelDef::Message: 'static,
            }
        );
    };
    (broadcast) => {
        $crate::__impl_accessor_fields!(
            name = broadcast,
            field = broadcast_channels,
            key = TChannelDef,
            channel = $crate::BroadcastChannel<TChannelDef::Message>,
            bounds = {
                TChannelDef: $crate::ChannelDef + 'static,
                TChannelDef::Message: Clone + 'static,
            }
        );
    };
    (watch) => {
        $crate::__impl_accessor_fields!(
            name = watch,
            field = watch_channels,
            key = TState,
            channel = $crate::WatchChannel<TState>,
            bounds = {
                TState: Clone + 'static,
            }
        );
    };
}

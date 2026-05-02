//! Internal macros that generate the broker accessor methods.

#[doc(hidden)]
#[macro_export]
macro_rules! impl_accessor_fields {
    ($name:ident) => {
        paste::paste! {
            #[doc = "Returns the registered channel for the requested type key, or `None` if it has not been added."]
            #[cfg_attr(
                feature = "tracing",
                tracing::instrument(
                    level = "trace",
                    skip(self),
                    fields(
                        accessor = stringify!($name),
                        channel = ::std::any::type_name::<TChannelDef>()
                    )
                )
            )]
            pub fn [<$name _maybe>]<TChannelDef>(&self) -> Option<&[<$name:camel Channel>]<TChannelDef::Message>>
            where
                TChannelDef: $crate::ChannelDef + 'static,
                TChannelDef::Message: Clone
            {
                let maybe_channel = self
                    .[<$name:snake _channels>]
                    .get(&::std::any::TypeId::of::<TChannelDef>())?
                    .downcast_ref::<[<$name:camel Channel>]<TChannelDef::Message>>();

                #[cfg(feature = "tracing")]
                tracing::trace!(
                    found = maybe_channel.is_some(),
                    "resolved channel broker entry"
                );

                maybe_channel
            }

            #[doc = "Returns the registered channel for the requested type key.\n\n# Panics\n\nPanics if the broker does not contain a matching entry."]
            #[cfg_attr(
                feature = "tracing",
                tracing::instrument(
                    level = "trace",
                    skip(self),
                    fields(
                        accessor = stringify!($name),
                        channel = ::std::any::type_name::<TChannelDef>()
                    )
                )
            )]
            pub fn $name<TChannelDef>(&self) -> &[<$name:camel Channel>]<TChannelDef::Message>
            where
                TChannelDef: $crate::ChannelDef + 'static,
                TChannelDef::Message: Clone
            {
                #[cfg(feature = "tracing")]
                tracing::trace!("accessing channel broker entry");

                self.[<$name _maybe>]::<TChannelDef>()
                    .expect(concat!("requested `", stringify!($name), "` channel is not registered in ChannelBroker"))
            }

            #[doc = "Returns a mutable reference to the registered channel for the requested type key, or `None` if it has not been added."]
            #[cfg_attr(
                feature = "tracing",
                tracing::instrument(
                    level = "trace",
                    skip(self),
                    fields(
                        accessor = stringify!($name),
                        channel = ::std::any::type_name::<TChannelDef>()
                    )
                )
            )]
            pub fn [<$name _mut_maybe>]<TChannelDef>(&mut self) -> Option<&mut [<$name:camel Channel>]<TChannelDef::Message>>
            where
                TChannelDef: $crate::ChannelDef + 'static,
                TChannelDef::Message: Clone
            {
                let maybe_channel = self
                    .[<$name:snake _channels>]
                    .get_mut(&::std::any::TypeId::of::<TChannelDef>())?
                    .downcast_mut::<[<$name:camel Channel>]<TChannelDef::Message>>();

                #[cfg(feature = "tracing")]
                tracing::trace!(
                    found = maybe_channel.is_some(),
                    "resolved channel broker entry"
                );

                maybe_channel
            }

            #[doc = "Returns a mutable reference to the registered channel for the requested type key.\n\n# Panics\n\nPanics if the broker does not contain a matching entry."]
            #[cfg_attr(
                feature = "tracing",
                tracing::instrument(
                    level = "trace",
                    skip(self),
                    fields(
                        accessor = stringify!($name),
                        channel = ::std::any::type_name::<TChannelDef>()
                    )
                )
            )]
            pub fn [<$name _mut>]<TChannelDef>(&mut self) -> &mut [<$name:camel Channel>]<TChannelDef::Message>
            where
                TChannelDef: $crate::ChannelDef + 'static,
                TChannelDef::Message: Clone
            {
                #[cfg(feature = "tracing")]
                tracing::trace!("accessing channel broker entry");

                self.[<$name _mut_maybe>]::<TChannelDef>()
                    .expect(concat!("requested `", stringify!($name), "` channel is not registered in ChannelBroker"))
            }
        }
    };
}

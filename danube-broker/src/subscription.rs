#[derive(Debug, Default)]
pub(crate) struct Subscription {}

#[derive(Debug, Clone)]
pub(crate) struct SubscriptionOption {
    subscription_name: String,
    consumer_id: f32,
    consumer_name: String,
    schema_type: String, // has to be SchemaType as type
}

#[derive(Debug, Default)]
pub struct Policies {
    max_producers_per_topic: Option<u32>,
    max_consumers_per_topic: Option<u32>,
    max_consumers_per_subscription: Option<u32>,
    max_subscriptions_per_topic: Option<u32>,
    max_topics_per_namespace: Option<u32>,
}

impl Policies {
    pub(crate) fn new() -> Self {
        Policies {
            ..Default::default()
        }
    }
}

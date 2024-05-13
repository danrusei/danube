#[derive(Debug, Default)]
pub(crate) struct DanubeResources {
    cluster: ClusterResources,
    namespace: NamespaceResources,
    topic: TopicResources,
}

impl DanubeResources {
    pub(crate) fn new() -> Self {
        DanubeResources::default()
    }
}

#[derive(Debug, Default)]
pub(crate) struct ClusterResources {}

#[derive(Debug, Default)]
pub(crate) struct NamespaceResources {}

#[derive(Debug, Default)]
pub(crate) struct TopicResources {}

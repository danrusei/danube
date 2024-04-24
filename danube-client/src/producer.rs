pub struct Producer {}

#[derive(Debug, Default)]
pub struct ProducerBuilder {
    topic: String,
    name: String,
}

impl ProducerBuilder {
    pub fn new() -> Self {
        ProducerBuilder::default()
    }

    /// sets the producer's topic
    pub fn with_topic<S: Into<String>>(mut self, topic: S) -> Self {
        self.topic = topic.into();
        self
    }

    /// sets the producer's name
    pub fn with_name<S: Into<String>>(mut self, name: S) -> Self {
        self.name = name.into();
        self
    }

    pub fn create(self) -> Producer {
        Producer {}
    }
}

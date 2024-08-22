use metrics_exporter_prometheus::PrometheusBuilder;
pub(crate) struct Metric {
    pub name: &'static str,
    description: &'static str,
}

pub(crate) const COUNTERS: [Metric; 6] = [
    TOPIC_BYTES_IN_COUNTER,
    TOPIC_BYTES_OUT_COUNTER,
    TOPIC_MSG_IN_COUNTER,
    TOPIC_MSG_OUT_COUNTER,
    CONSUMER_BYTES_OUT_COUNTER,
    CONSUMER_MSG_OUT_COUNTER,
];
pub(crate) const GAUGES: [Metric; 3] = [TOPIC_PRODUCERS, TOPIC_CONSUMERS, BROKER_TOPICS];
pub(crate) const HISTOGRAMS: [Metric; 2] = [TOPIC_MSG_IN_RATE, CONSUMER_MSG_OUT_RATE];

// BROKER Metrics --------------------------

pub(crate) const BROKER_TOPICS: Metric = Metric {
    name: "broker_topics",
    description: "Total number of topics served by broker",
};

// TOPIC Metrics --------------------------

pub(crate) const TOPIC_BYTES_IN_COUNTER: Metric = Metric {
    name: "topic_bytes_in_counter",
    description: "Total bytes published to the topic (bytes)",
};

pub(crate) const TOPIC_BYTES_OUT_COUNTER: Metric = Metric {
    name: "topic_bytes_out_counter",
    description: "Total bytes delivered to consumer (bytes)",
};

pub(crate) const TOPIC_MSG_IN_COUNTER: Metric = Metric {
    name: "topic_msg_in_counter",
    description: "Total messages published to the topic (msg).",
};

pub(crate) const TOPIC_MSG_OUT_COUNTER: Metric = Metric {
    name: "topic_msg_out_counter",
    description: "Total messages delivered to consumer (msg).",
};

pub(crate) const TOPIC_MSG_IN_RATE: Metric = Metric {
    name: "topic_msg_in_rate",
    description: "Message publishing time to topic",
};

pub(crate) const TOPIC_PRODUCERS: Metric = Metric {
    name: "topic_producers",
    description: "Total number of producers for topic",
};

pub(crate) const TOPIC_CONSUMERS: Metric = Metric {
    name: "topic_consumers",
    description: "Total number od consumers for topic",
};

// CONSUMER Metrics --------------------------

pub(crate) const CONSUMER_BYTES_OUT_COUNTER: Metric = Metric {
    name: "consumer_bytes_out_counter",
    description: "Total bytes delivered to consumer (bytes)",
};

pub(crate) const CONSUMER_MSG_OUT_COUNTER: Metric = Metric {
    name: "consumer_msg_out_counter",
    description: "Total messages delivered to consumer (msg).",
};

pub(crate) const CONSUMER_MSG_OUT_RATE: Metric = Metric {
    name: "consumer_msg_out_rate",
    description: "Message rate sent to consumer",
};

pub(crate) fn init_metrics(prom_addr: Option<std::net::SocketAddr>) {
    println!("initializing metrics exporter");

    if let Some(addr) = prom_addr {
        PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()
            .expect("failed to install Prometheus recorder");
    }

    for name in COUNTERS {
        register_counter(name)
    }

    for name in GAUGES {
        register_gauge(name)
    }

    for name in HISTOGRAMS {
        register_histogram(name)
    }
}

/// Registers a counter with the given name.
fn register_counter(metric: Metric) {
    metrics::describe_counter!(metric.name, metric.description);
    let _counter = metrics::counter!(metric.name);
}

/// Registers a gauge with the given name.
fn register_gauge(metric: Metric) {
    metrics::describe_gauge!(metric.name, metric.description);
    let _gauge = metrics::gauge!(metric.name);
}

/// Registers a histogram with the given name.
fn register_histogram(metric: Metric) {
    metrics::describe_histogram!(metric.name, metric.description);
    let _histogram = metrics::histogram!(metric.name);
}

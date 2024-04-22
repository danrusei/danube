# Define broker ports
BROKER_PORTS := 6650 6651 6652

# ETCD configuration
ETCD_NAME := my-etcd
ETCD_DATA_DIR := ./etcd-data
ETCD_PORT := 2379

# HAProxy configuration
HAPROXY_CONFIG := haproxy.cfg
HAPROXY_PORT := 8080

# Rust build command
#BUILD_CMD := cargo build --release

.PHONY: all brokers etcd haproxy clean

all: etcd brokers haproxy

etcd:
	@echo "Starting ETCD..."
	docker run -d --name $(ETCD_NAME) -p $(ETCD_PORT):$(ETCD_PORT) \
	    -v $(PWD)/$(ETCD_DATA_DIR):/etcd-data \
	    quay.io/coreos/etcd:latest \
	    /usr/local/bin/etcd \
	    --name $(ETCD_NAME) \
	    --data-dir /etcd-data \
	    --advertise-client-urls http://0.0.0.0:$(ETCD_PORT) \
	    --listen-client-urls http://0.0.0.0:$(ETCD_PORT)
	@echo "ETCD instance started on port: $(ETCD_PORT)"

brokers:
	@echo "Building Danube brokers..."
	@for port in $(BROKER_PORTS); do \
		cargo build --release --package danube-broker --bin danube-broker && \
		RUST_BACKTRACE=1 ./target/release/danube-broker --service-port $$port & \
	done
	@echo "Danube brokers started on ports: $(BROKER_PORTS)"

haproxy:
	@echo "Starting HAProxy..."
	@haproxy -f $(HAPROXY_CONFIG) -d
	@echo "HAProxy listening on port: $(HAPROXY_PORT)"

clean:
	@echo "Cleaning up..."
#	@for port in $(BROKER_PORTS); do \
#		kill `pgrep -f "danube-broker --service-port $$port"`; \
#	done
	@echo "Cleaning up ETCD instance..."
	docker rm -f $(ETCD_NAME)
	@echo "ETCD instance removed"
#	@docker-compose -f etcd-cluster.yml down
#	@make -C target clean




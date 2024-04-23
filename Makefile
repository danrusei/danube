.DEFAULT_GOAL := no_target_specified

# Define broker ports
BROKER_PORTS := 6650 6651 6652

# ETCD configuration
ETCD_NAME := my-etcd
ETCD_DATA_DIR := ./etcd-data
ETCD_PORT := 2379

# HAProxy configuration
HAPROXY_CONFIG := haproxy.cfg
HAPROXY_PORT := 50051

# Rust build command
#BUILD_CMD := cargo build --release

.PHONY: all brokers etcd haproxy etcd-clean brokers-clean haproxy-clean

no_target_specified:
	@echo "Please specify a target to build. Available targets:"
	@echo "all           -- create the stack with etcd, brokers & haproxy"
	@echo "etcd          -- start ETCD instance in a docker container"
	@echo "etcd-clean    -- remove the ETCD instance"
	@echo "brokers       -- compile the danube-broker and listen on 6650 6651 6652 ports"
	@echo "brokers-clean -- remove the broker instances"
	@echo "haproxy       -- start an HAProxy instance with the haproxy.cfg config"
	@echo "haproxy-clean -- remove the HAProxy instance"


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

etcd-clean:
	@echo "Cleaning up ETCD instance..."
	docker rm -f $(ETCD_NAME)
	@echo "ETCD instance removed"
#	@docker-compose -f etcd-cluster.yml down
#	@make -C target clean

brokers:
	@echo "Building Danube brokers..."
	@for port in $(BROKER_PORTS); do \
		cargo build --release --package danube-broker --bin danube-broker && \
		RUST_BACKTRACE=1 ./target/release/danube-broker --service-port $$port & \
	done
	@echo "Danube brokers started on ports: $(BROKER_PORTS)"

brokers-clean:
	@echo "Cleaning up Brokers instances..."
	@for port in $(BROKER_PORTS); do \
		kill `pgrep -f "danube-broker --service-port $$port"`; \
	done

haproxy:
	@echo "Starting HAProxy..."
	@haproxy -f $(HAPROXY_CONFIG) -D
	@echo "HAProxy listening on port: $(HAPROXY_PORT)"

haproxy-clean:
	@echo "Stopping HAProxy..."
	@pkill haproxy
	@echo "Cleaning up..."





# Makefile that runs the Brokers and ETCD in docker instances
.DEFAULT_GOAL = no_target_specified

# Image name
IMAGE_NAME = docker-danube-broker

# ETCD settings
ETCD_NAME = etcd-danube
ETCD_IMAGE = bitnami/etcd:latest
ETCD_PORT1 = 2379
ETCD_PORT2 = 2380
ETCD_DATA_DIR = ./etcd-data

# Docker network
NETWORK = grpc-network

# List of BROKERs ports
BROKER_PORTS := 6650 6651 6652
BROKER_NAME = danube-broker 

.PHONY: build network etcd brokers clean

no_target_specified:
	@echo "Please specify a target to build. Available targets:"
	@echo "build         -- build the Broker docker image"
	@echo "etcd          -- start ETCD instance in a docker container"
	@echo "brokers       -- run the danube-broker instances, that listen on 6650 6651 6652 ports"
	@echo "clean         -- clean up all instances and the network"     

build:
	cp ../target/release/danube-broker danube-broker
	docker build -t $(IMAGE_NAME) .

network:
	docker network create $(NETWORK) || true

etcd: network
	docker run -d --name $(ETCD_NAME) --network $(NETWORK) \
		-p $(ETCD_PORT1):$(ETCD_PORT1) -p $(ETCD_PORT2):$(ETCD_PORT2) \
		-v $(ETCD_DATA_DIR):/bitnami/etcd/data \
		--env ALLOW_NONE_AUTHENTICATION=yes \
		--env ETCD_ADVERTISE_CLIENT_URLS=http://$(ETCD_NAME):$(ETCD_PORT1) \
		$(ETCD_IMAGE)

brokers: network
	@for port in $(BROKER_PORTS); do \
    	name=$(BROKER_NAME)_$$port; \
    	docker run -d --name $$name --network $(NETWORK) \
        	-p $$port:50051 $(IMAGE_NAME) \
        	$$port; \
    done

clean:
	docker rm -f $(ETCD_NAME) || true
	@for port in $(BROKER_PORTS); do \
		docker rm -f $(BROKER_NAME)_$$port || true; \
	done
	docker network rm $(NETWORK) || true
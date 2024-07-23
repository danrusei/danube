# Start with the official Rust image
FROM rust:latest as builder

# Set the working directory
WORKDIR /app

# Install protobuf-compiler
RUN apt-get update && apt-get install -y protobuf-compiler

# Copy the project files
COPY . .

# Build the project
RUN cargo build --release

# Use a smaller base image for the final image
FROM debian:buster-slim

# Install protobuf-compiler in the final image as well
RUN apt-get update && apt-get install -y protobuf-compiler

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/danube-broker /usr/local/bin/danube-broker

# Expose the ports your broker listens on
EXPOSE 6650 6651

# Define entrypoint and default command
ENTRYPOINT ["/usr/local/bin/danube-broker"]
CMD ["--broker-addr", "0.0.0.0:6650", "--cluster-name", "MY_CLUSTER", "--meta-store-addr", "0.0.0.0:2379"]
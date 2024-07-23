# Start with the official Rust image
FROM rust:latest as builder

# Set the working directory
WORKDIR /app

# Copy the project files
COPY . .

# Build the project
RUN cargo build --release

# Use a smaller base image for the final image
FROM debian:buster-slim

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/danube-broker /usr/local/bin/danube-broker

# Expose the port your broker listens on
EXPOSE 6650 6651

# Command to run your broker
CMD ["danube-broker"]
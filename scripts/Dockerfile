# Use a minimal base image
FROM debian:bullseye-slim

# Set the working directory
WORKDIR /app

# Copy the Rust binary into the container
COPY danube-broker /app/danube-broker

# Make the binary executable
RUN chmod +x /app/danube-broker

# Define the entry point and default command
ENTRYPOINT ["/app/danube-broker"]
CMD ["--server_addr", "0.0.0.0:50051"]

# Expose the port (default is 50051, you can change this)
EXPOSE 50051
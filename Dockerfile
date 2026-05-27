FROM openjdk:17-jdk-slim AS build

# Install sbt
RUN apt-get update && apt-get install -y curl gnupg && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | \
      gpg --dearmor -o /etc/apt/keyrings/scalasbt.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/scalasbt.gpg] https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    apt-get update && apt-get install -y sbt && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files into the container
COPY . /app

# Set JAVA_HOME
ENV JAVA_HOME=/usr/local/openjdk-17

# Build the assembly JAR
RUN export TERM=xterm-color && sbt -mem 8192 migrator/assembly

FROM scratch AS final
COPY --from=build /app/migrator/target/scala-2.13/scylla-migrator-assembly.jar .


# Entry point or CMD can be your spark-submit command or left blank
CMD ["bash"]

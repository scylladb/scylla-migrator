FROM openjdk:11-jdk-slim AS build

# Install sbt
RUN apt-get update && apt-get install -y curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add && \
    apt-get update && apt-get install -y sbt && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files into the container
COPY . /app

# Set JAVA_HOME
ENV JAVA_HOME=/usr/local/openjdk-11

# Run build.sh to produce the jar
RUN chmod +x build.sh && ./build.sh

FROM scratch AS final
COPY --from=build /app/migrator/target/scala-2.13/scylla-migrator-assembly.jar .


# Entry point or CMD can be your spark-submit command or left blank
CMD ["bash"]

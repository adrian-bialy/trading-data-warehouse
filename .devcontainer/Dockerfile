FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install required packages including software-properties-common for add-apt-repository
RUN apt-get update && \
    apt-get install -y sudo apt-transport-https make build-essential ca-certificates curl gnupg lsb-release wget bzip2 git postgresql-client software-properties-common && \
    rm -rf /var/lib/apt/lists/*

# Add deadsnakes PPA and install Python 3.11 along with development packages
RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y python3.11 python3.11-dev python3.11-distutils && \
    rm -rf /var/lib/apt/lists/*

# Update alternatives: set python3 to point to python3.11 over python3.10
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 2

# Install Docker and the Docker Compose plugin
RUN apt-get update && \
    apt-get install -y apt-transport-https ca-certificates curl lsb-release && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - && \
    echo "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin && \
    rm -rf /var/lib/apt/lists/*

# Install OpenJDK 11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Create a non-root user "devuser" with sudo access
RUN groupadd -g 1000 devuser && \
    useradd -m -u 1000 -g devuser devuser && \
    echo 'devuser ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    chsh -s /bin/bash devuser

WORKDIR /workspace
USER devuser

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Ensure Poetry is in the PATH
ENV PATH="/home/devuser/.local/bin:${PATH}"

# Install the Poetry shell plugin
RUN poetry self add poetry-plugin-shell

CMD ["bash"]

# Trading Data Warehouse

Trading Data Warehouse is a CLI tool for ingesting and transforming trading data from various sources.

## Prerequisites

- Python 3.11
- Docker
- [Poetry](https://python-poetry.org/) for dependency management
- [PySpark](https://spark.apache.org/)
- OpenJDK 11

## Using Preconfigured .devcontainer

A preconfigured VS Code dev container is available to simplify your setup. This container includes the necessary settings and extensions preinstalled, so you can focus on development without manual configuration.

1. Ensure the `.devcontainer` folder is present at the root of your project.
2. Open the project in VS Code, and when prompted, select "Reopen in Container" to launch the preconfigured development environment.

This setup will automatically prepare your environment with Python 3.11, the required dependencies, and VS Code settings optimized for the Trading Data Warehouse project.


## Getting Started

1. **Clone the Repository**

    ```sh
    git clone https://github.com/adrian-bialy/trading-data-warehouse.git

    cd trading-data-warehouse
    ```
2. **Install Dependencies**

    *Note*: If you are using the preconfigured `.devcontainer`, all dependencies required for both development and running the application are automatically handled.

    ```sh
    poetry install
    poetry shell
    ```
3. **Configure the Application**
    - Rename `config.yaml.example` to `config.yaml` and adjust it to fit your settings.
    - Create `.env` file based on `.env.example` with your API keys and database credentials.
4. **Run docker compose**

    This command will start a Postgres container where the trading data will be loaded. Make sure your configuration files are set up to connect your application to this database.
    ```sh
    sudo docker compose up -d
    ```
## CLI Overview

The CLI supports two primary actions: **ingest** and **transform**.

### Ingest Data

To ingest data, execute the following command. For example, to ingest tickers for the rapid_yahoo source:

```sh
python -m tdw.main --action ingest --dataset rapid_yahoo.tickers
```

Alternatively, use the provided launch configuration from `launch.json.example` in your IDE.

### Transform Data

This section is to be implemented.

## Contributing
Thank you for your interest in contributing to Trading Data Warehouse! We welcome contributions from the community. Please read through this guide before submitting a pull request.

1. **Clone the Repository**

   For new contributors, fork the repository on GitHub, then clone your fork locally and checkout the `develop` branch.

   For existing contributors, you can clone the main repository and switch to the `develop` branch.

   ```sh
   git clone https://github.com/adrian-bialy/trading-data-warehouse.git
   cd trading-data-warehouse
   git checkout develop
   ```
2. **Development Environment Setup**  
   - **Using VS Code Dev Container**:  
     Open the project in VS Code and choose "Reopen in Container" when prompted.
   - **Configuration**:  
     Rename `config.yaml.example` to `config.yaml` and `.env.example` to `.env`. Adjust the configuration to fit your environment.

### Branching and Pull Requests

- **Feature Branches**  
  Create a new branch for your changes:
  ```sh
  git checkout -b feature/your-feature-name
  ```

- **Commit Messages**  
  Use clear, descriptive commit messages. Refer to [Conventional Commits](https://www.conventionalcommits.org/) for message guidelines.

- **Submitting Pull Requests**  
  Push your branch and open a pull request (PR) against the `develop` branch. Include a detailed description of your changes and reference any related issues.

### Testing

- **Running Tests**  
  Run the test suite to ensure everything passes:
  ```sh
  make test
  ```

- **Adding New Tests**  
  When adding features or fixing bugs, please include unit tests that cover your changes.

### Coding Standards and Best Practices

- Use linters to maintain consistency:
  ```sh
  make black lint flake
  ```
### Before committing
  You can combine lint and test with the command:
  ```sh
  make precommit
  ```
## Reporting Issues

If you encounter any bugs or have feature requests, please open an issue on GitHub.  
Include as much detail as possible:
- A clear and descriptive title.
- Steps to reproduce and logs or screenshots, if applicable.
- Expected versus actual behavior.

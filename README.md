
# Data Pipeline with Apache Airflow

This repository provides a simple data pipeline setup using Apache Airflow to process CSV data, perform data cleaning, and load the cleaned data into a PostgreSQL database. With this repository, you can quickly set up the data pipeline environment using Docker Compose. Below are the steps to get started:

## Prerequisites

Before you begin, make sure you have Docker and Docker Compose installed on your machine.

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Usage

1. **Clone the Repository**

   Clone this repository to your local machine:

   ```bash
   git clone https://github.com/mohamedelmarzouqi/airflowPipelineToPostgres.git

2. **Navigate to the Repository**

   Change your working directory to the cloned repository:

   ```bash
   cd airflowPipelineToPostgres

3. **Build and Start the Docker Compose Services**

   Use Docker Compose to build and start the services defined in the **docker-compose.yml** file:

   ```bash
   docker-compose up --build
   

## Data Pipeline Configuration

-The Apache Airflow DAG (Directed Acyclic Graph) for the data pipeline is defined in the **dags/** directory. You can modify this DAG to customize your data processing tasks.

-The config/ directory holds configuration files, including database connection details, and Airflow configurations.

Data files (e.g., CSV files) can be placed in the data/ directory for processing.

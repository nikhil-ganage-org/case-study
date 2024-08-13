# Architecture Diagram
![Diagram](Diagram.svg)

---

# Data Warehouse Diagram
![Diagram](Datawarehouse_Diagram.svg)

---

# Crunchbase to Parquet API Integration

## Prerequisites

- Docker
- Crunchbase API key

## Building the Docker Image

1. Clone the repository.

2. Navigate to the directory with the `Dockerfile`.

3. Build the Docker image:

   ```bash
   docker build -t crunchbase-parquet-integration .
   ```

## Running the Docker Container

Run the container, passing the Crunchbase API key as an environment variable:

```bash
docker run -e CRUNCHBASE_API_KEY=your_api_key crunchbase-parquet-integration
```

This will execute the script, retrieve the data, and save it as a Parquet file inside the container.

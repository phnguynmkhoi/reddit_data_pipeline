# Reddit Data Extraction Pipeline

This project is a fully containerized ELT (Extract, Load, Transform) pipeline that automates the extraction of top posts from a specific Reddit subreddit. The pipeline leverages Docker, Apache Airflow for orchestration, Apache Spark for data transformation, and MinIO as an S3-compatible object storage solution.

## Project Overview

The data pipeline workflow is as follows:
1. **Extract**: Use the Reddit API to fetch top posts from a specified subreddit.
2. **Load**: Store the raw data in a MinIO bucket, organized in the `raw_data` folder.
3. **Transform**: Use Spark to clean and structure the data, saving the results to the `transformed_data` folder.
4. **Archive**: Move the processed data from `raw_data` to the `archived_data` folder in MinIO.

The entire pipeline is managed by Airflow and uses a Spark cluster for scalable data processing. The project runs on Docker Compose, making setup and deployment straightforward.

## Technologies Used

- **Airflow**: Manages the ELT pipeline workflow and scheduling.
- **Spark**: Handles data transformations.
- **MinIO**: S3-compatible storage for holding raw, transformed, and archived data.
- **Docker & Docker Compose**: Containerizes and orchestrates all services, allowing easy deployment.

## Pipeline Workflow

1. **Reddit Data Extraction**: A script uses the Reddit API to pull top posts from a specified subreddit.
2. **Raw Data Loading**: Extracted data is saved to the MinIO `raw` folder.
3. **Data Transformation**: Spark transforms the raw data (e.g., data cleaning, normalization) and stores the result as parquet for optimize storage and compression in the `transformed` folder.
4. **Data Archiving**: Raw data is moved from `raw` to `archived` in MinIO to free up space and maintain a historical record.

![datapipeline](https://github.com/user-attachments/assets/11ebb93d-2bc0-47b5-946b-44ea55460c47)

## Running the Project

1. Clone the repository:
    ```bash
    git clone https://github.com/phnguynmkhoi/reddit_data_pipeline.git
    cd reddit_data_pipeline
    ```

2. Create .env file and add your Reddit API credentials in the appropriate configuration file. 

3. Build the Docker images:
    ```bash
    docker build -f Dockerfile.airflow -t airflow-default .
    docker build -f Dockerfile.spark -t spark-cluster:3.1.3 .
    ```

4. Run the entire pipeline:
    ```bash
    docker-compose up
    ```

5. The pipeline will start running based on the Airflow scheduler. You can monitor the tasks through the Airflow UI at `http://localhost:8080`.

## Accessing MinIO

- **MinIO Console**: [http://localhost:10000](http://localhost:10000)
- Use the credentials in your `.env` or Docker Compose file to log in.
- Also remember to create key and paste it to your `.env` file 

## Airflow UI

Airflow’s web server is accessible at [http://localhost:8080](http://localhost:8080), where you can monitor, manage, and troubleshoot DAG runs.

## Data Output

- **Raw Data**: Available in MinIO under the `raw` folder.
- **Transformed Data**: Available in MinIO under the `transformed` folder.
- **Archived Data**: Processed data is moved from `raw` to `archived` once the pipeline completes.

## Notes

- Ensure that the MinIO and Reddit API credentials are configured before running the pipeline.
- You can create additional Spark jobs for more advanced data transformations and aggregations based on the extracted data.
- This project can be scaled and extended to handle multiple subreddits or different data sources.

## Future Enhancements

- **Data Validation**: Implement data validation checks in the transformation step to improve data quality.
- **Pipeline Split**: Split the project into two parts by creating a trigger mechanism in MinIO. When new data is loaded to MinIO, a trigger can prompt Airflow to automatically run the Spark job for data transformation, providing more flexibility beyond the current automated schedule.
- **Notification System**: Add a notification system to report the status of each pipeline stage.
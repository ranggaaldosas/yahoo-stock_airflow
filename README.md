### Project ETL Stock IDX - Stream Processing

> End-to-end data pipeline with Extract-Load-Transform concept

- **Data Stack**
  - Orchestration = Apache Airlow
  - Connector = Airbyte
  - Data Transformation = dbt
  - OLAP/Data Warehouse = Google BigQuery
  - Stream Processing = Google Pub/Sub


#### A. System Design
![system_design](./assets/)

#### B. Airflow Graph
![flow](./assets/airflow_graph_stockmarket.png)

#### C. Postgres Data Warehouse
![dwh](./assets/postgres_dwh.png)

#### D. Metabase Dashboard 
![metabase](./assets/metabase_dashboard.png)
![metabase](./assets/metabase_dashboard_dark.png)

#### E. Slack Notification 
1. Setup Slack Token -> https://api.slack.com/apps
2. Airflow Dashboard -> Admin -> Connection -> Slack API
3. add on_failure_callback and on_success_callback on your own DAG
4. Documentation -> https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/notifications/index.html

![dwh](./assets/slack_notification.png)
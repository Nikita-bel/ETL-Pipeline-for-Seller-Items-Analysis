# **ETL Pipeline for Seller Analytics**  
**Airflow + Spark on Kubernetes + Greenplum**  

This project implements an **ETL pipeline** to process seller item data, compute business metrics, and load results into Greenplum for analytics.  

## **🚀 Key Components**  
- **Apache Airflow**: Orchestrates the workflow  
- **Spark on Kubernetes**: Distributed data processing  
- **Greenplum**: Analytical SQL database  
- **S3**: Data lake storage  

## **🔧 Pipeline Workflow**  
1. **Spark Processing** (`job_submit` task):  
   - Reads Parquet data from S3 (`s3a://startde-project/nikita-belov-cbx8663/seller_items`)  
   - Computes metrics:  
     - Revenue calculations (`total_revenue`, `potential_revenue`)  
     - Return estimates (`returned_items_count`)  
     - Inventory turnover (`days_to_sold`)  
   - Writes processed data back to S3  

2. **Greenplum Integration** (`items_datamart` task):  
   - Creates an **external table** linked to S3 data  
   - Generates analytical `unreliable_sellers_view` identifying:  
     - Sellers with stock aging >100 days  
     - Suspicious order patterns  

## **⚙️ Requirements**  
- **Airflow 2.5+** with providers:  
  ```bash
  pip install apache-airflow-providers-cncf-kubernetes apache-airflow-providers-common-sql
  ```
- **Spark 3.3+** running on Kubernetes  
- **Greenplum** with PXF configured for S3 access  

## **🛠 Configuration**  
1. **Airflow Connections**:  
   - `kubernetes_karpov`: Kubernetes cluster access  
   - `greenplume_karpov`: Greenplum credentials  

2. **Data Locations**:  
   - Input/Output: `s3a://startde-project/nikita-belov-cbx8663/seller_items`  

3. **Spark Resources**:  
   - Configured in `spark_submit.yaml` (CPU/memory settings)  

## **🔒 Security Note**  
- **Never commit** credentials in code  
- Store secrets in:  
  - Airflow Variables  
  - Vault/Secret Manager  
- Add `spark_submit.yaml` to `.gitignore`  

## **🛠️ Potential Improvements**  
- Add unit/integration tests  
- Implement incremental data loading  
- Set up Airflow alerts for job failures  
- Add data quality checks (e.g., Great Expectations)  


## 📌 Project Overview
This project demonstrates end-to-end data analytics of Sales details using Microsoft Fabric. I have used Sales record  data, stored in a Fabric Data Lakehouse, and transform it through staging and presentation layers using Data Factory activities, used notebook to transform the data , and Pipeline to run Sequentually
## 🛠 Tools Used
- Microsoft Fabric
- Power BI
- SQL
- Warehouse
- Pyspark
- JSON configuration
- Lakehouse

## 📊 Project Architecture
1. Data Ingestion
2. Data Transformation
3. Semantic Model Creation
4. Power BI Reporting

## 📁 Project Files
- Pyspark scripts for transformation
- Power BI dashboard (.pbix)


 ## Code used in Notebook: raw_to_Landing

Overall Explanation

Raw to Landing (Data Ingestion Layer)

The raw CSV source file is stored in the Lakehouse under Files/raw. A PySpark notebook reads the raw file, performs basic validation (schema check, null handling if applicable), and writes the output to the Landing folder.

A Microsoft Fabric Pipeline is used to trigger and manage the ingestion process, ensuring automated and repeatable data movement from Raw to Landing.

This layer acts as the initial ingestion checkpoint before data is processed into the Bronze layer.


Latest Processed Data
For the Script Activity “Latest Processed Data”

```
file_name = '2016 Feb.csv'
processed_date = '2026-02-13'
adfs_path = 'abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Files/raw'
full_path = f"{adfs_path}/{file_name}"
print(full_path)
df = spark.read.csv(path= full_path,header=True,inferSchema=True)
display(df)
from pyspark.sql.functions import lit
if df.count() > 1 :
     df_new = df.withColumn("Processed_Date",lit(processed_date))
     df_new.write.format('csv').option("header","true").mode("append").partitionBy("Processed_Date").save("abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Files/Landing")
     display("Data loading to Landing Zone sucessfully")
else:
    display("file contain no data")
    display(df_new)
```

### Pipeline for Raw To Landing
A ForEach activity is used in the Fabric Pipeline to dynamically iterate over all files available in the Raw folder. The pipeline fetches the file list and executes the Notebook for each file individually, enabling automated and scalable data ingestion.

This design supports batch file processing and eliminates the need for hardcoded file references.

<img width="1184" height="568" alt="image" src="https://github.com/user-attachments/assets/ecf6f046-28eb-42bf-901e-709da307d6af" />


 ## Code used in Notebook: Landing_to_Bronze

Overall Explanation
In this step, data is moved from the Landing layer (Lakehouse Files) to the Bronze layer (Lakehouse Tables) using a Spark notebook. The notebook reads the input files from the landing folder by defining the file path and format. A new schema named tbl_sales_bronze is created to logically organize the Bronze tables. The data is then loaded into structured Delta tables with minimal transformation, preserving the raw structure for further processing in the Silver layer.

```
abfs_path = "abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Files/Landing"
partition_path = f"/Processed_Date={today_date}"
file_path = abfs_path+partition_path
display(file_path)
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,VarcharType,DoubleType,ArrayType,DateType
v_schema = StructType([
    StructField("Row_ID",StringType(),True),
    StructField("Order_ID",StringType(),True),
    StructField("Order_Date",DateType(),True),
    StructField("Ship_Date",DateType(),True),
    StructField("Ship_Mode",StringType(),True),
    StructField("Customer_ID",StringType(),True),
    StructField("Customer_Name",StringType(),True),
    StructField("Segment",StringType(),True),
    StructField("Postal_Code",StringType(),True),
    StructField("City",StringType(),True),    
    StructField("State",StringType(),True),  
    StructField("Country",StringType(),True),     
    StructField("Region",StringType(),True),     
    StructField("Market",StringType(),True), 
    StructField("Product_ID",StringType(),True), 
    StructField("Category",StringType(),True),     
    StructField("Sub_Category",StringType(),True),  
    StructField("Product_Name",StringType(),True),     
    StructField("Sales",DoubleType(),True),     
    StructField("Quantity",IntegerType(),True), 
    StructField("Discount",DoubleType(),True), 
    StructField("Profit",DoubleType(),True),  
    StructField("Shipping_Cost",DoubleType(),True), 
    StructField("Order_Priority",StringType(),True), 
    StructField("Month",StringType(),True), 
    StructField("Year",StringType(),True)
])
df = spark.read.format('csv').option("header","True").schema(v_schema).load(file_path)
display(df)
df.createOrReplaceTempView("t_new_data")
Fabric_tblSales_bronze = "abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Tables/dbo/tblsales_bronze"
try:
    spark.read.format('delta').load(Fabric_tblSales_bronze).createOrReplaceTempView('tblsales_bronze')

except:
    v_create_table = f"""CREATE TABLE IF NOT EXISTS tblsales_bronze (
Row_ID    string ,
Order_ID    string ,
Order_Date   date ,
Ship_Date   date ,
Ship_Mode    string ,
Customer_ID    string ,
Customer_Name    string ,
Segment    string ,
Postal_Code    string ,
City    string ,    
State    string ,  
Country    string ,     
Region    string ,     
Market    string , 
Product_ID    string , 
Category    string ,     
Sub_Category    string ,  
Product_Name    string ,     
Sales  DOUBLE ,     
Quantity  int , 
Discount  DOUBLE , 
Profit  DOUBLE ,  
Shipping_Cost  DOUBLE , 
Order_Priority    string , 
Month    string , 
Year    string ,
processed_date date
)"""

spark.sql(v_create_table)
spark.read.format('delta').load(Fabric_tblSales_bronze).createOrReplaceTempView('tblsales_bronze')

 sql_statement = f"""merge into tblsales_bronze as target using 
                    t_new_data as source 
                    on target.Order_ID = source.Order_ID and target.Customer_ID = source.Customer_ID
                    WHEN MATCHED THEN 
                    UPDATE SET 
                    target.Row_ID = source.Row_ID,
                    target.Order_ID = source.Order_ID,
                    target.Order_Date = source.Order_Date,
                    target.Ship_Date = source.Ship_Date,
                    target.Ship_Mode  = source.Ship_Mode ,
                    target.Customer_ID = source.Customer_ID,
                    target.Customer_Name = source.Customer_Name,
                    target.Segment = source.Segment,
                    target.Postal_Code  = source.Postal_Code ,
                    target.City = source.City,
                    target.State = source.State,
                    target.Country = source.Country,
                    target.Region = source.Region,
                    target.Market = source.Market,
                    target.Product_ID   = source.Product_ID  ,
                    target.Category = source.Category,
                    target.Sub_Category = source.Sub_Category,
                    target.Product_Name = source.Product_Name,
                    target.Sales = source.Sales,
                    target.Quantity = source.Quantity,
                    target.Discount = source.Discount,
                    target.Profit   = source.Profit  ,
                    target.Shipping_Cost = source.Shipping_Cost,
                    target.Order_Priority = source.Order_Priority,
                    target.Month = source.Month,
                    target.Year = source.Year,
                    target.processed_date = '{today_date}'
                WHEN NOT MATCHED THEN 
                    INSERT (
                    Row_ID   ,   
                    Order_ID   ,   
                    Order_Date     ,
                    Ship_Date     ,
                    Ship_Mode    ,  
                    Customer_ID   ,   
                    Customer_Name  ,    
                    Segment      ,
                    Postal_Code   ,   
                    City          ,
                    State        ,
                    Country       ,    
                    Region         ,  
                    Market       ,
                    Product_ID    ,  
                    Category        ,   
                    Sub_Category   ,     
                    Product_Name    ,       
                    Sales     ,    
                    Quantity    , 
                    Discount     , 
                    Profit     ,  
                    Shipping_Cost     , 
                    Order_Priority   ,    
                    Month ,      
                    Year   ,   
                    processed_date     
                           ) VALUES (
                           source.Row_ID  ,    
source.Order_ID      ,
source.Order_Date     ,
source.Ship_Date     ,
source.Ship_Mode      ,
source.Customer_ID     , 
source.Customer_Name    ,  
source.Segment      ,
source.Postal_Code   ,   
source.City          ,
source.State        ,
source.Country       ,    
source.Region         ,  
source.Market       ,
source.Product_ID    ,   
source.Category       ,   
source.Sub_Category     ,   
source.Product_Name    ,       
source.Sales     ,     
source.Quantity    , 
source.Discount     , 
source.Profit     ,  
source.Shipping_Cost     , 
source.Order_Priority,       
source.Month      , 
source.Year      ,
'{today_date}'
)"""
spark.sql(sql_statement).show()                

```


 ## Code used in Notebook: Bronze_to_Silver

Overall Explanation
In this step, data is moved from the Bronze layer to the Silver layer where business transformations are applied. The notebook removes duplicate records, handles null values, and performs data cleansing to improve quality. Additional derived columns such as delivery_date (calculated from order and shipping dates) and profit_margin are created to enrich the dataset. A new schema named tblsales_silver is created, and the transformed data is loaded into Silver tables using a MERGE operation to support incremental updates.

```
Fabric_bronze_path = 'abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Tables/dbo/tblsales_bronze'

from pyspark.sql.functions import col
df = spark.read.format('delta').load(Fabric_bronze_path).filter(col("Processed_date")==str(today_date))
display(df)

### Data cleaning 

### Handling Duplicates
print('Before removing Duplicates',df.count())
df_distinct = df.dropDuplicates()
print('After removing Duplicates',df.count())


### Drop rows with missing values 

df_nonNull = df_distinct.dropna(subset=['Order_ID','Customer_ID'])
## Business Transformation 
## calculating Delivery days


df_Dev_date = df_nonNull.withColumn("Delivery_Days",(col('Ship_date')-col('Order_Date')).cast('int'))

display(df_Dev_date)

df_profit_margin = df_Dev_date.withColumn("Profit_Margin",(col('Profit')/col('Sales')))
display(df_profit_margin)

df_profit_margin.createOrReplaceTempView('t_tblsales_Silver')

Fabric_tblSales_Silver = "abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Tables/dbo/tblsales_silver"
try:
    spark.read.format('delta').load(Fabric_tblSales_Silver).createOrReplaceTempView('tblsales_Silver')

except:
    v_create_table = f"""CREATE TABLE IF NOT EXISTS tblsales_Silver (
Row_ID    string ,
Order_ID    string ,
Order_Date   date ,
Ship_Date   date ,
Ship_Mode    string ,
Customer_ID    string ,
Customer_Name    string ,
Segment    string ,
Postal_Code    string ,
City    string ,    
State    string ,  
Country    string ,     
Region    string ,     
Market    string , 
Product_ID    string , 
Category    string ,     
Sub_Category    string ,  
Product_Name    string ,     
Sales  DOUBLE ,     
Quantity  int , 
Discount  DOUBLE , 
Profit  DOUBLE ,  
Shipping_Cost  DOUBLE , 
Order_Priority    string , 
Month    string , 
Year    string ,
processed_date date,
Delivery_Days int,
Profit_Margin DOUBLE
)"""

spark.sql(v_create_table)
spark.read.format('delta').load(Fabric_tblSales_Silver).createOrReplaceTempView('tblsales_Silver')


sql_statement = f"""merge into tblsales_silver as target using 
                    t_tblsales_Silver as source 
                    on target.Order_ID = source.Order_ID and target.Customer_ID = source.Customer_ID
                    WHEN MATCHED THEN 
                    UPDATE SET 
                    target.Row_ID = source.Row_ID,
                    target.Order_ID = source.Order_ID,
                    target.Order_Date = source.Order_Date,
                    target.Ship_Date = source.Ship_Date,
                    target.Ship_Mode  = source.Ship_Mode ,
                    target.Customer_ID = source.Customer_ID,
                    target.Customer_Name = source.Customer_Name,
                    target.Segment = source.Segment,
                    target.Postal_Code  = source.Postal_Code ,
                    target.City = source.City,
                    target.State = source.State,
                    target.Country = source.Country,
                    target.Region = source.Region,
                    target.Market = source.Market,
                    target.Product_ID   = source.Product_ID  ,
                    target.Category = source.Category,
                    target.Sub_Category = source.Sub_Category,
                    target.Product_Name = source.Product_Name,
                    target.Sales = source.Sales,
                    target.Quantity = source.Quantity,
                    target.Discount = source.Discount,
                    target.Profit   = source.Profit  ,
                    target.Shipping_Cost = source.Shipping_Cost,
                    target.Order_Priority = source.Order_Priority,
                    target.Month = source.Month,
                    target.Year = source.Year,
                    target.processed_date = source.processed_date,
                    target.Delivery_Days = source.Delivery_Days,
                    target.Profit_Margin = source.Profit_Margin
                WHEN NOT MATCHED THEN 
                    INSERT (
                    Row_ID   ,   
                    Order_ID   ,   
                    Order_Date     ,
                    Ship_Date     ,
                    Ship_Mode    ,  
                    Customer_ID   ,   
                    Customer_Name  ,    
                    Segment      ,
                    Postal_Code   ,   
                    City          ,
                    State        ,
                    Country       ,    
                    Region         ,  
                    Market       ,
                    Product_ID    ,  
                    Category        ,   
                    Sub_Category   ,     
                    Product_Name    ,       
                    Sales     ,    
                    Quantity    , 
                    Discount     , 
                    Profit     ,  
                    Shipping_Cost     , 
                    Order_Priority   ,    
                    Month ,      
                    Year   ,   
                    processed_date   ,
                    Delivery_Days  ,
                    Profit_Margin
                           ) VALUES (
                           source.Row_ID  ,    
source.Order_ID      ,
source.Order_Date     ,
source.Ship_Date     ,
source.Ship_Mode      ,
source.Customer_ID     , 
source.Customer_Name    ,  
source.Segment      ,
source.Postal_Code   ,   
source.City          ,
source.State        ,
source.Country       ,    
source.Region         ,  
source.Market       ,
source.Product_ID    ,   
source.Category       ,   
source.Sub_Category     ,   
source.Product_Name    ,       
source.Sales     ,     
source.Quantity    , 
source.Discount     , 
source.Profit     ,  
source.Shipping_Cost     , 
source.Order_Priority,       
source.Month      , 
source.Year      ,
source.processed_date ,
source.Delivery_Days ,
source.Profit_Margin
)"""
spark.sql(sql_statement).show()                



```

 ## Code used in Notebook: Silver_To_Gold

Overall Explanation

In the Gold layer, data is transformed into a business-ready model for reporting and analytics. The notebook reads the cleansed data from the Silver tables and performs data modeling based on a star schema approach. The dataset is split into one Fact table and two Dimension tables: Dim_Product and Dim_Customer. Separate schemas are created to logically organize the Gold layer tables. Surrogate keys and structured definitions are applied while creating the dimension tables. A MERGE operation is used to load and maintain incremental data updates in both dimension and fact tables. This layer ensures optimized query performance and supports analytical reporting use cases. The final Gold tables serve as the single source of truth for dashboards and business insights.

<img width="898" height="598" alt="image" src="https://github.com/user-attachments/assets/0fd48afe-6d7e-4317-90ee-6fa11cc4f5ea" />


```
from pyspark.sql.functions import col 
df = spark.read.format('delta').load("abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Tables/dbo/tblsales_silver").filter(col('processed_date')==str(today_date))


from pyspark.sql.types import *

from delta.tables import DeltaTable


v_structure_Schema = StructType([
    StructField('Customer_ID',StringType(),True) ,
    StructField('Customer_Name',StringType(),True) ,
    StructField('Segment',StringType(),True) ,
    StructField('Postal_Code',StringType(),True) ,
    StructField('City',StringType(),True) ,
    StructField('State',StringType(),True) ,
    StructField('Country',StringType(),True) ,
    StructField('Region',StringType(),True) ,
    StructField('Market',StringType(),True) 
])

DeltaTable.createIfNotExists(spark).tableName('Dim_Customer').addColumns(v_structure_Schema).execute()

from pyspark.sql.types import *


v_structure_Schema1 = StructType([
    StructField('Product_ID',StringType(),True) ,
    StructField('Catagory',StringType(),True) ,
    StructField('Sub_Catagory',StringType(),True) ,
    StructField('Product_Name',StringType(),True) 
])

DeltaTable.createIfNotExists(spark).tableName('Dim_Product').addColumns(v_structure_Schema1).execute()


v_dim_fact_Schema = StructType([
    StructField("Row_ID",StringType(),True),
    StructField("Order_ID",StringType(),True),
    StructField("Customer_ID",StringType(),True),
    StructField("Customer_Name",StringType(),True),
    StructField("Order_Date",DateType(),True),
    StructField("Ship_Date",DateType(),True),
    StructField("Ship_Mode",StringType(),True),
    StructField("Sales",DoubleType(),True),     
    StructField("Quantity",IntegerType(),True), 
    StructField("Discount",DoubleType(),True), 
    StructField("Profit",DoubleType(),True),  
    StructField("Shipping_Cost",DoubleType(),True), 
    StructField("Order_Priority",StringType(),True), 
    StructField("Month",StringType(),True), 
    StructField("Year",StringType(),True),
    StructField("Processed_Date",StringType(),True),
    StructField("Product_ID",StringType(),True),
    StructField("Delivery_Date",StringType(),True),    
    StructField("Profit_Margin",StringType(),True)  
])

DeltaTable.createIfNotExists(spark).tableName('Fact_Sales').addColumns(v_dim_fact_Schema).execute()


df_Dim_customer = (df.select(
    'Customer_ID',
    'Customer_Name' ,
    'Segment',
    'Postal_Code',
    'City',
    'State' ,
    'Country' ,
    'Region' ,
    'Market').dropDuplicates())


from delta.tables import *
fabric_dim_customer = "abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Tables/dbo/dim_customer"
dim_customer = DeltaTable.forPath(spark,fabric_dim_customer)


dim_customer.alias('target').merge(
df_Dim_customer.alias('source'),'target.Customer_ID = source.Customer_ID'
).whenMatchedUpdate(
    set = {'target.Customer_Name':'source.Customer_Name',
    'target.Segment':'source.Segment',
    'target.Postal_Code':'source.Postal_Code',
    'target.City':'source.City',
    'target.State':'source.State',
    'target.Country':'source.Country',
    'target.Region':'source.Region',
    'target.Market':'source.Market'
}).whenNotMatchedInsert(
     values={
        'Customer_ID':'source.Customer_ID',
        'Customer_Name':'source.Customer_Name',
    'Segment':'source.Segment',
    'Postal_Code':'source.Postal_Code',
    'City':'source.City',
    'State':'source.State',
    'Country':'source.Country',
    'Region':'source.Region',
    'Market':'source.Market'
     }
).execute()


history_df = dim_customer.history()
display(history_df)
operation_metrics = history_df.select("operationMetrics").collect()[0][0]
display(operation_metrics)
rows_inserted = operation_metrics.get("numTargetRowsInserted",0)
rows_updated = operation_metrics.get("numTargetRowsUpdated",0)
rows_deleted = operation_metrics.get("numTargetRowsDeleted",0)
rows_impacted = int(rows_inserted)+int(rows_updated)+int(rows_deleted)

print('Total rows of Table',dim_customer.toDF().count())
print(f"Rows Inserted : {rows_inserted} ")
print(f"Rows Updated : {rows_updated} ")
print(f"Rows deleted : {rows_deleted} ")
print(f"Rows impacted : {rows_impacted} ")



df_Dim_product = (df.select(
    'Product_ID',
    'Category' ,
    'Sub_Category',
    'Product_Name').dropDuplicates())

from delta.tables import *
fabric_dim_Product = "abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Tables/dbo/dim_product"
dim_Product = DeltaTable.forPath(spark,fabric_dim_Product)


dim_Product.alias('target').merge(
df_Dim_product.alias('source'),'target.Product_ID = source.Product_ID'
).whenMatchedUpdate(
    set = {
    'target.Catagory':'source.Category',
    'target.Sub_Catagory':'source.Sub_Category',
    'target.Product_Name':'source.Product_Name'
}).whenNotMatchedInsert(
     values={
'Product_ID':'source.Product_ID',
    'Catagory':'source.Category',
    'Sub_Catagory':'source.Sub_Category',
    'Product_Name':'source.Product_Name'
     }
).execute()


history_df = dim_Product.history()
display(history_df)
operation_metrics = history_df.select("operationMetrics").collect()[0][0]
display(operation_metrics)
rows_inserted = operation_metrics.get("numTargetRowsInserted",0)
rows_updated = operation_metrics.get("numTargetRowsUpdated",0)
rows_deleted = operation_metrics.get("numTargetRowsDeleted",0)
rows_impacted = int(rows_inserted)+int(rows_updated)+int(rows_deleted)

print('Total rows of Table',dim_customer.toDF().count())
print(f"Rows Inserted : {rows_inserted} ")
print(f"Rows Updated : {rows_updated} ")
print(f"Rows deleted : {rows_deleted} ")
print(f"Rows impacted : {rows_impacted} ")


df_Fact_sales = (df.select(
    'Row_ID',
    'Order_ID',
    'Customer_ID',
    'Customer_Name',
    'Order_Date',
    'Ship_Date',
    'Ship_Mode',
    'Sales',     
    'Quantity', 
    'Discount', 
    'Profit',  
    'Shipping_Cost', 
    'Order_Priority', 
    'Month', 
    'Year',
    'Processed_Date',
    'Product_ID',
    'Delivery_Days',    
    'Profit_Margin').dropDuplicates())


from delta.tables import *
fabric_dim_Fact_Sales = "abfss://Sales_workspace@onelake.dfs.fabric.microsoft.com/Sales_lakehouse.Lakehouse/Tables/dbo/fact_sales"
dim_fact = DeltaTable.forPath(spark,fabric_dim_Fact_Sales)


dim_fact.alias('target').merge(
df_Fact_sales.alias('source'),'target.Order_ID = source.Order_ID'
).whenMatchedUpdate(
    set = {'target.Row_ID':'source.Row_ID',
    #'target.Order_ID':'source.Order_ID',
    'target.Customer_ID':'source.Customer_ID',
    'target.Customer_Name':'source.Customer_Name',
    'target.Order_Date':'source.Order_Date',
    'target.Ship_Date':'source.Ship_Date',
    'target.Ship_Mode':'source.Ship_Mode',
    'target.Sales':'source.Sales',
    'target.Quantity':'source.Quantity',
    'target.Discount':'source.Discount',
    'target.Profit':'source.Profit',
    'target.Shipping_Cost':'source.Shipping_Cost',
    'target.Order_Priority':'source.Order_Priority',
    'target.Month':'source.Month',
    'target.Year':'source.Year',
    'target.Processed_Date':'source.Processed_Date',
    'target.Delivery_Date':'source.Delivery_Days',
    'target.Profit_Margin':'source.Profit_Margin'
}).whenNotMatchedInsert(
     values={
'target.Row_ID':'source.Row_ID',
    'target.Order_ID':'source.Order_ID',
    'target.Customer_ID':'source.Customer_ID',
    'target.Customer_Name':'source.Customer_Name',
    'target.Order_Date':'source.Order_Date',
    'target.Ship_Date':'source.Ship_Date',
    'target.Ship_Mode':'source.Ship_Mode',
    'target.Sales':'source.Sales',

    'target.Quantity':'source.Quantity',
    'target.Discount':'source.Discount',
    'target.Profit':'source.Profit',
    'target.Shipping_Cost':'source.Shipping_Cost',
    'target.Order_Priority':'source.Order_Priority',
    'target.Month':'source.Month',
    'target.Year':'source.Year',
    'target.Processed_Date':'source.Processed_Date',
    'target.Delivery_Date':'source.Delivery_Days',
    'target.Profit_Margin':'source.Profit_Margin'
     }
).execute()

history_df = dim_fact.history()
display(history_df)
operation_metrics = history_df.select("operationMetrics").collect()[0][0]
display(operation_metrics)
rows_inserted = operation_metrics.get("numTargetRowsInserted",0)
rows_updated = operation_metrics.get("numTargetRowsUpdated",0)
rows_deleted = operation_metrics.get("numTargetRowsDeleted",0)
rows_impacted = int(rows_inserted)+int(rows_updated)+int(rows_deleted)

print('Total rows of Table',dim_fact.toDF().count())
print(f"Rows Inserted : {rows_inserted} ")
print(f"Rows Updated : {rows_updated} ")
print(f"Rows deleted : {rows_deleted} ")
print(f"Rows impacted : {rows_impacted} ")



```

## Final pipeline Orcestration:

The final pipeline orchestrates the complete end-to-end data flow using pipeline activities. An Invoke Pipeline activity is used to trigger the Raw-to-Landing pipeline as the initial step. Upon successful completion of this pipeline, the remaining transformation notebooks (Landing-to-Bronze, Bronze-to-Silver, and Silver-to-Gold) are executed sequentially. This dependency-based execution ensures proper data flow across all layers of the Medallion architecture. The orchestration design improves modularity, reusability, and maintainability of the overall data engineering workflow.

<img width="2220" height="410" alt="image" src="https://github.com/user-attachments/assets/97eb2053-c6b4-4a1c-869f-18d95230e372" />


## 📷 Dashboard Preview

In the final step, a Power BI report was developed using the Gold layer tables as the data source. The report includes interactive visualizations such as sales trends by order date, sales distribution by shipping mode, and key performance indicators like total sales. The fact and dimension tables created in the Gold layer were used to build a structured data model for reporting. These visualizations provide business insights into sales performance, customer behavior, and product distribution. This reporting layer represents the final consumption layer of the end-to-end Medallion architecture pipeline.

<img width="2618" height="1402" alt="image" src="https://github.com/user-attachments/assets/a5f0d70d-c6c8-4d3f-b819-92123e8d0441" />




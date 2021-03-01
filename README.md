# glue-studio-tutorial
Tutorial for creating an ETL job to extract, filter, join, and aggregate data using AWS Glue Studio. https://aws.amazon.com/blogs/big-data/making-etl-easier-with-aws-glue-studio/

A CloudFormation stack in launched to create the required resources. Then the Glue Studio drag and drop tool is used to generate the PySpark ETL code. Once the job is run, Athena tables are available to query for this dataset.

Below is the diagram of the ETL job in the Glue Studio interface.

![](https://user-images.githubusercontent.com/22268825/109463425-18e2fe80-7ab9-11eb-97a0-e24f2fa952c4.png?raw=true)

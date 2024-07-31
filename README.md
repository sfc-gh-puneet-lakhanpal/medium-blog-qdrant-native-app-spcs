# Set up Distributed multi-node Qdrant vector DB as Native App on Snowpark Container Services (SPCS)

## Prerequisites
* Install Snowflake CLI and configure it. I created a file at location ~/.snowflake/config.toml with the following entry:
```
[connections.default]
account = "account"
user = "username"
password = "password"
```
* Docker Desktop

## Steps
### As a native app provider
1. Open up a terminal and type `cd 01_provider`. 
2. Create a role: Open up SnowSight and create a new worksheet. Copy contents from `step0_setup.sql` into the worksheet and run all cells. This will create a new role qdrant_provider_role and grant some privileges.
3. Create objects using the role: Copy the contents from `step1_create_objects.sql` into the Snowsight worksheet. These SQL statements will use the qdrant_provider_role created earlier and then create a database, schema, warehouse and an image repository where the qdrant images will be stored.
4. Perform docker build, tag and push: Using a terminal window, follow the steps mentioned in `step2_docker_stuff.md` to perform docker build, tag and pushes of all relevant images into the Snowpark Container Services image repository.
5. Deploy qdrant app on provider account: Using a terminal window, follow steps mentioned in the `step3_deploy_app.md` to deploy the app on a provider account.
6. Next, go to SnowSight -> Projects -> App Packages and create a version/patch for the app. Once the version/patch is available, the app can be shared to consumer accounts through a live listing and performing a direct private listing share to another snowflake account. 

### As a native app consumer
1. Consumer account prerequisites: Open up Snowsight on the consumer Snowflake account. Copy the contents from `02_consumer\step0_setup.sql` and execute in the consumer account. These steps will create a new qdrant_consumer_role and grant privileges to the role.
2. Follow the steps highlighted in this video to install the native app in the consumer account: https://www.youtube.com/watch?v=Hzp1xuiDx2k
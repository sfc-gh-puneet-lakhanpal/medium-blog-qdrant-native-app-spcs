CREATE APPLICATION ROLE IF NOT EXISTS qdrant_app_role;

CREATE SCHEMA IF NOT EXISTS qdrant_app_core_schema;
GRANT USAGE ON SCHEMA qdrant_app_core_schema TO APPLICATION ROLE qdrant_app_role;

CREATE OR ALTER VERSIONED SCHEMA qdrant_app_public_schema;
GRANT USAGE ON SCHEMA qdrant_app_public_schema TO APPLICATION ROLE qdrant_app_role;


CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.get_qdrant_service_status(service_type STRING)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_qdrant_service_status'
AS
$$
from snowflake.snowpark import functions as F
def get_qdrant_service_status(session, service_type):
   service_name = ''
   servicestatuses = ['No Service']
   if service_type.lower() == 'primary' or service_type.lower() == 'driver':
      if service_type.lower() == 'primary':
         service_name = 'qdrant_app_core_schema.QDRANTPRIMARYSERVICE'
      elif service_type.lower() == 'driver':
         service_name = 'qdrant_app_core_schema.QDRANTDRIVERSERVICE'
      try:
         sql = f"""SELECT VALUE:status::VARCHAR as SERVICESTATUS FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{service_name}')), outer => true)) f"""
         servicestatuses = [row['SERVICESTATUS'] for row in session.sql(sql).collect()]
      except:
         pass
   elif service_type.lower() == 'secondary':
      servicestatuses = []
      secondary_service_names = session.table('qdrant_app_core_schema.SERVICE_NAME_TRACKER').filter("SERVICE_TYPE = 'secondary'").select('SERVICE_NAME').to_pandas()['SERVICE_NAME'].tolist()
      if len(secondary_service_names)>0:
         initial_service_name = secondary_service_names[0]
         snowdf = session.sql(f"SELECT VALUE:status::VARCHAR as SERVICESTATUS FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{initial_service_name}')), outer => true)) f")
         for service_name in secondary_service_names[1:]:
            snowdf = snowdf.union_all(session.sql(f"SELECT VALUE:status::VARCHAR as SERVICESTATUS FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{service_name}')), outer => true)) f"))
         servicestatuses = [service['SERVICESTATUS'] for service in snowdf.collect()]
   return servicestatuses
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.get_qdrant_service_status(STRING) TO APPLICATION ROLE qdrant_app_role;


CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.has_app_been_initialized()
RETURNS BOOLEAN
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'has_app_been_initialized'
AS
$$
from snowflake.snowpark import functions as F
def has_app_been_initialized(session):
   exists = False
   try:
      result = session.table('qdrant_app_core_schema.APP_INITIALIZATION_TRACKER').collect()[0]['VALUE']
      if result == True:
         return True
   except:
      pass
   return exists
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.has_app_been_initialized() TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.recreate_qdrant_compute_pool_and_service(service_type STRING, qdrant_primary_instance_type STRING, qdrant_secondary_instance_type STRING, qdrant_driver_instance_type STRING, query_warehouse STRING, num_instances INTEGER)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'recreate_qdrant_compute_pool_and_service'
AS
$$
from snowflake.snowpark import functions as F
from snowflake.snowpark import Session
from enum import Enum
COMPUTE_POOL_CORES_MAPPING = {'CPU_X64_XS': 1,'CPU_X64_S': 3,'CPU_X64_M': 6,'CPU_X64_L': 28,'HIGHMEM_X64_S': 6,'HIGHMEM_X64_M': 28,'HIGHMEM_X64_L': 124,'GPU_NV_S': 6,'GPU_NV_M': 44,'GPU_NV_L': 92}
VALID_INSTANCE_TYPES = list(COMPUTE_POOL_CORES_MAPPING.keys())
COMPUTE_POOL_GPU_MAPPING = {'CPU_X64_XS': None,'CPU_X64_S': None,'CPU_X64_M': None,'CPU_X64_L': None,'HIGHMEM_X64_S': None,'HIGHMEM_X64_M': None,'HIGHMEM_X64_L': None,'GPU_NV_S': 1,'GPU_NV_M': 4,'GPU_NV_L': 8}
COMPUTE_POOL_MEMORY_MAPPING = {'CPU_X64_XS': 6,'CPU_X64_S': 13,'CPU_X64_M': 28,'CPU_X64_L': 116,'HIGHMEM_X64_S': 58,'HIGHMEM_X64_M': 240,'HIGHMEM_X64_L': 984,'GPU_NV_S': 27,'GPU_NV_M': 178,'GPU_NV_L': 1112}
CPU_CONSTANT = 'CPU'
GPU_CONSTANT = 'GPU'
HIGH_MEM_CONSTANT = 'HIGH_MEM'
INSTANCE_TYPE_MAPPING = {'CPU_X64_XS': CPU_CONSTANT,'CPU_X64_S': CPU_CONSTANT,'CPU_X64_M': CPU_CONSTANT,'CPU_X64_L': CPU_CONSTANT,'HIGHMEM_X64_S': CPU_CONSTANT,'HIGHMEM_X64_M': CPU_CONSTANT,'HIGHMEM_X64_L': CPU_CONSTANT,'GPU_NV_S': GPU_CONSTANT,'GPU_NV_M': GPU_CONSTANT,'GPU_NV_L': GPU_CONSTANT}
DHSM_MEMORY_FACTOR = 0.1
INSTANCE_AVAILABLE_MEMORY_FOR_QDRANT_FACTOR = 0.8
MIN_DSHM_MEMORY = 11
SNOW_OBJECT_IDENTIFIER_MAX_LENGTH = 255
CP_OBJECT_IDENTIFIER_MAX_LENGTH=63
class ResourceStatus(Enum):
    UNKNOWN = "UNKNOWN"  # status is unknown because we have not received enough data from K8s yet.
    PENDING = "PENDING"  # resource set is being created, can't be used yet
    READY = "READY"  # resource set has been deployed.
    DELETING = "DELETING"  # resource set is being deleted
    FAILED = "FAILED"  # resource set has failed and cannot be used anymore
    DONE = "DONE"  # resource set has finished running
    NOT_FOUND = "NOT_FOUND"  # not found or deleted
    INTERNAL_ERROR = "INTERNAL_ERROR"  # there was an internal service error.
def execute_sql(session, sql):
   _ = session.sql(sql).collect()
def get_valid_object_identifier_name(name:str, object_type:str="cp"):
   result = ""
   if object_type.lower() == 'cp':
      result = str(name).upper()[0:CP_OBJECT_IDENTIFIER_MAX_LENGTH-1]
   else:
	   result = str(name).upper()[0:SNOW_OBJECT_IDENTIFIER_MAX_LENGTH-1]
   return result
def get_query_warehouse_config(session, query_warehouse):
   config = {}
   if query_warehouse=='XS':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'XSMALL'
   elif query_warehouse=='S':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'SMALL'
   elif query_warehouse=='M':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'MEDIUM'
   elif query_warehouse=='L':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'LARGE'
   elif query_warehouse=='XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'XLARGE'
   elif query_warehouse=='2XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'XXLARGE'
   elif query_warehouse=='3XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'XXXLARGE'
   elif query_warehouse=='4XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'X4LARGE'
   elif query_warehouse=='5XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'X5LARGE'
   elif query_warehouse=='6XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'X6LARGE'
   elif query_warehouse=='MSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'MEDIUM'
   elif query_warehouse=='LSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'LARGE'
   elif query_warehouse=='XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'XLARGE'
   elif query_warehouse=='2XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'XXLARGE'
   elif query_warehouse=='3XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'XXXLARGE'
   elif query_warehouse=='4XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'X4LARGE'
   elif query_warehouse=='5XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'X5LARGE'
   elif query_warehouse=='6XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'X6LARGE'
   return config
def generate_spec_for_qdrant_primary(session, dshm_memory_for_qdrant_primary, instance_available_memory_for_qdrant_primary, cp_type, gpus_available_for_qdrant_primary):
   if cp_type=='GPU':
      qdrant_primary_spec_def = f"""
spec:
   containers:
   -  name: primary
      image: /qdrant_provider_db/qdrant_provider_schema/qdrant_provider_image_repo/native_app_qdrant_primary
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /artifacts
      -  name: qdrantstorage
         mountPath: /qdrant/storage
      env:
         QDRANT_PRIMARY_ADDRESS: http://instances.qdrantprimaryservice.qdrant-app-core-schema:6335
         QDRANT__CLUSTER__ENABLED: true
         QDRANT__LOG_LEVEL: DEBUG
      resources:
         limits:
            nvidia.com/gpu: {gpus_available_for_qdrant_primary}
         requests:
            nvidia.com/gpu: {gpus_available_for_qdrant_primary}
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_qdrant_primary}
   -  name: artifacts
      source: '@qdrant_app_public_schema.ARTIFACTS'
   -  name: qdrantstorage
      source: block
      size: 200Gi
   endpoints:
   -  name: webui
      port: 6333
      public: true
   -  name: grpc
      port: 6334
      protocol: TCP
   -  name: internalcomm
      port: 6335
      protocol: TCP
   """
   else:
      qdrant_primary_spec_def = f"""
spec:
   containers:
   -  name: primary
      image: /qdrant_provider_db/qdrant_provider_schema/qdrant_provider_image_repo/native_app_qdrant_primary
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /artifacts
      -  name: qdrantstorage
         mountPath: /qdrant/storage
      env:
         QDRANT_PRIMARY_ADDRESS: http://instances.qdrantprimaryservice.qdrant-app-core-schema:6335
         QDRANT__CLUSTER__ENABLED: true
         QDRANT__LOG_LEVEL: DEBUG
      resources:
         requests:
            memory: "{instance_available_memory_for_qdrant_primary}"
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_qdrant_primary}
   -  name: artifacts
      source: '@qdrant_app_public_schema.ARTIFACTS'
   -  name: qdrantstorage
      source: block
      size: 200Gi
   endpoints:
   -  name: webui
      port: 6333
      public: true
   -  name: grpc
      port: 6334
      protocol: TCP
   -  name: internalcomm
      port: 6335
      protocol: TCP
   """
   return qdrant_primary_spec_def
def generate_spec_for_qdrant_driver(session, dshm_memory_for_qdrant_driver, instance_available_memory_for_qdrant_driver, cp_type, gpus_available_for_qdrant_driver):
   if cp_type=='GPU':
      qdrant_driver_spec_def = f"""
spec:
   containers:
   -  name: driver
      image: /qdrant_provider_db/qdrant_provider_schema/qdrant_provider_image_repo/native_app_qdrant_driver
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /artifacts
      env:
         QDRANT_PRIMARY_REST_ADDRESS: http://instances.qdrantprimaryservice.qdrant-app-core-schema:6333
         QDRANT__CLUSTER__ENABLED: true
         QDRANT__LOG_LEVEL: DEBUG
      resources:
         limits:
            nvidia.com/gpu: {gpus_available_for_qdrant_driver}
         requests:
            nvidia.com/gpu: {gpus_available_for_qdrant_driver}
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_qdrant_driver}
   -  name: artifacts
      source: '@qdrant_app_public_schema.ARTIFACTS'
   endpoints:
   -  name: notebook
      port: 8888
      public: true
   -  name: api
      port: 9000
      public: true
   """
   else:
      qdrant_driver_spec_def = f"""
spec:
   containers:
   -  name: driver
      image: /qdrant_provider_db/qdrant_provider_schema/qdrant_provider_image_repo/native_app_qdrant_driver
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /artifacts
      env:
         QDRANT_PRIMARY_REST_ADDRESS: http://instances.qdrantprimaryservice.qdrant-app-core-schema:6333
         QDRANT__CLUSTER__ENABLED: true
         QDRANT__LOG_LEVEL: DEBUG
      resources:
         requests:
            memory: "{instance_available_memory_for_qdrant_driver}"
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_qdrant_driver}
   -  name: artifacts
      source: '@qdrant_app_public_schema.ARTIFACTS'
   endpoints:
   -  name: notebook
      port: 8888
      public: true
   -  name: api
      port: 9000
      public: true
   """
   return qdrant_driver_spec_def
def generate_spec_for_qdrant_secondary(session, num_qdrant_secondaries, dshm_memory_for_qdrant_secondary, instance_available_memory_for_qdrant_secondary, cp_type, gpus_available_for_qdrant_secondary):
   qdrant_secondary_spec_def = ""
   if num_qdrant_secondaries>0:
      if cp_type=='GPU':
         qdrant_secondary_spec_def = f"""
spec:
   containers:
   -  name: secondary
      image: /qdrant_provider_db/qdrant_provider_schema/qdrant_provider_image_repo/native_app_qdrant_secondary
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /artifacts
      -  name: qdrantstorage
         mountPath: /qdrant/storage
      env:
         QDRANT_PRIMARY_ADDRESS: http://instances.qdrantprimaryservice.qdrant-app-core-schema:6335
         QDRANT_PRIMARY_REST_ADDRESS: http://instances.qdrantprimaryservice.qdrant-app-core-schema:6333
         QDRANT_SECONDARY_ADDRESS: QDRANT_SECONDARY_ADDRESS_PLACEHOLDER
         QDRANT__CLUSTER__ENABLED: true
         QDRANT__LOG_LEVEL: DEBUG
      resources:
         limits:
            nvidia.com/gpu: {gpus_available_for_qdrant_secondary}
         requests:
            nvidia.com/gpu: {gpus_available_for_qdrant_secondary}
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_qdrant_secondary}
   -  name: artifacts
      source: '@qdrant_app_public_schema.ARTIFACTS'
   -  name: qdrantstorage
      source: block
      size: 200Gi
   endpoints:
   -  name: grpc
      port: 6334
      protocol: TCP
   -  name: internalcomm
      port: 6335
      protocol: TCP
   """
      else:
         qdrant_secondary_spec_def = f"""
spec:
   containers:
   -  name: secondary
      image: /qdrant_provider_db/qdrant_provider_schema/qdrant_provider_image_repo/native_app_qdrant_secondary
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /artifacts
      -  name: qdrantstorage
         mountPath: /qdrant/storage
      env:
         QDRANT_PRIMARY_ADDRESS: http://instances.qdrantprimaryservice.qdrant-app-core-schema:6335
         QDRANT_PRIMARY_REST_ADDRESS: http://instances.qdrantprimaryservice.qdrant-app-core-schema:6333
         QDRANT_SECONDARY_ADDRESS: QDRANT_SECONDARY_ADDRESS_PLACEHOLDER
         QDRANT__CLUSTER__ENABLED: true
         QDRANT__LOG_LEVEL: DEBUG
      resources:
         requests:
            memory: "{instance_available_memory_for_qdrant_secondary}"
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_qdrant_secondary}
   -  name: artifacts
      source: '@qdrant_app_public_schema.ARTIFACTS'
   -  name: qdrantstorage
      source: block
      size: 200Gi
   endpoints:
   -  name: grpc
      port: 6334
      protocol: TCP
   -  name: internalcomm
      port: 6335
      protocol: TCP
   """
   return qdrant_secondary_spec_def

def get_resource_status(session:Session, resource_name:str):
   import json
   try:
      row = session.sql(f"CALL SYSTEM$GET_SERVICE_STATUS('{resource_name}');").collect()
   except Exception:
      # Silent fail as SPCS status call is not guaranteed to return in time. Will rely on caller to retry.
      return None

   resource_metadata = json.loads(row[0]["SYSTEM$GET_SERVICE_STATUS"])[0]
   if resource_metadata and resource_metadata["status"]:
      try:
         status = resource_metadata["status"]
         return ResourceStatus(status)
      except ValueError:
         pass
   return None

def block_until_resource_is_ready(session:Session, resource_name: str, max_retries: int = 180, retry_interval_secs: int = 10) -> None:
   import time
   for attempt_idx in range(max_retries):
      status = get_resource_status(session=session, resource_name=resource_name)
      if status == ResourceStatus.READY:
         return
      if (
         status
         in [
               ResourceStatus.FAILED,
               ResourceStatus.NOT_FOUND,
               ResourceStatus.INTERNAL_ERROR,
               ResourceStatus.DELETING,
         ]
         or attempt_idx >= max_retries - 1
      ):
         error_message = "failed"
         if attempt_idx >= max_retries - 1:
               error_message = "does not reach ready/done status"
         raise Exception(f"Service {resource_name} {error_message}." f"\nStatus: {status if status else ''} \n")
      time.sleep(retry_interval_secs)
def create_service(session, service_type, service_name, cp_name, qdrant_primary_instance_type, qdrant_secondary_instance_type, qdrant_driver_instance_type, num_instances, query_warehouse_name):
   if num_instances>0:
      entry_name = ''
      cp_type = ''
      qdrant_num_instances = 0
      create_specs_sql = "CREATE TABLE IF NOT EXISTS qdrant_app_core_schema.YAML (name varchar, value varchar)"
      execute_sql(session, create_specs_sql)
      cpu_or_gpu=''
      if service_type.lower() == 'primary':
         entry_name = 'QDRANT_PRIMARY'
         cp_type = qdrant_primary_instance_type
         instance_available_memory_for_qdrant_primary = str(int(INSTANCE_AVAILABLE_MEMORY_FOR_QDRANT_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])) + 'G'
         dshm = int(DHSM_MEMORY_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])
         if dshm < MIN_DSHM_MEMORY:
            dshm = MIN_DSHM_MEMORY
         dshm_memory_for_qdrant_primary = str(MIN_DSHM_MEMORY) + 'Gi'
         cpu_or_gpu = INSTANCE_TYPE_MAPPING[cp_type]
         gpus_available_for_qdrant_primary = COMPUTE_POOL_GPU_MAPPING[cp_type]
         qdrant_service_spec = generate_spec_for_qdrant_primary(session, dshm_memory_for_qdrant_primary, instance_available_memory_for_qdrant_primary, cpu_or_gpu, gpus_available_for_qdrant_primary)
         delete_prior_service_spec_sql = "DELETE FROM qdrant_app_core_schema.YAML where NAME = 'QDRANT_PRIMARY'"
         execute_sql(session, delete_prior_service_spec_sql)
         qdrant_primary_snow_df = session.create_dataframe([['QDRANT_PRIMARY', qdrant_service_spec]], schema=["NAME", "VALUE"])
         qdrant_primary_snow_df.write.save_as_table("qdrant_app_core_schema.YAML", mode="append")
         create_service_sql = f"""CREATE SERVICE {service_name}
            IN COMPUTE POOL {cp_name}
            FROM SPECIFICATION 
            {chr(36)}{chr(36)}
            {qdrant_service_spec}
            {chr(36)}{chr(36)}
            EXTERNAL_ACCESS_INTEGRATIONS = (reference('external_access_reference'))
            QUERY_WAREHOUSE={query_warehouse_name}
         """
         execute_sql(session, create_service_sql)
         qdrant_service_name_snow_df = session.create_dataframe([[f'{service_type.lower()}', f'{service_name}']], schema=["SERVICE_TYPE", "SERVICE_NAME"])
         qdrant_service_name_snow_df.write.save_as_table("qdrant_app_core_schema.SERVICE_NAME_TRACKER", mode="append")
         execute_sql(session, f"GRANT USAGE ON SERVICE {service_name} TO APPLICATION ROLE qdrant_app_role")
         execute_sql(session, f"GRANT MONITOR ON SERVICE {service_name} TO APPLICATION ROLE qdrant_app_role")
         execute_sql(session, f"GRANT SERVICE ROLE {service_name}!ALL_ENDPOINTS_USAGE TO APPLICATION ROLE qdrant_app_role")
      elif service_type.lower() == 'secondary':
         entry_name = 'QDRANT_SECONDARY'
         cp_type = qdrant_secondary_instance_type
         qdrant_num_instances = num_instances
         instance_available_memory_for_qdrant_secondary = str(int(INSTANCE_AVAILABLE_MEMORY_FOR_QDRANT_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])) + 'G'
         dshm = int(DHSM_MEMORY_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])
         if dshm < MIN_DSHM_MEMORY:
            dshm = MIN_DSHM_MEMORY
         dshm_memory_for_qdrant_secondary = str(MIN_DSHM_MEMORY) + 'Gi'   
         delete_prior_service_spec_sql = "DELETE FROM qdrant_app_core_schema.YAML where NAME = 'QDRANT_SECONDARY'"
         execute_sql(session, delete_prior_service_spec_sql)
         cpu_or_gpu = INSTANCE_TYPE_MAPPING[cp_type]
         gpus_available_for_qdrant_secondary = COMPUTE_POOL_GPU_MAPPING[cp_type]
         qdrant_service_spec = generate_spec_for_qdrant_secondary(session, num_instances, dshm_memory_for_qdrant_secondary, instance_available_memory_for_qdrant_secondary, cpu_or_gpu, gpus_available_for_qdrant_secondary)
         qdrant_secondary_snow_df = session.create_dataframe([['QDRANT_SECONDARY', qdrant_service_spec]], schema=["NAME", "VALUE"])
         qdrant_secondary_snow_df.write.save_as_table("qdrant_app_core_schema.YAML", mode="append")
         #block_until_resource_is_ready(session=session, resource_name='qdrant_app_core_schema.QDRANTPRIMARYSERVICE')
         for instancenum in range(0, qdrant_num_instances):
            qdrant_service_spec = generate_spec_for_qdrant_secondary(session, num_instances, dshm_memory_for_qdrant_secondary, instance_available_memory_for_qdrant_secondary, cpu_or_gpu, gpus_available_for_qdrant_secondary)
            service_name = f'qdrant_app_core_schema.QDRANTSECONDARYSERVICE{instancenum}'
            service_address=f'http://instances.qdrantsecondaryservice{instancenum}.qdrant-app-core-schema:6335'
            qdrant_service_spec = qdrant_service_spec.replace('QDRANT_SECONDARY_ADDRESS_PLACEHOLDER', f'{service_address}')
            create_service_sql = f"""CREATE SERVICE {service_name}
               IN COMPUTE POOL {cp_name}
               FROM SPECIFICATION 
               {chr(36)}{chr(36)}
               {qdrant_service_spec}
               {chr(36)}{chr(36)}
               EXTERNAL_ACCESS_INTEGRATIONS = (reference('external_access_reference'))
               QUERY_WAREHOUSE={query_warehouse_name}
               """
            execute_sql(session, create_service_sql)
            qdrant_service_name_snow_df = session.create_dataframe([[f'{service_type.lower()}', f'{service_name}']], schema=["SERVICE_TYPE", "SERVICE_NAME"])
            qdrant_service_name_snow_df.write.save_as_table("qdrant_app_core_schema.SERVICE_NAME_TRACKER", mode="append")
            execute_sql(session, f"GRANT USAGE ON SERVICE {service_name} TO APPLICATION ROLE qdrant_app_role")
            execute_sql(session, f"GRANT MONITOR ON SERVICE {service_name} TO APPLICATION ROLE qdrant_app_role")
            execute_sql(session, f"GRANT SERVICE ROLE {service_name}!ALL_ENDPOINTS_USAGE TO APPLICATION ROLE qdrant_app_role")
      elif service_type.lower() == 'driver':
         entry_name = 'QDRANT_DRIVER'
         cp_type = qdrant_driver_instance_type
         qdrant_num_instances = 1
         instance_available_memory_for_qdrant_driver = str(int(INSTANCE_AVAILABLE_MEMORY_FOR_QDRANT_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])) + 'G'
         dshm = int(DHSM_MEMORY_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])
         if dshm < MIN_DSHM_MEMORY:
            dshm = MIN_DSHM_MEMORY
         dshm_memory_for_qdrant_driver = str(MIN_DSHM_MEMORY) + 'Gi'
         cpu_or_gpu = INSTANCE_TYPE_MAPPING[cp_type]
         gpus_available_for_qdrant_driver = COMPUTE_POOL_GPU_MAPPING[cp_type]
         qdrant_service_spec = generate_spec_for_qdrant_driver(session, dshm_memory_for_qdrant_driver, instance_available_memory_for_qdrant_driver, cpu_or_gpu, gpus_available_for_qdrant_driver)
         delete_prior_service_spec_sql = "DELETE FROM qdrant_app_core_schema.YAML where NAME = 'QDRANT_DRIVER'"
         execute_sql(session, delete_prior_service_spec_sql)
         qdrant_driver_snow_df = session.create_dataframe([['QDRANT_DRIVER', qdrant_service_spec]], schema=["NAME", "VALUE"])
         qdrant_driver_snow_df.write.save_as_table("qdrant_app_core_schema.YAML", mode="append")
         create_service_sql = f"""CREATE SERVICE {service_name}
         IN COMPUTE POOL {cp_name}
         FROM SPECIFICATION 
         {chr(36)}{chr(36)}
         {qdrant_service_spec}
         {chr(36)}{chr(36)}
         MIN_INSTANCES={qdrant_num_instances}
         MAX_INSTANCES={qdrant_num_instances}
         EXTERNAL_ACCESS_INTEGRATIONS = (reference('external_access_reference'))
         QUERY_WAREHOUSE={query_warehouse_name}
         """
         execute_sql(session, create_service_sql)
         qdrant_service_name_snow_df = session.create_dataframe([[f'{service_type.lower()}', f'{service_name}']], schema=["SERVICE_TYPE", "SERVICE_NAME"])
         qdrant_service_name_snow_df.write.save_as_table("qdrant_app_core_schema.SERVICE_NAME_TRACKER", mode="append")
         execute_sql(session, f"GRANT USAGE ON SERVICE {service_name} TO APPLICATION ROLE qdrant_app_role")
         execute_sql(session, f"GRANT MONITOR ON SERVICE {service_name} TO APPLICATION ROLE qdrant_app_role")
         execute_sql(session, f"GRANT SERVICE ROLE {service_name}!ALL_ENDPOINTS_USAGE TO APPLICATION ROLE qdrant_app_role")
      else:
         return ['Please only provide one of the options: primary, driver or secondary']
      return "SUCCESS"
   else:
      return "NO SERVICE CREATED"
def is_app_initialized(session):
   try:
      result = session.table("qdrant_app_core_schema.APP_INITIALIZATION_TRACKER").collect()[0]['VALUE']
      if result == True: 
         return True
   except:
      pass
   return False
def initialize_artifacts(session, query_warehouse_type, query_warehouse_name):
   #create artifacts once
   sql1 = """create stage if not exists qdrant_app_public_schema.ARTIFACTS ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE)"""
   sql2 = """GRANT READ, WRITE ON STAGE qdrant_app_public_schema.ARTIFACTS TO APPLICATION ROLE qdrant_app_role"""
   #sql3 = """create stage if not exists qdrant_app_core_schema.QDRANTPRIMARYSTORAGE ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE)"""
   #sql4 = """GRANT READ, WRITE ON STAGE qdrant_app_core_schema.QDRANTPRIMARYSTORAGE TO APPLICATION ROLE qdrant_app_role"""
   execute_sql(session, sql1)
   execute_sql(session, sql2)
   #execute_sql(session, sql3)
   #execute_sql(session, sql4)
   query_warehouse_config = get_query_warehouse_config(session, query_warehouse_type)
   warehouse_size = query_warehouse_config['WAREHOUSE_SIZE']
   warehouse_type = query_warehouse_config['WAREHOUSE_TYPE']
   create_warehouse_sql = f"create or replace warehouse {query_warehouse_name} WAREHOUSE_SIZE={warehouse_size} WAREHOUSE_TYPE={warehouse_type} INITIALLY_SUSPENDED=TRUE AUTO_RESUME=TRUE"
   execute_sql(session, create_warehouse_sql)
   #initialize app
   initialize_service_names_sql = "CREATE OR REPLACE TABLE qdrant_app_core_schema.SERVICE_NAME_TRACKER (SERVICE_TYPE STRING, SERVICE_NAME STRING)"
   execute_sql(session, initialize_service_names_sql)
   #initialize app
   initialize_app_sql = "CREATE OR REPLACE TABLE qdrant_app_core_schema.APP_INITIALIZATION_TRACKER (VALUE boolean)"
   execute_sql(session, initialize_app_sql)
   app_initialization_df = session.create_dataframe([[True]], schema=["VALUE"])
   app_initialization_df.write.save_as_table("qdrant_app_core_schema.APP_INITIALIZATION_TRACKER", mode="append")
   qdrant_config_sql = "CREATE OR REPLACE TABLE qdrant_app_core_schema.QDRANT_CONFIG (cp_type varchar, instance_name varchar, num_instances integer, query_warehouse varchar)"
   execute_sql(session, qdrant_config_sql)
def get_qdrant_cluster_config_per_service(session, service_type):
   cp_type = ''
   if service_type.lower() == 'primary':
      cp_type = 'QDRANT_PRIMARY'
   elif service_type.lower() == 'secondary':
      cp_type = 'QDRANT_SECONDARY'
   elif service_type.lower() == 'driver':
      cp_type = 'QDRANT_DRIVER'
   else:
      return ['Please only provide one of the options: primary, driver or secondary']
   qdrant_cluster_config = {}
   try:
      qdrant_conf_snowdf = session.table('qdrant_app_core_schema.QDRANT_CONFIG')
      qdrant_conf = qdrant_conf_snowdf.filter(qdrant_conf_snowdf['CP_TYPE']==cp_type).collect()[0]
      qdrant_cluster_config["cp_type"] = cp_type
      qdrant_cluster_config["instance_name"] = qdrant_conf['INSTANCE_NAME']
      qdrant_cluster_config["num_instances"] = qdrant_conf['NUM_INSTANCES']
      qdrant_cluster_config["query_warehouse"] = qdrant_conf['QUERY_WAREHOUSE']
   except:
      pass
   return qdrant_cluster_config
def does_the_service_exist_already(session, service_type, instance_name, num_instances, query_warehouse):
   cp_type = ''
   if service_type.lower() == 'primary':
      cp_type = 'QDRANT_PRIMARY'
   elif service_type.lower() == 'secondary':
      cp_type = 'QDRANT_SECONDARY'
   elif service_type.lower() == 'driver':
      cp_type = 'QDRANT_DRIVER'
   else:
      return ['Please only provide one of the options: primary, driver or secondary']
   service_exists_already = False
   proposed_qdrant_cluster_config = {"cp_type": cp_type, "instance_name": instance_name, "num_instances": num_instances, "query_warehouse": query_warehouse}
   qdrant_cluster_config_for_service = get_qdrant_cluster_config_per_service(session, service_type)
   len_qdrant_cluster_config_for_service = len(qdrant_cluster_config_for_service)
   if (proposed_qdrant_cluster_config==qdrant_cluster_config_for_service):
      service_exists_already = True
   return service_exists_already
def generate_qdrant_config_per_service(session, service_type, instance_name, query_warehouse, num_instances):
   cp_type = ''
   if service_type.lower() == 'primary':
      cp_type = 'QDRANT_PRIMARY'
   elif service_type.lower() == 'secondary':
      cp_type = 'QDRANT_SECONDARY'
   elif service_type.lower() == 'driver':
      cp_type = 'QDRANT_DRIVER'
   else:
      return ['Please only provide one of the options: primary, driver or secondary']
   snow_df = session.create_dataframe([[cp_type, instance_name, num_instances, query_warehouse]], schema=["CP_TYPE", "INSTANCE_NAME", "NUM_INSTANCES", "QUERY_WAREHOUSE"])
   snow_df.write.save_as_table("qdrant_app_core_schema.QDRANT_CONFIG", mode="append")
def recreate_qdrant_compute_pool_and_service(session, service_type, qdrant_primary_instance_type, qdrant_secondary_instance_type, qdrant_driver_instance_type, query_warehouse, num_instances):
   if (qdrant_primary_instance_type.upper() not in VALID_INSTANCE_TYPES) or (qdrant_secondary_instance_type.upper() not in VALID_INSTANCE_TYPES) or (qdrant_driver_instance_type.upper() not in VALID_INSTANCE_TYPES):
      return f"Invalid value provided for instance_types: {qdrant_primary_instance_type}, {qdrant_secondary_instance_type}, {qdrant_driver_instance_type}"
   if num_instances<0:
      return f"Invalid value provided for num_instances: {num_instances}. Must be >=0"
   service_name = ''
   cp_name = ''
   qdrant_instance_type = ''
   qdrant_num_instances = 0
   current_database = session.get_current_database().replace('"', '')
   query_warehouse_name = get_valid_object_identifier_name(f"{current_database}_qdrant_query_warehouse")
   if service_type.lower() == 'primary':
      service_name = 'qdrant_app_core_schema.QDRANTPRIMARYSERVICE'
      cp_name = f"{current_database}_qdrant_primary_cp"
      qdrant_instance_type = qdrant_primary_instance_type
      qdrant_num_instances = 1
   elif service_type.lower() == 'secondary':
      service_name = 'qdrant_app_core_schema.QDRANTSECONDARYSERVICE'
      cp_name = f"{current_database}_qdrant_secondary_cp"
      qdrant_instance_type = qdrant_secondary_instance_type
      qdrant_num_instances = num_instances
   elif service_type.lower() == 'driver':
      service_name = 'qdrant_app_core_schema.QDRANTDRIVERSERVICE'
      cp_name = f"{current_database}_qdrant_driver_cp"
      qdrant_instance_type = qdrant_driver_instance_type
      qdrant_num_instances = 1
   else:
      return ['Please only provide one of the options: primary, driver or secondary']
   cp_name = get_valid_object_identifier_name(cp_name)
   if not is_app_initialized(session):
      initialize_artifacts(session, query_warehouse, query_warehouse_name)
   does_the_service_exist_already_response = does_the_service_exist_already(session, service_type, qdrant_instance_type, qdrant_num_instances, query_warehouse)
   if not does_the_service_exist_already_response:
      session.sql(f"alter compute pool IF EXISTS {cp_name} stop all").collect()
      session.sql(f"drop compute pool IF EXISTS {cp_name}").collect()
      if qdrant_num_instances > 0:
         sql = f"""CREATE COMPUTE POOL {cp_name}
         MIN_NODES = {qdrant_num_instances}
         MAX_NODES = {qdrant_num_instances}
         INSTANCE_FAMILY = {qdrant_instance_type}
         AUTO_RESUME = true
         AUTO_SUSPEND_SECS = 3600
         """
         session.sql(sql).collect()
         create_service(session, service_type, service_name, cp_name, qdrant_primary_instance_type, qdrant_secondary_instance_type, qdrant_driver_instance_type, qdrant_num_instances, query_warehouse_name)
      generate_qdrant_config_per_service(session, service_type, qdrant_instance_type, query_warehouse, qdrant_num_instances)
      return f"SUCCESS"
   else:
      return "NO CHANGES MADE TO THE SERVICE"
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.recreate_qdrant_compute_pool_and_service(STRING, STRING, STRING, STRING, STRING, INTEGER) TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.get_qdrant_service_specs()
RETURNS TABLE(NAME VARCHAR, VALUE VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_qdrant_service_specs'
AS
$$
def get_qdrant_service_specs(session):
   qdrant_conf_snowdf = session.table('qdrant_app_core_schema.YAML')
   return qdrant_conf_snowdf
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.get_qdrant_service_specs() TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.get_qdrant_service_status_with_message(service_type STRING)
RETURNS TABLE(SERVICESTATUS VARCHAR, SERVICEMESSAGE VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_qdrant_service_status_with_message'
AS
$$
from snowflake.snowpark import functions as F
def get_qdrant_service_status_with_message(session, service_type):
   snowdf = session.sql("select 'NOT READY' as SERVICESTATUS, 'NOT READY' AS SERVICEMESSAGE WHERE 1=0")
   if service_type.lower() == 'primary':
      service_name = 'qdrant_app_core_schema.QDRANTPRIMARYSERVICE'
      sql = f"SELECT VALUE:status::VARCHAR as SERVICESTATUS, VALUE:message::VARCHAR as SERVICEMESSAGE FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{service_name}')), outer => true)) f"
      snowdf = session.sql(sql)
   elif service_type.lower() == 'secondary':
      secondary_service_names = session.table('qdrant_app_core_schema.SERVICE_NAME_TRACKER').filter("SERVICE_TYPE = 'secondary'").select('SERVICE_NAME').to_pandas()['SERVICE_NAME'].tolist()
      if len(secondary_service_names)>0:
         initial_service_name = secondary_service_names[0]
         snowdf = session.sql(f"SELECT VALUE:status::VARCHAR as SERVICESTATUS, VALUE:message::VARCHAR as SERVICEMESSAGE FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{initial_service_name}')), outer => true)) f")
         for service_name in secondary_service_names[1:]:
            snowdf = snowdf.union_all(session.sql(f"SELECT VALUE:status::VARCHAR as SERVICESTATUS, VALUE:message::VARCHAR as SERVICEMESSAGE FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{service_name}')), outer => true)) f"))
   elif service_type.lower() == 'driver':
      service_name = 'qdrant_app_core_schema.QDRANTDRIVERSERVICE'
      sql = f"SELECT VALUE:status::VARCHAR as SERVICESTATUS, VALUE:message::VARCHAR as SERVICEMESSAGE FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{service_name}')), outer => true)) f"
      snowdf = session.sql(sql)
   else:
      return ['Please only provide one of the options: primary, driver or secondary']
   return snowdf
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.get_qdrant_service_status_with_message(STRING) TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.get_service_name_tracker()
RETURNS TABLE(SERVICE_NAME VARCHAR, SERVICE_TYPE VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_service_name_tracker'
AS
$$
from snowflake.snowpark import functions as F
def get_service_name_tracker(session):
   snowdf = session.table("qdrant_app_core_schema.SERVICE_NAME_TRACKER").select("SERVICE_NAME", "SERVICE_TYPE")
   return snowdf
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.get_service_name_tracker() TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.get_qdrant_public_endpoints()
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_all_endpoints'
AS
$$
def get_public_endpoints_by_service_name(session, service_name):
   public_endpoints = []
   rows = 0
   try:
      rows = session.sql(f'SHOW ENDPOINTS IN SERVICE {service_name}').collect()
   except:
      return public_endpoints
   ingress_urls = [row['ingress_url'] for row in rows]
   ingress_enabled = [row['is_public'] for row in rows]
   if any(ispublic == 'true' for ispublic in ingress_enabled) and any(url=='Endpoints provisioning in progress... check back in a few minutes' for url in ingress_urls):
      endpoint = {}
      service_name_shorted = str(service_name.split('.')[1]).lower()
      endpoint[service_name_shorted] = 'Endpoints provisioning in progress... check back in a few minutes'
      public_endpoints.append(endpoint)
      return public_endpoints
   for row in rows:
      if row['is_public'] == 'true':
         endpoint = {}
         endpoint[row['name']] = row['ingress_url']
         public_endpoints.append(endpoint)
   return public_endpoints
def get_all_endpoints(session):
   public_endpoints = []
   service_names = [row['SERVICE_NAME'] for row in session.table('qdrant_app_core_schema.SERVICE_NAME_TRACKER').select('SERVICE_NAME').collect()]
   for service_name in service_names:
      qdrant_public_endpoints = get_public_endpoints_by_service_name(session, service_name)
      if len(qdrant_public_endpoints)>0:
         public_endpoints.append(qdrant_public_endpoints)
   return public_endpoints
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.get_qdrant_public_endpoints() TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.delete_qdrant_cluster()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'delete_qdrant_cluster'
AS
$$
SNOW_OBJECT_IDENTIFIER_MAX_LENGTH = 255
CP_OBJECT_IDENTIFIER_MAX_LENGTH=63
def execute_sql(session, sql):
   _ = session.sql(sql).collect()
def get_valid_object_identifier_name(name:str, object_type:str="cp"):
   result = ""
   if object_type.lower() == 'cp':
      result = str(name).upper()[0:CP_OBJECT_IDENTIFIER_MAX_LENGTH-1]
   else:
	   result = str(name).upper()[0:SNOW_OBJECT_IDENTIFIER_MAX_LENGTH-1]
   return result
def delete_qdrant_cluster(session):
   current_database = session.get_current_database().replace('"', '')
   qdrant_primary_cp_name = get_valid_object_identifier_name(f"{current_database}_qdrant_primary_cp")
   qdrant_secondary_cp_name = get_valid_object_identifier_name(f"{current_database}_qdrant_secondary_cp")
   query_warehouse_name = get_valid_object_identifier_name(f"{current_database}_qdrant_query_warehouse")
   qdrant_specs_table_name = "qdrant_app_core_schema.YAML"
   qdrant_service_name_table_name = 'qdrant_app_core_schema.SERVICE_NAME_TRACKER'
   execute_sql(session, f"alter compute pool if exists {qdrant_primary_cp_name} stop all")
   execute_sql(session, f"alter compute pool if exists {qdrant_secondary_cp_name} stop all")
   execute_sql(session, f"drop compute pool if exists {qdrant_primary_cp_name}")
   execute_sql(session, f"drop compute pool if exists {qdrant_secondary_cp_name}")
   execute_sql(session, f"drop table if exists {qdrant_specs_table_name}")
   execute_sql(session, f"drop warehouse if exists {query_warehouse_name}")
   execute_sql(session, f"drop table if exists {qdrant_service_name_table_name}")
   delete_prior_app_initialization_sql = "DELETE FROM qdrant_app_core_schema.APP_INITIALIZATION_TRACKER where 1=1"
   _ = session.sql(delete_prior_app_initialization_sql).collect()
   app_initialization_df = session.create_dataframe([[False]], schema=["VALUE"])
   app_initialization_df.write.save_as_table("qdrant_app_core_schema.APP_INITIALIZATION_TRACKER", mode="append")
   return "SUCCESS"
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.delete_qdrant_cluster() TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.get_qdrant_cluster_config()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_qdrant_cluster_config'
AS
$$
def get_qdrant_cluster_config(session):
   qdrant_cluster_config = {}
   try:
      qdrant_conf_snowdf = session.table('qdrant_app_core_schema.QDRANT_CONFIG')
      qdrant_primary_conf = qdrant_conf_snowdf.filter(qdrant_conf_snowdf['CP_TYPE']=='QDRANT_PRIMARY').collect()[0]
      qdrant_primary_instance_family = qdrant_primary_conf['INSTANCE_NAME']
      qdrant_query_warehouse = qdrant_primary_conf['QUERY_WAREHOUSE']
      qdrant_cluster_config['qdrant_primary'] = qdrant_primary_instance_family
      qdrant_cluster_config['query_warehouse'] = qdrant_query_warehouse

      qdrant_driver_conf = qdrant_conf_snowdf.filter(qdrant_conf_snowdf['CP_TYPE']=='QDRANT_DRIVER').collect()[0]
      qdrant_driver_instance_family = qdrant_driver_conf['INSTANCE_NAME']
      qdrant_cluster_config['qdrant_driver'] = qdrant_driver_instance_family
      try:
         qdrant_secondary_conf_results = qdrant_conf_snowdf.filter(qdrant_conf_snowdf['CP_TYPE']=='QDRANT_SECONDARY').collect()
         qdrant_secondary_conf = qdrant_secondary_conf_results[0]
         qdrant_secondary_instance_family = qdrant_secondary_conf['INSTANCE_NAME']
         qdrant_secondary_num_instances = qdrant_secondary_conf['NUM_INSTANCES']
         qdrant_cluster_config['qdrant_secondary'] = [qdrant_secondary_instance_family, qdrant_secondary_num_instances]
      except:
         pass
   except:
      pass
   return qdrant_cluster_config
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.get_qdrant_cluster_config() TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.show_qdrant_cluster_config_contents()
RETURNS TABLE(cp_type varchar, instance_name varchar, num_instances integer, query_warehouse varchar)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'show_qdrant_cluster_config_contents'
AS
$$
def show_qdrant_cluster_config_contents(session):
   snowdf = session.table('qdrant_app_core_schema.QDRANT_CONFIG')
   return snowdf
$$;

GRANT USAGE ON PROCEDURE qdrant_app_public_schema.show_qdrant_cluster_config_contents() TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE PROCEDURE qdrant_app_public_schema.get_qdrant_service_logs(external_service_name STRING, container_name STRING)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_qdrant_service_logs'
AS
$$
from snowflake.snowpark import functions as F
def get_qdrant_service_logs(session, external_service_name, container_name):
   service_name = f'qdrant_app_core_schema.{external_service_name}'
   try:
      sql1 = f"""SELECT VALUE:status::VARCHAR as SERVICESTATUS FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{service_name}')), outer => true)) f"""
      servicestatuses = [row['SERVICESTATUS'] for row in session.sql(sql1).collect()]
      if any(status == 'PENDING' for status in servicestatuses):
         return "Logs unavailable since service is in pending status"
      sql2 = f"""select system$GET_SERVICE_LOGS('{service_name}', 0, '{container_name}', 1000) as LOG"""
      return session.sql(sql2).collect()[0]['LOG']
   except:
      return "Service does not exist"
$$;
GRANT USAGE ON PROCEDURE qdrant_app_public_schema.get_qdrant_service_logs(STRING, STRING) TO APPLICATION ROLE qdrant_app_role;

-- Create callbacks called in the manifest.yml
CREATE OR REPLACE PROCEDURE qdrant_app_core_schema.register_single_callback(ref_name STRING, operation STRING, ref_or_alias STRING)
RETURNS STRING
LANGUAGE SQL
AS 
$$
  BEGIN
    CASE (operation)
      WHEN 'ADD' THEN
        SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
      WHEN 'REMOVE' THEN
        SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
      WHEN 'CLEAR' THEN
        SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
    ELSE
      RETURN 'unknown operation: ' || operation;
    END CASE;
  END;
$$;

GRANT USAGE ON PROCEDURE qdrant_app_core_schema.register_single_callback(STRING, STRING, STRING) TO APPLICATION ROLE qdrant_app_role;

-- Configuration callback for the `EXTERNAL_ACCESS_REFERENCE` defined in the manifest.yml
-- The procedure returns a json format object containing information about the EAI to be created, that is
-- and show the same information in a popup-window in the UI.
-- There are no allowed_secrets since the API doesn't require authentication.
CREATE OR REPLACE PROCEDURE qdrant_app_core_schema.get_configuration(ref_name STRING)
RETURNS STRING
LANGUAGE SQL
AS 
$$
BEGIN
  CASE (UPPER(ref_name))
      WHEN 'EXTERNAL_ACCESS_REFERENCE' THEN
          RETURN OBJECT_CONSTRUCT(
              'type', 'CONFIGURATION',
              'payload', OBJECT_CONSTRUCT(
                  'host_ports', ARRAY_CONSTRUCT('0.0.0.0:443','0.0.0.0:80'),
                  'allowed_secrets', 'NONE')
          )::STRING;
      ELSE
          RETURN '';
  END CASE;
END;	
$$;

GRANT USAGE ON PROCEDURE qdrant_app_core_schema.get_configuration(STRING) TO APPLICATION ROLE qdrant_app_role;

CREATE OR REPLACE STREAMLIT qdrant_app_public_schema.qdrant_config_streamlit_app
     FROM '/code_artifacts/streamlit'
     MAIN_FILE = '/streamlit_app.py';

GRANT USAGE ON SCHEMA qdrant_app_public_schema TO APPLICATION ROLE qdrant_app_role;
GRANT USAGE ON STREAMLIT qdrant_app_public_schema.qdrant_config_streamlit_app TO APPLICATION ROLE qdrant_app_role;


-- The rest of this script is left blank for purposes of your learning and exploration. 

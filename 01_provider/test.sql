use role qdrant_provider_role;
use database qdrant_on_spcs_app;
use warehouse qdrant_provider_wh;

call qdrant_app_public_schema.get_qdrant_service_status_with_message('primary');
call qdrant_app_public_schema.get_qdrant_service_logs('QDRANTPRIMARYSERVICE', 'primary');

call qdrant_app_public_schema.get_qdrant_service_status_with_message('secondary');



call qdrant_app_public_schema.get_qdrant_service_status_with_message('driver');

call qdrant_app_public_schema.get_qdrant_service_logs('QDRANTDRIVERSERVICE', 'driver');

call  qdrant_app_public_schema.get_qdrant_cluster_config();
ls @qdrant_app_core_schema.QDRANTSTORAGE;

call qdrant_app_public_schema.get_service_name_tracker();



CALL qdrant_app_public_schema.delete_qdrant_cluster();

CALL qdrant_app_public_schema.recreate_qdrant_compute_pool_and_service('primary', 'CPU_X64_L', 'CPU_X64_L', 'CPU_X64_XS', 'L', 1);
show compute pools like '%_qdrant_primary_cp%';

call qdrant_app_public_schema.get_qdrant_service_status_with_message('primary');
call qdrant_app_public_schema.get_qdrant_service_logs('QDRANTPRIMARYSERVICE', 'primary');
CALL qdrant_app_public_schema.recreate_qdrant_compute_pool_and_service('driver', 'CPU_X64_L', 'CPU_X64_L', 'CPU_X64_XS', 'L', 1);
call qdrant_app_public_schema.get_qdrant_service_status_with_message('driver');
call qdrant_app_public_schema.get_qdrant_service_logs('QDRANTDRIVERSERVICE', 'driver');

CALL qdrant_app_public_schema.recreate_qdrant_compute_pool_and_service('secondary', 'CPU_X64_L', 'CPU_X64_L', 'CPU_X64_XS', 'L', 3);
call qdrant_app_public_schema.get_qdrant_service_status_with_message('secondary');



SELECT VALUE:status::VARCHAR as SERVICESTATUS FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('QDRANT_APP_CORE_SCHEMA.QDRANTSECONDARYSERVICE0')), outer => true)) f;

call qdrant_app_public_schema.get_qdrant_service_logs('QDRANTSECONDARYSERVICE0', 'secondary');

call qdrant_app_public_schema.get_qdrant_primary_service_status();

call qdrant_app_public_schema.get_qdrant_service_logs('QDRANTSECONDARYSERVICE1', 'secondary');
call qdrant_app_public_schema.get_qdrant_service_logs('QDRANTSECONDARYSERVICE2', 'secondary');
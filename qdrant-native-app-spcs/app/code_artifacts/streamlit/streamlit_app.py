import streamlit as st
st.set_page_config(layout="wide")
from dataclasses import dataclass
from snowflake.snowpark.context import get_active_session
import snowflake.permissions as permission
from snowflake.snowpark.functions import call_udf, lit
import json
session = get_active_session()
cpu_instance_type_dict = {
    'CPU_X64_XS': 0, 
    'CPU_X64_S': 1, 
    'CPU_X64_M': 2, 
    'CPU_X64_L': 3, 
    'HIGHMEM_X64_S': 4, 
    'HIGHMEM_X64_M': 5, 
    'HIGHMEM_X64_L': 6
    }
driver_instance_type_dict = {
    'CPU_X64_XS': 0, 
    'CPU_X64_S': 1, 
    'CPU_X64_M': 2, 
    'CPU_X64_L': 3, 
    'HIGHMEM_X64_S': 4, 
    'HIGHMEM_X64_M': 5, 
    'HIGHMEM_X64_L': 6,
    'GPU_NV_S': 7, 
    'GPU_NV_M': 8, 
    'GPU_NV_L': 9
}
warehouse_type_dict = {
    'XS': 0,
    'S': 1,
    'M': 2,
    'L': 3,
    'XL': 4,
    '2XL': 5,
    '3XL': 6,
    '4XL': 7,
    '5XL': 8,
    '6XL': 9,
    'MSOW': 10,
    'LSOW': 11,
    'XLSOW': 12,
    '2XLSOW': 13,
    '3XLSOW': 14,
    '4XLSOW': 15,
    '5XLSOW': 16,
    '6XLSOW': 17  
}
cpu_instance_types = list(cpu_instance_type_dict.keys())
driver_instance_types = list(driver_instance_type_dict.keys())
warehouse_types = list(warehouse_type_dict.keys())
current_database = session.get_current_database().replace('"', '')
@dataclass
class Reference:
    name: str
    label: str
    type: str
    description: str
    bound_alias: str
def setup():
    refs = get_references()
    for ref in refs:
        name = ref.name
        label = ref.label
        if not ref.bound_alias:
            st.header("First-time setup")
            st.caption("""
            Follow the instructions below to set up your application.
            Once you have completed the steps, you will be able to continue to the main app.
            """)
            st.button(f"{label} ↗", on_click=permission.request_reference, args=[name], key=name)
        else:
            st.session_state.privileges_granted = True
            st.experimental_rerun()
        if not ref.bound_alias: return
    if st.button("Continue to app", type="primary"):
        st.session_state.privileges_granted = True
        st.experimental_rerun()
def get_references():
   app_name = session.get_current_database()
   data_frame = session.create_dataframe([''])
   refs = data_frame.select(call_udf('system$get_reference_definitions', lit(app_name))).collect()[0][0]
   references = []
   for row in json.loads(refs):
      bound_alias = row["bindings"][0]["alias"] if row["bindings"] else None
      references.append(Reference(row["name"], row["label"], row["object_type"], row["description"], bound_alias))
   return references
def get_ui_params():
    qdrant_cluster_config = json.loads(session.sql(f"call {current_database}.qdrant_app_public_schema.get_qdrant_cluster_config()").collect()[0]['GET_QDRANT_CLUSTER_CONFIG'])
    if 'qdrant_primary' in qdrant_cluster_config:
        qdrant_primary_instance_family = qdrant_cluster_config['qdrant_primary']
        qdrant_primary_instance_type_selector_index = cpu_instance_type_dict[qdrant_primary_instance_family]
        query_warehouse = qdrant_cluster_config['query_warehouse']
        query_warehouse_selector_index = warehouse_type_dict[query_warehouse]
    else:
        qdrant_primary_instance_type_selector_index = 3
        query_warehouse_selector_index = 3
    if 'qdrant_driver' in qdrant_cluster_config:
        qdrant_driver_instance_family = qdrant_cluster_config['qdrant_driver']
        qdrant_driver_instance_type_selector_index = driver_instance_type_dict[qdrant_driver_instance_family]
    else:
        qdrant_driver_instance_type_selector_index = 3
    if 'qdrant_secondary' in qdrant_cluster_config:
        qdrant_secondary_specs = qdrant_cluster_config['qdrant_secondary']
        qdrant_secondary_instance_family = qdrant_secondary_specs[0]
        qdrant_secondary_instance_type_selector_index = cpu_instance_type_dict[qdrant_secondary_instance_family]
        slider_num_qdrant_secondaries = qdrant_secondary_specs[1]
    else:
        qdrant_secondary_instance_type_selector_index = 3
        slider_num_qdrant_secondaries = 3
    return [qdrant_primary_instance_type_selector_index, qdrant_secondary_instance_type_selector_index, slider_num_qdrant_secondaries, query_warehouse_selector_index, qdrant_driver_instance_type_selector_index]
def run_streamlit():
    with st.spinner(f"Initializing..."):
        has_app_been_initialized = session.sql(f"call {current_database}.qdrant_app_public_schema.has_app_been_initialized()").collect()[0]['HAS_APP_BEEN_INITIALIZED']
        qdrant_primary_instance_type_selector_index, qdrant_secondary_instance_type_selector_index, slider_num_qdrant_secondaries, query_warehouse_selector_index, qdrant_driver_instance_type_selector_index = get_ui_params()
        with st.form("configuration"):
            st.write("Qdrant Cluster Configuration")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                cur_qdrant_primary_instance_type = st.selectbox("Primary:",cpu_instance_types, qdrant_primary_instance_type_selector_index)
            with col2:
                cur_qdrant_secondary_instance_type = st.selectbox("Secondary:",cpu_instance_types, qdrant_secondary_instance_type_selector_index)
                cur_num_qdrant_secondaries = st.slider("Select #:",0,10,slider_num_qdrant_secondaries, key='slider_secondary')
            with col3:
                cur_qdrant_driver_instance_type = st.selectbox("Driver:",driver_instance_types, qdrant_driver_instance_type_selector_index)
            with col4: 
                cur_query_warehouse_type = st.selectbox("Warehouse:", warehouse_types, query_warehouse_selector_index )
            # Every form must have a submit button.
            left_button_col, cent_button_col1,cent_button_col2, right_button_col = st.columns(4)
            with cent_button_col1:
                submitted = st.form_submit_button("Create Cluster / Check Status")
            with cent_button_col2:
                deleted = st.form_submit_button("Delete Cluster")
            if submitted:
                with st.spinner(f"Submitting cluster configuration..."):
                    _ = session.sql(f"CALL {current_database}.qdrant_app_public_schema.recreate_qdrant_compute_pool_and_service('primary', '{cur_qdrant_primary_instance_type}', '{cur_qdrant_secondary_instance_type}', '{cur_qdrant_driver_instance_type}', '{cur_query_warehouse_type}', 1)").collect()
                    _ = session.sql(f"CALL {current_database}.qdrant_app_public_schema.recreate_qdrant_compute_pool_and_service('driver', '{cur_qdrant_primary_instance_type}', '{cur_qdrant_secondary_instance_type}', '{cur_qdrant_driver_instance_type}', '{cur_query_warehouse_type}', 1)").collect()
                    _ = session.sql(f"CALL {current_database}.qdrant_app_public_schema.recreate_qdrant_compute_pool_and_service('secondary', '{cur_qdrant_primary_instance_type}', '{cur_qdrant_secondary_instance_type}', '{cur_qdrant_driver_instance_type}', '{cur_query_warehouse_type}', {cur_num_qdrant_secondaries})").collect()
            if deleted:
                with st.spinner(f"Deleting cluster..."):
                    _ = session.sql("CALL delete_qdrant_cluster()").collect()
            if submitted or deleted:
                qdrant_primary_instance_type_selector_index, qdrant_secondary_instance_type_selector_index, slider_num_qdrant_secondaries, query_warehouse_selector_index, qdrant_driver_instance_type_selector_index = get_ui_params()
                has_app_been_initialized = session.sql(f"call {current_database}.qdrant_app_public_schema.has_app_been_initialized()").collect()[0]['HAS_APP_BEEN_INITIALIZED']
            st.write('\n\n')
            st.write('\n\n')
            st.write('\n\n')
            st.write('\n\n')
            st.write('\n\n')
            if has_app_been_initialized:
                col5, col6, col7  = st.columns(3)
                with col5:
                    st.caption("Qdrant Primary Service Status:")
                    qdrant_primary_service_status = session.sql(f"CALL {current_database}.qdrant_app_public_schema.get_qdrant_service_status('primary')").collect()[0]['GET_QDRANT_SERVICE_STATUS']
                    st.write(qdrant_primary_service_status)
                with col6:
                    st.caption("Qdrant Secondary Service Status:")
                    qdrant_secondary_service_status = session.sql(f"CALL {current_database}.qdrant_app_public_schema.get_qdrant_service_status('secondary')").collect()[0]['GET_QDRANT_SERVICE_STATUS']
                    st.write(qdrant_secondary_service_status)
                with col7:
                    st.caption("Driver Service Status:")
                    qdrant_driver_service_status = session.sql(f"CALL {current_database}.qdrant_app_public_schema.get_qdrant_service_status('driver')").collect()[0]['GET_QDRANT_SERVICE_STATUS']
                    st.write(qdrant_driver_service_status)
                st.caption("Endpoints:")
                urls = session.sql(f"CALL {current_database}.qdrant_app_public_schema.get_qdrant_public_endpoints()").collect()[0]['GET_QDRANT_PUBLIC_ENDPOINTS']
                st.write(urls)
                
if __name__ == '__main__':
   if 'privileges_granted' not in st.session_state:
      setup()
   else:
      run_streamlit()
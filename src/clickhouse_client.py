import requests
import streamlit as st
import pandas as pd
import json

class ClickHouseClient:
    def __init__(self):
        self.host = st.secrets["clickhouse"]["host"].rstrip('/')
        self.username = st.secrets["clickhouse"]["username"]
        self.password = st.secrets["clickhouse"]["password"]
    
    def execute_query(self, query: str, params: dict = None):
        """
        Executes a query against ClickHouse using the HTTP interface.
        Appends 'FORMAT JSONEachRow' to the query to ensure JSON output.
        """
        # Ensure the query requests JSON output if not already specified
        if "FORMAT" not in query.upper():
            query_with_format = f"{query} FORMAT JSONEachRow"
        else:
            query_with_format = query
            
        auth = (self.username, self.password)
        
        # ClickHouse HTTP interface accepts parameters as query string: ?param_name=value
        # In SQL, use {name:Type}
        url_params = {}
        if params:
            for k, v in params.items():
                url_params[f"param_{k}"] = v

        try:
            # Using data-binary as per the curl example provided by the user
            response = requests.post(
                self.host,
                params=url_params,
                data=query_with_format.encode('utf-8'),
                auth=auth,
                timeout=120
            )
            
            if response.status_code != 200:
                st.error(f"ClickHouse Error ({response.status_code}): {response.text}")
                return None
            
            # ClickHouse returns newline-delimited JSON with JSONEachRow
            data = []
            if response.text.strip():
                for line in response.text.strip().split('\n'):
                    if line.strip():
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError as err:
                            print(f"JSON Parse Error on line: {line}\n{err}")
                            continue
            
            if not data:
                print(f"RAW CLICKHOUSE RESPONSE:\n{response.text[:1000]}")
            return data
            
        except requests.exceptions.RequestException as e:
            st.error(f"Error executing query: {e}")
            return None

def get_client():
    return ClickHouseClient()


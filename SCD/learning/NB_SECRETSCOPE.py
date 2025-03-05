# Databricks notebook source
import requests

# Set Databricks instance and token
databricks_instance = "adb-678318348142550.10.azuredatabricks.net"
token = "dapi36ed0dbff9208b067bbf551d04a4b3bf"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}


response_scope = requests.post(
    f"https://{databricks_instance}/api/2.0/secrets/scopes/create",
    headers=headers,
    json={"scope": "my-scope"}
)
print(response_scope.status_code, response_scope.json())

# Add a username secret
response_username = requests.post(
    f"https://{databricks_instance}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": "my-scope", "key": "username", "string_value": "intern"}
)
print(response_username.status_code, response_username.json())

# Add a password secret
response_password = requests.post(
    f"https://{databricks_instance}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": "my-scope", "key": "password", "string_value": "Syren@123"}
)
print(response_password.status_code, response_password.json())

# COMMAND ----------

dbutils.secrets.list("my-scope")
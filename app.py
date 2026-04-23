from flask import Flask, render_template, request, jsonify
import requests
import xml.etree.ElementTree as ET
import urllib3

# Suppress SSL warnings for dev environment
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# Fixed credentials for PMTS SAP System
AUTH = ("basis", "Refresh@111222333")

def fix_sap_url(url):
    """Ensures the URL uses HTTPS and includes the mandatory SAP port 44300."""
    # Ensure HTTPS
    if url.startswith("http://"):
        url = url.replace("http://", "https://")
    
    # Inject port 44300 if missing from the domain
    if ".net" in url and ":44300" not in url:
        url = url.replace(".net", ".net:44300")
    return url

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_entities', methods=['POST'])
def get_entities():
    """Parses metadata and returns all available EntitySets for the dropdown."""
    try:
        req_data = request.get_json()
        raw_url = req_data.get('url', '')
        metadata_url = fix_sap_url(raw_url)
        
        response = requests.get(metadata_url, auth=AUTH, verify=False, timeout=15)
        if response.status_code != 200:
            return jsonify({"error": f"Failed to reach SAP. Status: {response.status_code}"}), 400

        root = ET.fromstring(response.content)
        
        # Expanded namespaces to cover V2, V4, and EDMX wrappers
        ns = {
            'edmx': 'http://schemas.microsoft.com/ado/2007/06/edmx',
            'v4_edm': 'http://docs.oasis-open.org/odata/ns/edm',
            'v2_edm': 'http://schemas.microsoft.com/ado/2008/09/edm',
            'v3_edm': 'http://schemas.microsoft.com/ado/2009/11/edm'
        }

        # Search for EntitySets across all common namespaces
        entity_sets = []
        search_paths = [".//v4_edm:EntitySet", ".//v2_edm:EntitySet", ".//v3_edm:EntitySet"]
        
        for path in search_paths:
            found = root.findall(path, ns)
            for es in found:
                entity_sets.append({
                    "name": es.get("Name"),
                    "type": es.get("EntityType")
                })
            
        if not entity_sets:
            return jsonify({"error": "No EntitySets found in metadata. Check your SAP service permissions."}), 404

        return jsonify({"entities": entity_sets})
    except Exception as e:
        return jsonify({"error": f"Metadata Error: {str(e)}"}), 500

@app.route('/fetch_data', methods=['POST'])
def fetch_data():
    """Fetches data for the specific EntitySet chosen from the dropdown."""
    try:
        req_data = request.get_json()
        metadata_url = fix_sap_url(req_data.get('url', ''))
        selected_entity = req_data.get('entity_set')
        
        # 1. Fetch Metadata again to get column names
        response = requests.get(metadata_url, auth=AUTH, verify=False, timeout=15)
        root = ET.fromstring(response.content)
        ns = {
            'v4_edm': 'http://docs.oasis-open.org/odata/ns/edm',
            'v2_edm': 'http://schemas.microsoft.com/ado/2008/09/edm',
            'v3_edm': 'http://schemas.microsoft.com/ado/2009/11/edm'
        }

        # Find the EntitySet and its associated Type
        entity_set = None
        for p in [".//v4_edm:EntitySet", ".//v2_edm:EntitySet"]:
            res = root.find(f"{p}[@Name='{selected_entity}']", ns)
            if res is not None:
                entity_set = res
                break
        
        if entity_set is None:
            return jsonify({"error": "EntitySet not found in metadata"}), 404

        entity_type_full = entity_set.get("EntityType")
        entity_type_name = entity_type_full.split('.')[-1]
        
        # Find Properties (Columns) for this EntityType
        type_def = None
        for p in [".//v4_edm:EntityType", ".//v2_edm:EntityType"]:
            res = root.find(f"{p}[@Name='{entity_type_name}']", ns)
            if res is not None:
                type_def = res
                break
        
        columns = []
        if type_def is not None:
            props = type_def.findall("v4_edm:Property", ns) + type_def.findall("v2_edm:Property", ns)
            columns = [p.get("Name") for p in props]

        # 2. Construct Data URL - Stripping existing queries to avoid duplication
        base_path = metadata_url.split('$metadata')[0].split('?')[0].rstrip('/')
        data_url = f"{base_path}/{selected_entity}"
        
        # Force JSON and Client 110
        params = {"$top": 100, "$format": "json", "sap-client": "110"}
        
        data_resp = requests.get(data_url, params=params, auth=AUTH, verify=False, timeout=20)
        
        if data_resp.status_code != 200:
            return jsonify({"error": f"SAP Data Error: {data_resp.text}"}), data_resp.status_code

        json_res = data_resp.json()
        
        # Handle different OData response structures (V2 vs V4)
        records = json_res.get('value') or json_res.get('d', {}).get('results') or json_res.get('d')
        
        return jsonify({
            "entity": selected_entity,
            "columns": columns,
            "data": records if isinstance(records, list) else [records]
        })

    except Exception as e:
        return jsonify({"error": f"Processing Error: {str(e)}"}), 500

if __name__ == '__main__':
    # Using 0.0.0.0 allows access from other devices on your network
    app.run(debug=True, host='0.0.0.0', port=5000)

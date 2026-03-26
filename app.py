from flask import Flask, render_template, request, jsonify
import requests
import xml.etree.ElementTree as ET
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
app = Flask(__name__)

# Fixed credentials for PMTS SAP System
AUTH = ("sap_api", "Refresh@1122334455")

def fix_sap_url(url):
    """Ensures the URL uses HTTPS and the correct SAP port if missing."""
    if "http://" in url and ":44300" not in url:
        url = url.replace("http://", "https://").replace(".net", ".net:44300")
    return url

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_entities', methods=['POST'])
def get_entities():
    """Parses metadata and returns all available EntitySets for the dropdown."""
    try:
        req_data = request.get_json()
        metadata_url = fix_sap_url(req_data.get('url', ''))
        
        response = requests.get(metadata_url, auth=AUTH, verify=False, timeout=15)
        if response.status_code != 200:
            return jsonify({"error": "Failed to reach Metadata"}), 400

        root = ET.fromstring(response.content)
        ns = {
            'v4_edm': 'http://docs.oasis-open.org/odata/ns/edm',
            'v2_edm': 'http://schemas.microsoft.com/ado/2008/09/edm'
        }

        # Find all EntitySets (tables) available in this service
        entity_sets = root.findall(".//v4_edm:EntitySet", ns) + root.findall(".//v2_edm:EntitySet", ns)
        
        entity_list = []
        for es in entity_sets:
            entity_list.append({
                "name": es.get("Name"),
                "type": es.get("EntityType")
            })
            
        return jsonify({"entities": entity_list})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/fetch_data', methods=['POST'])
def fetch_data():
    """Fetches data for the specific EntitySet chosen from the dropdown."""
    try:
        req_data = request.get_json()
        metadata_url = fix_sap_url(req_data.get('url', ''))
        selected_entity = req_data.get('entity_set') # Selected from dropdown
        
        # 1. Fetch Metadata to get column names for the specific selection
        response = requests.get(metadata_url, auth=AUTH, verify=False, timeout=15)
        root = ET.fromstring(response.content)
        ns = {
            'v4_edm': 'http://docs.oasis-open.org/odata/ns/edm',
            'v2_edm': 'http://schemas.microsoft.com/ado/2008/09/edm'
        }

        # Find the specific EntitySet and its Type
        entity_set = root.find(f".//v4_edm:EntitySet[@Name='{selected_entity}']", ns) or \
                     root.find(f".//v2_edm:EntitySet[@Name='{selected_entity}']", ns)
        
        entity_type_name = entity_set.get("EntityType").split('.')[-1]
        type_def = root.find(f".//v4_edm:EntityType[@Name='{entity_type_name}']", ns) or \
                   root.find(f".//v2_edm:EntityType[@Name='{entity_type_name}']", ns)
        
        columns = [p.get("Name") for p in (type_def.findall("v4_edm:Property", ns) or type_def.findall("v2_edm:Property", ns))]

        # 2. Construct Data URL and Fetch
        base_path = metadata_url.split('$metadata')[0].rstrip('/')
        data_url = f"{base_path}/{selected_entity}"
        params = {"$top": 100, "$format": "json", "sap-client": "100"}
        
        data_resp = requests.get(data_url, params=params, auth=AUTH, verify=False, timeout=20)
        json_res = data_resp.json()
        records = json_res.get('value') or json_res.get('d', {}).get('results') or json_res.get('d')
        
        return jsonify({
            "entity": selected_entity,
            "columns": columns,
            "data": records if isinstance(records, list) else [records]
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
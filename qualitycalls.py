import pandas as pd
import requests
import io

# Airtable API details
AIRTABLE_BASE_ID = "appv2NG9TOP8aiYUm"
AIRTABLE_TABLE_NAME = "Feb Quality Calls"
AIRTABLE_API_KEY = "patNrsSsWF1NpOExt.7d405eb26f24695e8cb633bc7d61d057416e01e731dc872e108b3bc7f37fc5ab"  # Keep this secure

# GitHub raw file URL (Updated with your GitHub username and repo)
GITHUB_ODS_URL = "https://raw.githubusercontent.com/guankong888/qualitycalls/main/MLS.ods"

# Airtable API Endpoint
AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"

# Headers for Airtable API
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

def fetch_ods_from_github():
    """Fetch the latest MLS.ods file from GitHub and save it locally."""
    response = requests.get(GITHUB_ODS_URL)
    if response.status_code == 200:
        with open("MLS.ods", "wb") as file:
            file.write(response.content)
        return "MLS.ods"
    else:
        print("Error fetching ODS:", response.status_code, response.text)
        return None

def fetch_airtable_records():
    """Fetch all existing records from Airtable."""
    records = []
    offset = None

    while True:
        url = AIRTABLE_URL
        if offset:
            url += f"?offset={offset}"

        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print("Error fetching Airtable records:", response.status_code, response.text)
            return None

        data = response.json()
        records.extend(data["records"])
        offset = data.get("offset")

        if not offset:
            break

    return records

def update_airtable_record(record_id, update_data):
    """Update an Airtable record."""
    response = requests.patch(f"{AIRTABLE_URL}/{record_id}", json={"fields": update_data}, headers=HEADERS)
    if response.status_code == 200:
        print(f"Updated record {record_id}")
    else:
        print(f"Error updating {record_id}:", response.status_code, response.text)

def sync_data():
    """Sync data from GitHub ODS to Airtable based on Club Code."""
    ods_file = fetch_ods_from_github()
    if ods_file is None:
        return

    # Read the ODS file and get all sheets
    df_dict = pd.read_excel(ods_file, sheet_name=None, engine="odf")

    # Filter relevant sheets
    relevant_sheets = ["Arizona", "California", "Utah", "Nevada", "Texas", "Florida"]
    filtered_data = pd.concat([df_dict[sheet] for sheet in relevant_sheets if sheet in df_dict], ignore_index=True)

    airtable_records = fetch_airtable_records()
    if airtable_records is None:
        return

    # Convert Airtable records to a dictionary with Club Code as key
    airtable_dict = {rec["fields"].get("Club Code"): rec["id"] for rec in airtable_records if "Club Code" in rec["fields"]}

    # Iterate over the ODS data and update matching Airtable records
    for _, row in filtered_data.iterrows():
        club_code = str(row["D"]).strip()  # Column D = Club Code in ODS
        if club_code in airtable_dict:
            record_id = airtable_dict[club_code]

            update_data = {
                "OM Email": row["L"],  # Column L = OM Email
                "DOM Email": row["J"],  # Column J = DOM Email
                "Contact": row["F"]  # Column F = Contact
            }

            update_airtable_record(record_id, update_data)

if __name__ == "__main__":
    sync_data()


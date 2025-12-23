from googleapiclient.discovery import build
from google.oauth2 import service_account

# Path to your service account key file
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Authenticate
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Build the service
service = build('sheets', 'v4', credentials=creds)

# Your spreadsheet ID (from the sheet URL)
SPREADSHEET_ID = '1QM2yrlYPU_dA4_8eRWkUGj-6wkPLs1M7jHO_cPQjnQY'
RANGE_NAME = 'Employee Idle Time Report / <35 hrs work!A4:B4'  # Range to update

# Values to write
values = [
    ["Dog Wipes", 75]
]
body = {
    'values': values
}

# Update request
result = service.spreadsheets().values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME,
    valueInputOption="RAW",
    body=body
).execute()

print(f"{result.get('updatedCells')} cells updated.")

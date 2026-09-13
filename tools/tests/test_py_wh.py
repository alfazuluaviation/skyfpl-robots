import requests
import json
import time

SUPABASE_URL = 'https://gongoqjjpwphhttumdjm.supabase.co'
SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o'

wh_body = {
    'cycle': '2608',
    'effective_date': '06/08/2026',
    'r2_path': 'navdata/cycles/2608/navdata_2608.json',
    'total_points': 14076,
    'audit_summary': {
        'airport': {'offered': 4432, 'delivered': 4432, 'rejected': 0},
        'heliport': {'offered': 1605, 'delivered': 1605, 'rejected': 0},
        'vor': {'offered': 77, 'delivered': 77, 'rejected': 0},
        'ndb': {'offered': 24, 'delivered': 24, 'rejected': 0},
        'fix': {'offered': 7938, 'delivered': 7938, 'rejected': 0},
        'total_rejected': 0
    },
    'generated_at': time.time()
}

wh_headers = {
    'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
    'apikey': SUPABASE_ANON_KEY,
    'Content-Type': 'application/json'
}

print("Chamando Edge Function do Supabase...")
res = requests.post(f"{SUPABASE_URL}/functions/v1/airac-navdata-ingest", json=wh_body, headers=wh_headers, timeout=60)
print("HTTP Status:", res.status_code)
print("Resposta:", res.text)

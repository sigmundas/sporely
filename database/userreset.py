import os
import requests

SUPABASE_URL = 'https://zkpjklzfwzefhjluvhfw.supabase.co'

# Replace this with your actual service_role key from Supabase Dashboard -> Project Settings -> API
SERVICE_ROLE_KEY = "eyJhbG..."

user_id = ""
new_email = ""

url = f"{SUPABASE_URL}/auth/v1/admin/users/{user_id}"

headers = {
    "apikey": SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

payload = {
    "email": new_email,
    # For a one-time test-user fix (since you don't have old inbox):
    "email_confirm": True,
}

resp = requests.put(url, headers=headers, json=payload, timeout=30)
resp.raise_for_status()

print("Updated user:", resp.json())
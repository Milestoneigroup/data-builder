import os
from dotenv import load_dotenv

# Load base then overrides — later files win (all override=True after .env seeds defaults).
load_dotenv(".env", override=True)
load_dotenv(".env.local", override=True)
load_dotenv("env.local", override=True)

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")

print(f"SUPABASE_URL found: {bool(url)}")
print(f"SUPABASE_URL value: {url}")
print(f"SERVICE_ROLE_KEY found: {bool(key)}")
print(f"Key starts with: {key[:20] if key else 'NONE'}")

# Try a basic connection
from supabase import create_client

if url and key:
    try:
        client = create_client(url, key)
        result = client.table("venues").select("id").limit(1).execute()
        print(f"CONNECTION SUCCESS: {result}")
    except Exception as e:
        print(f"CONNECTION FAILED: {e}")
else:
    print("Missing URL or key — check env.local")

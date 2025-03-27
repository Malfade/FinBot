import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the token with additional debugging
API_TOKEN = os.getenv('API_TOKEN')

# Print token for debugging (be careful not to share this!)
print(f"Token loaded: {bool(API_TOKEN)}")  # Will print True if token exists
print(f"Token length: {len(API_TOKEN) if API_TOKEN else 'N/A'}")

# Rest of your bot code...
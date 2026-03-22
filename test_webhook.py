import requests

url = "https://discord.com/api/webhooks/1484367907029778432/OJUKZtVitPwrIAODGmUy8F3IOR3KPXvSTN2zOQw0RSR6rU4iVxJITjToI5YwCJ9zEolV"

try:
    resp = requests.post(url, json={"content": "Test message"})
    print(f"Status: {resp.status_code}")
    print(f"Text: {resp.text}")
except Exception as e:
    print(f"Error: {e}")

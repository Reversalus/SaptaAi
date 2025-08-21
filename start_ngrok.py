#!/usr/bin/env python3
"""
Start ngrok tunnel for the webhook server
"""

from pyngrok import ngrok
import time

def start_ngrok_tunnel():
    """Start ngrok tunnel for port 5000"""
    try:
        print("🚀 Starting ngrok tunnel...")
        
        # Start ngrok tunnel
        public_url = ngrok.connect(5000)
        
        print("✅ ngrok tunnel started successfully!")
        print(f"🌐 Public URL: {public_url}")
        print(f"🔗 Webhook URL: {public_url}/webhook")
        print("\n📋 Next steps:")
        print("1. Copy the webhook URL above")
        print("2. Update your Google Apps Script with this URL")
        print("3. Submit a test form response")
        print("\n⏹️  Press Ctrl+C to stop the tunnel")
        
        # Keep the tunnel open
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping ngrok tunnel...")
        ngrok.kill()
        print("✅ ngrok tunnel stopped")
    except Exception as e:
        print(f"❌ Error starting ngrok: {e}")

if __name__ == "__main__":
    start_ngrok_tunnel() 
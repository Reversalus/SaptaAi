#!/usr/bin/env python3
"""
Startup script for the Matrimonial Webhook Server
This script checks dependencies and starts the webhook server with proper error handling.
"""

import os
import sys
import subprocess
import importlib.util

def check_dependency(module_name, package_name=None):
    """Check if a Python module is available"""
    if package_name is None:
        package_name = module_name
    
    spec = importlib.util.find_spec(module_name)
    if spec is None:
        print(f"❌ Missing dependency: {package_name}")
        return False
    else:
        print(f"✅ Found dependency: {package_name}")
        return True

def install_dependency(package_name):
    """Install a Python package using pip"""
    try:
        print(f"📦 Installing {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        print(f"✅ Successfully installed {package_name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install {package_name}: {e}")
        return False

def check_dependencies():
    """Check and install required dependencies"""
    print("🔍 Checking dependencies...")
    
    required_deps = [
        ("flask", "Flask"),
        ("requests", "requests"),
        ("googleapiclient", "google-api-python-client"),
        ("google.oauth2", "google-auth"),
    ]
    
    missing_deps = []
    for module_name, package_name in required_deps:
        if not check_dependency(module_name, package_name):
            missing_deps.append(package_name)
    
    if missing_deps:
        print(f"\n⚠️  Missing dependencies: {', '.join(missing_deps)}")
        response = input("Would you like to install them automatically? (y/n): ")
        
        if response.lower() in ['y', 'yes']:
            for package in missing_deps:
                if not install_dependency(package):
                    print(f"❌ Failed to install {package}. Please install it manually.")
                    return False
        else:
            print("❌ Please install the missing dependencies manually:")
            print(f"pip install {' '.join(missing_deps)}")
            return False
    
    print("✅ All dependencies are available!")
    return True

def check_environment():
    """Check if required environment variables are set"""
    print("\n🔍 Checking environment variables...")
    
    required_vars = [
        "SENDER_EMAIL",
        "SENDER_PASSWORD", 
        "ADMIN_EMAIL"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"⚠️  Missing environment variables: {', '.join(missing_vars)}")
        print("Please set them before running the webhook server:")
        for var in missing_vars:
            print(f"export {var}=your_value_here")
        return False
    
    print("✅ All required environment variables are set!")
    return True

def check_files():
    """Check if required files exist"""
    print("\n🔍 Checking required files...")
    
    required_files = [
        "app.py",
        "service_account2.json",
        "webhook_server.py"
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing required files: {', '.join(missing_files)}")
        return False
    
    print("✅ All required files are present!")
    return True

def start_webhook_server():
    """Start the webhook server"""
    print("\n🚀 Starting Matrimonial Webhook Server...")
    
    try:
        # Import and run the webhook server
        from webhook_server import app, initialize_processing
        
        # Initialize processing
        initialize_processing()
        
        # Get port from environment or use default
        port = int(os.getenv("PORT", 5000))
        
        print(f"🌐 Webhook server will be available at: http://localhost:{port}")
        print("📋 Available endpoints:")
        print(f"   - GET  http://localhost:{port}/ (Server info)")
        print(f"   - GET  http://localhost:{port}/health (Health check)")
        print(f"   - GET  http://localhost:{port}/status (Processing status)")
        print(f"   - POST http://localhost:{port}/webhook (Webhook endpoint)")
        print(f"   - POST http://localhost:{port}/trigger (Manual trigger)")
        print("\n🔄 Server is starting... (Press Ctrl+C to stop)")
        
        # Run the Flask app
        app.run(host='0.0.0.0', port=port, debug=False)
        
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting webhook server: {e}")
        return False
    
    return True

def main():
    """Main function"""
    print("=" * 60)
    print("🏠 Matrimonial Webhook Server Startup")
    print("=" * 60)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check environment variables
    if not check_environment():
        print("\n💡 You can still run the server, but some features may not work.")
        response = input("Continue anyway? (y/n): ")
        if response.lower() not in ['y', 'yes']:
            sys.exit(1)
    
    # Check required files
    if not check_files():
        sys.exit(1)
    
    # Start the webhook server
    start_webhook_server()

if __name__ == "__main__":
    main() 
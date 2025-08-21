from flask import Flask, request, jsonify
import threading
import time
import logging
import os
from datetime import datetime
import requests
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json
from json.decoder import JSONDecodeError

# Import the main processing function from app.py
from app import process_new_matrimonial_registration, fetch_data_from_google_sheets, logger

app = Flask(__name__)

# Configuration
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "m5Z7q_hQtYPiO8VUXB0GSOPw3AC7RuDklwmu4JWpbVY")
GOOGLE_FORM_ID = "1eARgTDQL6ndopf4jyzB7Kj5FlG_84OBSaK6C5a0R0fo"
SERVICE_ACCOUNT_FILE = "service_account2.json"
SPREADSHEET_ID = "182002sppODMuNCyNady9t8C7mTBMIE3UAxWuXK8dZ3w"  # Updated source spreadsheet ID

# Track processing status
processing_status = {
    "is_processing": False,
    "last_processed": None,
    "last_submission_count": 0,
    "current_submission_count": 0
}

def get_form_submissions_count():
    """Get the current number of form submissions"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, 
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        service = build("sheets", "v4", credentials=credentials)
        sheet = service.spreadsheets()
        # Get the form responses
        try:
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID, 
                range="'Form Responses 38'!A1:BH1000"
            ).execute()
        except JSONDecodeError as jde:
            logger.error(f"JSON decode error when getting form submissions count: {jde}")
            return 0
        except Exception as api_e:
            logger.error(f"API error when getting form submissions count: {api_e}")
            return 0
        if not isinstance(result, dict):
            logger.error(f"Unexpected result type from Sheets API: {type(result)}. Raw result: {result}")
            return 0
        values = result.get("values", [])
        # Subtract 1 for header row, return 0 if no data
        return max(0, len(values) - 1) if values else 0
    except JSONDecodeError as jde:
        logger.error(f"JSON decode error in get_form_submissions_count: {jde}")
        return 0
    except Exception as e:
        logger.error(f"Error getting form submissions count: {e}")
        return 0

def check_for_new_submissions():
    """Check if there are new form submissions and process them"""
    global processing_status
    
    try:
        current_count = get_form_submissions_count()
        processing_status["current_submission_count"] = current_count
        
        # Check if there are new submissions
        if current_count > processing_status["last_submission_count"]:
            logger.info(f"New form submission detected! Previous: {processing_status['last_submission_count']}, Current: {current_count}")
            
            # Process the new submission
            success = process_new_matrimonial_registration()
            
            if success:
                processing_status["last_processed"] = datetime.now().isoformat()
                processing_status["last_submission_count"] = current_count
                logger.info("Successfully processed new form submission")
            else:
                logger.error("Failed to process new form submission")
                
        return current_count
        
    except Exception as e:
        logger.error(f"Error checking for new submissions: {e}")
        return processing_status["last_submission_count"]

def periodic_check():
    """Periodically check for new form submissions"""
    while True:
        try:
            if not processing_status["is_processing"]:
                processing_status["is_processing"] = True
                check_for_new_submissions()
                processing_status["is_processing"] = False
            else:
                logger.info("Processing already in progress, skipping this check")
                
        except Exception as e:
            logger.error(f"Error in periodic check: {e}")
            processing_status["is_processing"] = False
            
        # Wait for 30 seconds before next check
        time.sleep(30)

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    """Handle webhook notifications from Google Forms"""
    try:
        # Verify webhook secret (if configured)
        if WEBHOOK_SECRET != "m5Z7q_hQtYPiO8VUXB0GSOPw3AC7RuDklwmu4JWpbVY":
            auth_header = request.headers.get('Authorization')
            if not auth_header or auth_header != f"Bearer {WEBHOOK_SECRET}":
                logger.warning("Unauthorized webhook request")
                return jsonify({"error": "Unauthorized"}), 401
        
        # Get webhook data
        data = request.get_json()
        logger.info(f"Received webhook: {json.dumps(data, indent=2)}")
        
        # Check if this is a form submission notification
        if data and isinstance(data, dict):
            # Extract relevant information
            form_id = data.get('formId', '')
            response_id = data.get('responseId', '')
            create_time = data.get('createTime', '')
            
            logger.info(f"Form submission detected - Form ID: {form_id}, Response ID: {response_id}")
            
            # Trigger processing in a separate thread to avoid blocking
            def process_async():
                try:
                    processing_status["is_processing"] = True
                    success = process_new_matrimonial_registration()
                    if success:
                        processing_status["last_processed"] = datetime.now().isoformat()
                        logger.info("Successfully processed webhook-triggered submission")
                    else:
                        logger.error("Failed to process webhook-triggered submission")
                except Exception as e:
                    logger.error(f"Error in async processing: {e}")
                finally:
                    processing_status["is_processing"] = False
            
            # Start processing in background thread
            thread = threading.Thread(target=process_async)
            thread.daemon = True
            thread.start()
            
            return jsonify({
                "status": "success",
                "message": "Webhook received and processing started",
                "form_id": form_id,
                "response_id": response_id
            }), 200
        
        return jsonify({"error": "Invalid webhook data"}), 400
        
    except Exception as e:
        logger.error(f"Error handling webhook: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/status', methods=['GET'])
def get_status():
    """Get the current processing status"""
    try:
        current_count = get_form_submissions_count()
        return jsonify({
            "status": "success",
            "is_processing": processing_status["is_processing"],
            "last_processed": processing_status["last_processed"],
            "last_submission_count": processing_status["last_submission_count"],
            "current_submission_count": current_count,
            "new_submissions": current_count - processing_status["last_submission_count"]
        }), 200
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/trigger', methods=['POST'])
def manual_trigger():
    """Manually trigger processing"""
    try:
        if processing_status["is_processing"]:
            return jsonify({
                "status": "error",
                "message": "Processing already in progress"
            }), 409
        
        # Start processing in background thread
        def process_async():
            try:
                processing_status["is_processing"] = True
                success = process_new_matrimonial_registration()
                if success:
                    processing_status["last_processed"] = datetime.now().isoformat()
                    logger.info("Successfully processed manual trigger")
                else:
                    logger.error("Failed to process manual trigger")
            except Exception as e:
                logger.error(f"Error in manual trigger processing: {e}")
            finally:
                processing_status["is_processing"] = False
        
        thread = threading.Thread(target=process_async)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            "status": "success",
            "message": "Manual processing triggered"
        }), 200
        
    except Exception as e:
        logger.error(f"Error in manual trigger: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Matrimonial Webhook Server"
    }), 200

@app.route('/', methods=['GET'])
def home():
    """Home page with basic information"""
    return jsonify({
        "service": "Matrimonial Webhook Server",
        "version": "1.0.0",
        "endpoints": {
            "webhook": "/webhook (POST) - Handle form submission webhooks",
            "status": "/status (GET) - Get processing status",
            "trigger": "/trigger (POST) - Manually trigger processing",
            "health": "/health (GET) - Health check"
        },
        "form_id": GOOGLE_FORM_ID,
        "spreadsheet_id": SPREADSHEET_ID
    }), 200

def initialize_processing():
    """Initialize the processing by getting current submission count"""
    try:
        current_count = get_form_submissions_count()
        processing_status["last_submission_count"] = current_count
        processing_status["current_submission_count"] = current_count
        logger.info(f"Initialized with {current_count} existing submissions")
    except Exception as e:
        logger.error(f"Error initializing processing: {e}")

if __name__ == '__main__':
    # Initialize processing status
    initialize_processing()
    
    # Start periodic checking in background thread
    periodic_thread = threading.Thread(target=periodic_check)
    periodic_thread.daemon = True
    periodic_thread.start()
    
    logger.info("Starting Matrimonial Webhook Server...")
    logger.info(f"Form ID: {GOOGLE_FORM_ID}")
    logger.info(f"Spreadsheet ID: {SPREADSHEET_ID}")
    
    # Run the Flask app
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False) 
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics.pairwise import cosine_similarity
from fpdf import FPDF
import smtplib
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
from dotenv import load_dotenv
import requests
import os
import re
from PIL import Image
from datetime import datetime
import logging
from functools import lru_cache
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
import io
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pickle
from flask import Flask, request, jsonify
from json.decoder import JSONDecodeError
from googleapiclient.errors import HttpError
from email.mime.image import MIMEImage
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("matrimonial_handler.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
# Load environment variables
load_dotenv()
# Constants
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SERVICE_ACCOUNT_FILE = "service_account2.json"
SPREADSHEET_ID = "182002sppODMuNCyNady9t8C7mTBMIE3UAxWuXK8dZ3w"  # Updated source spreadsheet ID
RANGE_NAME = "'Form Responses 38'!A1:BH1000"
STATIC_HEADER_IMAGE = "logo.png"  # Using the existing logo.png file
# Target Google Sheet constants for tracking sent emails
TARGET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
# Use the same service account file for target sheet
TARGET_SERVICE_ACCOUNT_FILE = SERVICE_ACCOUNT_FILE
TARGET_SPREADSHEET_ID = "1qTOJJXmNQ6axt_khGoH9X64_SHIPJyVoVGUqUZexUYE"  # Updated target spreadsheet ID
TARGET_RANGE_NAME = "Sheet1!A:M"  # Updated to include column M for email text: Sr no, name, whatsappnumber, email, birth date, location, pdf_url, top1_url, top2_url, top3_url, top4_url, top5_url, email_text
# Google Drive constants for PDF upload
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
# Use the same service account file for Drive
DRIVE_SERVICE_ACCOUNT_FILE = SERVICE_ACCOUNT_FILE
# Configuration to disable Google Drive uploads if needed
DISABLE_DRIVE_UPLOAD = os.getenv("DISABLE_DRIVE_UPLOAD", "false").lower() == "true"

# Email Configuration - Set default values if environment variables are not available
# IMPORTANT: Replace these placeholder values with your actual Gmail credentials to enable email functionality
# 
# Option 1: Modify these values directly in the code below
# Option 2: Set environment variables: SENDER_EMAIL, SENDER_PASSWORD, ADMIN_EMAIL
#
# Example configuration:
# SENDER_EMAIL = "yourname@gmail.com"                    # Your Gmail address
# SENDER_PASSWORD = "abcd efgh ijkl mnop"               # Your Gmail App Password (16 characters)
# ADMIN_EMAIL = "admin@yourcompany.com"                  # Admin email to receive notifications
#
# Note: For SENDER_PASSWORD, use Gmail App Password, NOT your regular Gmail password
# To get App Password: Google Account → Security → 2-Step Verification → App passwords

SENDER_EMAIL = os.getenv("SENDER_EMAIL", "jerrydisuza2322@gmail.com")  # Replace with your Gmail address
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD", "gxmy osns fxzh btsp")  # Replace with your Gmail App Password
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "omjani282005@gmail.com")  # Replace with admin email address

# Email functionality control
ENABLE_EMAIL = os.getenv("ENABLE_EMAIL", "true").lower() == "true"

# Cache for Google Sheets data
_sheets_data_cache = None
_last_fetch_time = None
CACHE_DURATION = 300  # 5 minutes
# OAuth constants and configuration for Drive uploads
OAUTH_SCOPES = ['https://www.googleapis.com/auth/drive']
TOKEN_PICKLE = 'token_drive_oauth.pickle'
PHOTO_WIDTH = 35
PHOTO_HEIGHT = 50

def get_oauth_drive_creds():
    creds = None
    token_path = 'token.json'
    client_secret_path = 'client_secret.json'
    OAUTH_SCOPES = ['https://www.googleapis.com/auth/drive']  # adjust your scopes

    # Load saved credentials if they exist
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, OAUTH_SCOPES)

    # If no valid credentials, authenticate locally and save them
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # First time authentication: run on your LOCAL machine only
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, OAUTH_SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the token for future use in JSON format
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    return creds

@lru_cache(maxsize=128)
def convert_height_to_cm(height_value):
    """Convert height to centimeters with caching"""
    if not height_value or pd.isna(height_value):
        return None
    try:
        height_str = str(height_value).lower()
        if "'" in height_str or '"' in height_str:
            feet = 0
            inches = 0
            if "'" in height_str:
                feet = int(height_str.split("'")[0])
            if '"' in height_str:
                inches = int(height_str.split('"')[0].split("'")[-1])
            return (feet * 30.48) + (inches * 2.54)
        return float(height_str)
    except:
        return None
def fetch_data_from_google_sheets():
    """Fetch data from Google Sheets with caching and improved error handling"""
    global _sheets_data_cache, _last_fetch_time
    current_time = datetime.now().timestamp()
    
    # Return cached data if it's still valid
    if _sheets_data_cache is not None and _last_fetch_time is not None:
        if current_time - _last_fetch_time < CACHE_DURATION:
            logger.info("Using cached Google Sheets data")
            return _sheets_data_cache
    
    try:
        logger.info(f"Attempting to fetch data from Google Sheets using service account: {SERVICE_ACCOUNT_FILE}")
        
        # Verify the service account file exists
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            logger.error(f"Service account file not found: {SERVICE_ACCOUNT_FILE}")
            return None
        
        # Create credentials with proper scopes
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, 
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        logger.info("Successfully created credentials")
        
        # Build the service
        service = build("sheets", "v4", credentials=credentials)
        sheet = service.spreadsheets()
        
        logger.info(f"Fetching data from spreadsheet ID: {SPREADSHEET_ID}")
        logger.info(f"Using range: {RANGE_NAME}")
        
        # Make the API request with better error handling
        try:
            # First, try to get spreadsheet metadata to verify access
            spreadsheet_info = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
            logger.info(f"Successfully accessed spreadsheet: {spreadsheet_info.get('properties', {}).get('title', 'Unknown')}")
            
            # Now fetch the actual data
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID, 
                range=RANGE_NAME,
                valueRenderOption='UNFORMATTED_VALUE',
                dateTimeRenderOption='FORMATTED_STRING'
            ).execute()
            
        except HttpError as http_error:
            error_details = http_error.error_details
            status_code = http_error.resp.status
            logger.error(f"HTTP error {status_code} when accessing Google Sheets: {http_error}")
            
            if status_code == 403:
                logger.error("Permission denied. Check if:")
                logger.error("1. The service account has access to the spreadsheet")
                logger.error("2. The spreadsheet is shared with the service account email")
                logger.error("3. The API is enabled in Google Cloud Console")
            elif status_code == 404:
                logger.error("Spreadsheet not found. Check if:")
                logger.error("1. The spreadsheet ID is correct")
                logger.error("2. The spreadsheet exists and is accessible")
            
            return None
            
        except JSONDecodeError as jde:
            logger.error(f"JSON decode error when fetching Google Sheets data: {jde}")
            logger.error("This usually means the API returned HTML instead of JSON")
            return None
            
        except Exception as api_e:
            logger.error(f"API error when fetching Google Sheets data: {api_e}")
            return None
        
        # Validate the response
        if not isinstance(result, dict):
            logger.error(f"Unexpected result type from Sheets API: {type(result)}. Raw result: {result}")
            return None
        
        # Check if we got any data
        values = result.get("values", [])
        if not values:
            logger.warning("No data found in Google Sheets range")
            return pd.DataFrame()
        
        # Validate that we have at least headers
        if len(values) < 1:
            logger.error("No headers found in the data")
            return pd.DataFrame()
        
        # Create DataFrame with proper error handling
        try:
            if len(values) == 1:
                # Only headers, no data rows
                df = pd.DataFrame(columns=values[0])
                logger.info("Only headers found, returning empty DataFrame with column names")
            else:
                # Headers + data rows
                df = pd.DataFrame(values[1:], columns=values[0])
                logger.info(f"Successfully retrieved {len(df)} rows from Google Sheets")
            
            # Clean up column names (remove extra spaces)
            df.columns = df.columns.str.strip()
            
            # Update cache
            _sheets_data_cache = df
            _last_fetch_time = current_time
            
            return df
            
        except Exception as df_error:
            logger.error(f"Error creating DataFrame: {df_error}")
            logger.error(f"Raw values structure: {values[:5]}")
            return None
    
    except FileNotFoundError as fnf:
        logger.error(f"Service account file not found: {fnf}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in fetch_data_from_google_sheets: {str(e)}", exc_info=True)
        return None
def test_target_sheet_connection():
    """Test the connection to the target Google Sheet and verify its structure"""
    try:
        logger.info("Testing target Google Sheet connection...")
        # Check if target service account file exists
        if not os.path.exists(TARGET_SERVICE_ACCOUNT_FILE):
            logger.error(f"Target service account file not found: {TARGET_SERVICE_ACCOUNT_FILE}")
            return False
        # Create credentials for target sheet
        credentials = service_account.Credentials.from_service_account_file(
            TARGET_SERVICE_ACCOUNT_FILE, scopes=TARGET_SCOPES
        )
        service = build("sheets", "v4", credentials=credentials)
        sheet = service.spreadsheets()
        # Try to read the target sheet
        result = sheet.values().get(
            spreadsheetId=TARGET_SPREADSHEET_ID, 
            range=TARGET_RANGE_NAME
        ).execute()
        values = result.get("values", [])
        if not values:
            logger.warning("Target sheet appears to be empty")
            return True  # Still consider it a success if we can connect
        logger.info(f"Target sheet has {len(values)} rows")
        if values:
            logger.info(f"First row (headers): {values[0]}")
        return True
    except Exception as e:
        logger.error(f"Error testing target sheet connection: {str(e)}", exc_info=True)
        return False
def extract_compatibility_text_from_email(email_message):
    """Extract only the compatibility match details from the email message in plain text format
    This is a simpler version that focuses on HTML parsing"""
    try:
        if not email_message:
            logger.warning("Empty email message provided")
            return ""
        
        # Convert to string if it's not already
        email_message = str(email_message)
        
        # Use BeautifulSoup if available, otherwise use regex
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(email_message, 'html.parser')
            
            # Find all <p> tags that contain match information
            match_paragraphs = []
            
            for p in soup.find_all('p'):
                text = p.get_text().strip()
                # Check if this paragraph contains a numbered match
                if re.match(r'^\d+\.\s+.*Overall Compatibility Score.*\d+%', text):
                    # Extract and format the match information
                    lines = text.split('\n')
                    formatted_lines = []
                    
                    for line in lines:
                        line = line.strip()
                        if line:
                            # Remove excessive whitespace
                            line = re.sub(r'\s+', ' ', line)
                            # Format match headers with bold
                            if re.match(r'^\d+\.\s+.*Overall Compatibility Score', line):
                                formatted_lines.append(f"{line}")
                            else:
                                formatted_lines.append(f"       {line}")
                    
                    if formatted_lines:
                        match_paragraphs.append('    '.join(formatted_lines))
            
            if match_paragraphs:
                result = '\n'.join(match_paragraphs)
                logger.info(f"Successfully extracted {len(match_paragraphs)} matches using BeautifulSoup")
                return result
                
        except ImportError:
            logger.info("BeautifulSoup not available, using regex approach")
        
        # Fallback to regex approach
        # Remove HTML tags but keep the content structure
        import re
        from html import unescape
        
        # Remove HTML tags but keep line breaks
        clean_text = re.sub(r'<br[^>]*>', '\n', email_message)
        clean_text = re.sub(r'<p[^>]*>', '\n', clean_text)
        clean_text = re.sub(r'</p>', '\n', clean_text)
        clean_text = re.sub(r'<[^>]+>', '', clean_text)
        
        # Decode HTML entities
        clean_text = unescape(clean_text)
        
        # Clean up whitespace but preserve line structure
        lines = [line.strip() for line in clean_text.split('\n')]
        
        # Find lines with match information
        match_lines = []
        current_match = []
        
        for line in lines:
            if not line:
                continue
                
            # Check if this is a match header line
            if re.match(r'^\d+\.\s+.*Overall Compatibility Score.*\d+%', line):
                # Save previous match if exists
                if current_match:
                    match_lines.append(format_match_block(current_match))
                # Start new match
                current_match = [f"**{line}**"]
            elif current_match and ('Personal, Professional' in line or 
                                  'Favorites, Likes' in line or 
                                  'Other Requirements' in line or
                                  'Compatibility Breakdown' in line):
                if 'Compatibility Breakdown' in line:
                    current_match.append('    Compatibility Breakdown:')
                else:
                    # Format as indented detail
                    clean_line = line.replace('-', '').strip()
                    if clean_line:
                        current_match.append(f'       - {clean_line}')
        
        # Add the last match
        if current_match:
            match_lines.append(format_match_block(current_match))
        
        if match_lines:
            result = '\n'.join(match_lines)
            logger.info(f"Successfully extracted {len(match_lines)} matches using regex")
            return result
        
        logger.warning("No compatibility information could be extracted")
        return "No compatibility information available"
        
    except Exception as e:
        logger.error(f"Error extracting compatibility text from email: {str(e)}", exc_info=True)
        return f"Error processing compatibility information: {str(e)}"
def create_compatibility_text_directly(top_matches, new_user_name=None):
    """Create plain text email message directly from match data identical to create_email_message text 
    but without PDF reference - for storing in 'Email Text' column"""
    try:
        if not isinstance(top_matches, pd.DataFrame) or len(top_matches) == 0:
            logger.warning("No valid match data provided")
            return "No matches found"
        
        # Ensure user_name is provided
        if not new_user_name:
            new_user_name = "User"
        
        # Ensure the user's name is in title case
        name_title_case = str(new_user_name).title() if new_user_name else "User"
        
        # Calculate overall compatibility score for proper sorting
        top_matches_copy = top_matches.copy()
        top_matches_copy['Overall_Score'] = (
            top_matches_copy.get('PPF %', 0) + 
            top_matches_copy.get('FavLikes %', 0) + 
            top_matches_copy.get('Others %', 0)
        ) / 3
        
        # Sort by Overall_Score in DESCENDING order (highest percentage first)
        sorted_matches = top_matches_copy.sort_values(by='Overall_Score', ascending=False).reset_index(drop=True)
        
        # Start of the plain text message
        message_text = f"""Dear {name_title_case},
Congratulations on creating your Sapta.ai Digital Persona.
Here are your closest matches with Compatibility scores:
"""
        
        match_parts = []
        match_counter = 1
        for _, row in sorted_matches.iterrows():
            try:
                match_name = row.get('Full Name', 'Unknown')
                ppf_score = float(row.get('PPF %', 0))
                fav_likes_score = float(row.get('FavLikes %', 0))
                others_score = float(row.get('Others %', 0))
                overall_score = row.get('Overall_Score', 0)
                
                # Skip "No Match Found" entries
                if match_name == "No Match Found" or overall_score == 0:
                    continue

                # Format each match in plain text with proper spacing
                match_text = f"{match_counter}. {match_name} - Overall Compatibility Score : {overall_score:.1f}%\n   Compatibility Breakdown:\n       - Personal, Professional & Family Details: {ppf_score:.1f}%\n      - Favorites, Likes & Hobbies: {fav_likes_score:.1f}%\n     - Other Requirements and Preferences: {others_score:.1f}%"
                
                match_parts.append(match_text)
                match_counter += 1
                logger.info(f"Processed match {match_counter-1}: {match_name} with score {overall_score:.1f}%")
                
            except Exception as e:
                logger.error(f"Error processing match: {e}")
                continue
        
        # Join all matches
        if match_parts:
            message_text += '\n'.join(match_parts)
        
        # Add closing message without PDF reference
        message_text += f"""
Best Wishes

Team Sapta.ai"""
        
        logger.info(f"Successfully created plain text email message for {match_counter-1} matches")
        return message_text
            
    except Exception as e:
        logger.error(f"Error creating plain text email message: {str(e)}", exc_info=True)
        return f"Error creating email message: {str(e)}"
def format_match_block(match_parts):
    """Format a match block into a single line with proper spacing"""
    try:
        if not match_parts:
            return ""
        
        # Join all parts with appropriate spacing
        return '    '.join(match_parts)
        
    except Exception as e:
        logger.error(f"Error formatting match block: {e}")
        return '    '.join(match_parts) if match_parts else ""

def format_match_text(match_info):
    """Format match information into the desired text format"""
    try:
        main_line = match_info['main']
        details = match_info.get('details', [])
        
        # Extract match number and name from main line
        match_pattern = re.search(r'(\d+)\.\s*([^-]+?)\s*-\s*Overall Compatibility Score\s*:\s*([\d.]+)%', main_line)
        if not match_pattern:
            return f"**{main_line}**"
        
        match_num = match_pattern.group(1)
        name = match_pattern.group(2).strip()
        overall_score = match_pattern.group(3)
        
        # Format the basic match info
        formatted_text = f"{match_num}. {name} - Overall Compatibility Score : {overall_score}%"
        
        # Add compatibility breakdown if details are available
        if details:
            formatted_text += "    Compatibility Breakdown:"
            for detail in details:
                # Clean up the detail line
                clean_detail = detail.replace('-', '').strip()
                if clean_detail:
                    formatted_text += f"       - {clean_detail}"
        
        return formatted_text
        
    except Exception as e:
        logger.error(f"Error formatting match text: {e}")
        return f"**{match_info.get('main', 'Unknown match')}**"

def clean_email_content(email_message):
    """Clean email content by removing headers and footers"""
    try:
        lines = email_message.split('\n')
        
        # Remove common email headers
        start_idx = 0
        for i, line in enumerate(lines):
            if any(header in line.lower() for header in [
                'from:', 'to:', 'subject:', 'date:', 'cc:', 'bcc:'
            ]):
                continue
            else:
                start_idx = i
                break
        
        # Remove common email footers
        end_idx = len(lines)
        for i in range(len(lines) - 1, -1, -1):
            line = lines[i].strip().lower()
            if any(footer in line for footer in [
                'unsubscribe', 'privacy policy', 'terms of service',
                'this email was sent', 'if you no longer wish'
            ]):
                end_idx = i
            elif line and not any(footer in line for footer in [
                'unsubscribe', 'privacy policy', 'terms of service'
            ]):
                break
        
        cleaned_lines = lines[start_idx:end_idx]
        return '\n'.join(cleaned_lines).strip()
        
    except Exception as e:
        logger.error(f"Error cleaning email content: {str(e)}")
        return email_message
def write_name_to_target_sheet(user_name, whatsapp_number=None, email_address=None, birth_date=None, location=None, pdf_url=None, top_match_urls=None, email_text=None):
    """Write the user name, WhatsApp number, email, birth date, location, PDF URL, top 5 match PDF URLs, and email text to the target Google Sheet with auto-incrementing Sr No"""
    try:
        # Input validation and cleaning
        if not user_name or not str(user_name).strip():
            logger.warning("Empty or invalid user name provided, skipping target sheet update")
            return False
            
        user_name = str(user_name).strip()
        whatsapp_number = str(whatsapp_number).strip() if whatsapp_number and str(whatsapp_number).strip() else ""
        email_address = str(email_address).strip() if email_address and str(email_address).strip() else ""
        birth_date = str(birth_date).strip() if birth_date and str(birth_date).strip() else ""
        location = str(location).strip() if location and str(location).strip() else ""
        pdf_url = str(pdf_url).strip() if pdf_url and str(pdf_url).strip() else ""
        
        # Handle email_text specifically
        if email_text is None:
            email_text = ""
            logger.warning("email_text is None, setting to empty string")
        else:
            email_text = str(email_text).strip()
            logger.info(f"Processing email_text with length: {len(email_text)}")
            
        # Debug logging
        logger.info(f"Input parameters for target sheet:")
        logger.info(f"  user_name: '{user_name}' (type: {type(user_name)})")
        logger.info(f"  whatsapp_number: '{whatsapp_number}' (type: {type(whatsapp_number)})")
        logger.info(f"  email_address: '{email_address}' (type: {type(email_address)})")
        logger.info(f"  birth_date: '{birth_date}' (type: {type(birth_date)})")
        logger.info(f"  location: '{location}' (type: {type(location)})")
        logger.info(f"  pdf_url: '{pdf_url}' (type: {type(pdf_url)})")
        logger.info(f"  email_text length: {len(email_text)} (type: {type(email_text)})")
        
        # Initialize and handle top match URLs
        if top_match_urls is None:
            top_match_urls = ["", "", "", "", ""]  # 5 empty strings for top 1-5
            logger.info("top_match_urls was None, initialized with 5 empty strings")
        elif len(top_match_urls) < 5:
            original_length = len(top_match_urls)
            # Pad with empty strings if less than 5 URLs provided
            top_match_urls.extend([""] * (5 - len(top_match_urls)))
            logger.info(f"Padded top_match_urls from {original_length} to 5 URLs")
        elif len(top_match_urls) > 5:
            # Truncate to 5 if more than 5 URLs provided
            top_match_urls = top_match_urls[:5]
            logger.info("Truncated top_match_urls to 5 URLs")
        
        # Clean up URLs
        top_match_urls = [str(url).strip() if url and str(url).strip() else "" for url in top_match_urls]
        
        logger.info(f"Final top_match_urls: {top_match_urls}")
        
        # Check if target service account file exists
        if not os.path.exists(TARGET_SERVICE_ACCOUNT_FILE):
            logger.error(f"Target service account file not found: {TARGET_SERVICE_ACCOUNT_FILE}")
            return False
        
        # Create credentials for target sheet
        credentials = service_account.Credentials.from_service_account_file(
            TARGET_SERVICE_ACCOUNT_FILE, scopes=TARGET_SCOPES
        )
        service = build("sheets", "v4", credentials=credentials)
        sheet = service.spreadsheets()
        
        # Get current data to determine the next Sr No
        try:
            result = sheet.values().get(
                spreadsheetId=TARGET_SPREADSHEET_ID, 
                range=TARGET_RANGE_NAME
            ).execute()
            values = result.get("values", [])
            logger.info(f"Retrieved {len(values)} rows from target sheet")
        except Exception as e:
            logger.error(f"Error reading target sheet: {e}")
            return False
        
        # Calculate next Sr No (assuming first column is Sr No)
        next_sr_no = 1
        if values and len(values) > 1:  # If there are existing rows (excluding header)
            try:
                # Find the highest Sr No in the first column
                sr_nos = []
                for row in values[1:]:  # Skip header row
                    if row and len(row) > 0:
                        try:
                            sr_nos.append(int(row[0]))
                        except (ValueError, IndexError):
                            continue
                if sr_nos:
                    next_sr_no = max(sr_nos) + 1
                    logger.info(f"Calculated next Sr No: {next_sr_no}")
            except Exception as e:
                logger.warning(f"Error calculating next Sr No, using 1: {e}")
                next_sr_no = 1
        
        # Prepare the new row data: Sr No, Name, WhatsApp Number, Email, Birth Date, Location, PDF URL, Top1 URL, Top2 URL, Top3 URL, Top4 URL, Top5 URL, Email Text
        new_row = [next_sr_no, user_name, whatsapp_number, email_address, birth_date, location, pdf_url] + top_match_urls + [email_text]
        
        logger.info(f"Prepared new row with {len(new_row)} columns:")
        for i, value in enumerate(new_row):
            if i < 7:  # First 7 columns
                logger.info(f"  Column {i}: '{value}' (length: {len(str(value))})")
            elif i < 12:  # Top match URLs
                logger.info(f"  Match URL {i-6}: '{value}' (length: {len(str(value))})")
            else:  # Email text
                logger.info(f"  Email text: length {len(str(value))}, preview: '{str(value)[:100]}...'")
        
        # Append the new row to the sheet
        body = {'values': [new_row]}
        
        try:
            result = sheet.values().append(
                spreadsheetId=TARGET_SPREADSHEET_ID,
                range=TARGET_RANGE_NAME,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            logger.info(f"Successfully added row to target sheet with Sr No {next_sr_no}")
            logger.info(f"Google Sheets API response: {result}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing to Google Sheets: {str(e)}")
            logger.error(f"Request body was: {body}")
            return False
        
    except Exception as e:
        logger.error(f"Error in write_name_to_target_sheet: {str(e)}", exc_info=True)
        return False
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any

def process_category_matches(new_user, potential_match, category_info=None):
    """
    Enhanced matching algorithm with proper requirement filtering and accurate scoring:
    1. Personal, Professional & Family (40%)
    2. Favorites, Likes & Hobbies (35%)
    3. Others (25%)
    
    Key improvements:
    - Hard filters for critical requirements (caste, religion, etc.)
    - Bi-directional preference matching
    - Enhanced scoring with requirement priorities
    - Better handling of missing data
    """
    
    # Define field categories with their exact column names
    personal_fields = [
        'Requirements & Preferences [Own business]',
        'Requirements & Preferences [Own house]',
        'Requirements & Preferences [Non-resident national]',
        'Requirements & Preferences [Staying alone]',
        'Requirements & Preferences [Financially independent]'
    ]
    
    professional_fields = [
        'Requirements & Preferences [Higher studies]',
        'Requirements & Preferences [Government service]',
        'Requirements & Preferences [Qualified professional]',
        'Requirements & Preferences [Highly educated]'
    ]
    
    family_fields = [
        'Requirements & Preferences [Small family]',
        'Requirements & Preferences [Joint family]',
        'Requirements & Preferences [With children]',
        'Requirements & Preferences [W/o children]'
    ]
    
    # Combine for PPF category
    ppf_fields = personal_fields + professional_fields + family_fields
    
    # Favorites, Likes & Hobbies category
    fav_likes_fields = [
        'Requirements & Preferences [Hobbies match]',
        'Requirements & Preferences [Likes]',
        'Requirements & Preferences [Dislikes]'
    ]
    
    # Others category
    others_fields = [
        'Requirements & Preferences [Re-marriage]',
        'Requirements & Preferences [Metro city]',
        'Requirements & Preferences [Kundli match]'
    ]
    
    # Critical requirements that act as hard filters
    critical_requirements = [
        'Requirements & Preferences [Caste]',
        'Requirements & Preferences [Religion]',
        'Requirements & Preferences [Re-marriage]',
        'Requirements & Preferences [With children]',
        'Requirements & Preferences [W/o children]',
        'Requirements & Preferences [Kundli match]'
    ]
    
    def normalize_value(value):
        """Enhanced normalization for better comparison"""
        if pd.isna(value) or value is None:
            return ""
        
        value = str(value).strip().lower()
        
        # Handle common variations
        replacements = {
            "yes": "true", "no": "false",
            "n/a": "", "none": "", "null": "",
            "any": "", "doesn't matter": "",
            "open": "", "flexible": ""
        }
        
        for old, new in replacements.items():
            value = value.replace(old, new)
        
        return value
    
    def get_user_actual_value(user_data, preference_field):
        """Get user's actual value for a given preference field"""
        # Map preference fields to actual user data fields
        preference_to_actual = {
            'Requirements & Preferences [Caste]': ['Caste', 'Community', 'Sub-caste'],
            'Requirements & Preferences [Religion]': ['Religion', 'Faith'],
            'Requirements & Preferences [Own business]': ['Occupation', 'Business Owner', 'Self Employed'],
            'Requirements & Preferences [Own house]': ['House Ownership', 'Property Owner'],
            'Requirements & Preferences [Higher studies]': ['Education', 'Qualification'],
            'Requirements & Preferences [Government service]': ['Job Type', 'Employment Sector'],
            'Requirements & Preferences [With children]': ['Children', 'Kids', 'Family Status'],
            'Requirements & Preferences [Re-marriage]': ['Marital Status', 'Marriage History'],
            'Requirements & Preferences [Metro city]': ['City', 'Location', 'Current Location']
        }
        
        possible_fields = preference_to_actual.get(preference_field, [])
        
        for field in possible_fields:
            for col in user_data.columns if hasattr(user_data, 'columns') else user_data.index:
                if field.lower() in col.lower():
                    return user_data[col].values[0] if hasattr(user_data, 'columns') else user_data[col]
        
        return None
    
    def check_critical_requirements(user, match):
        """Check if critical requirements are met - acts as hard filter"""
        filter_passed = True
        failed_requirements = []
        
        for req_field in critical_requirements:
            # Check user's requirement
            user_req_col = find_matching_column(user, req_field)
            match_req_col = find_matching_column(match, req_field)
            
            if user_req_col:
                user_requirement = normalize_value(user[user_req_col].values[0] if hasattr(user, 'columns') else user[user_req_col])
                
                if user_requirement and user_requirement not in ['', 'false', 'any', 'open']:
                    # Get match's actual value for this requirement
                    match_actual_value = get_user_actual_value(match, req_field)
                    
                    if match_actual_value:
                        match_actual_normalized = normalize_value(match_actual_value)
                        
                        # Special handling for different requirement types
                        if not check_requirement_compatibility(req_field, user_requirement, match_actual_normalized):
                            filter_passed = False
                            failed_requirements.append(f"{req_field}: wanted '{user_requirement}', got '{match_actual_normalized}'")
            
            # Check match's requirement against user's actual value (bi-directional)
            if match_req_col:
                match_requirement = normalize_value(match[match_req_col] if not hasattr(match, 'columns') else match[match_req_col].values[0])
                
                if match_requirement and match_requirement not in ['', 'false', 'any', 'open']:
                    user_actual_value = get_user_actual_value(user, req_field)
                    
                    if user_actual_value:
                        user_actual_normalized = normalize_value(user_actual_value)
                        
                        if not check_requirement_compatibility(req_field, match_requirement, user_actual_normalized):
                            filter_passed = False
                            failed_requirements.append(f"{req_field} (reverse): wanted '{match_requirement}', got '{user_actual_normalized}'")
        
        return filter_passed, failed_requirements
    
    def check_requirement_compatibility(req_field, required_value, actual_value):
        """Check if actual value meets the requirement"""
        if not required_value or not actual_value:
            return True
        
        # Handle different types of requirements
        if 'caste' in req_field.lower() or 'religion' in req_field.lower():
            # Exact match for caste/religion
            return required_value == actual_value
        
        elif 'children' in req_field.lower():
            # Handle children preferences
            if 'with children' in req_field.lower():
                return 'yes' in actual_value or 'have' in actual_value or actual_value == 'true'
            elif 'w/o children' in req_field.lower():
                return 'no' in actual_value or 'none' in actual_value or actual_value == 'false'
        
        elif 'marriage' in req_field.lower():
            # Handle re-marriage preferences
            if required_value == 'true':
                return 'divorced' in actual_value or 'widowed' in actual_value or 'remarriage' in actual_value
            else:
                return 'single' in actual_value or 'never married' in actual_value
        
        else:
            # For other boolean requirements
            return required_value == actual_value or (required_value == 'true' and actual_value in ['yes', 'true'])
    
    def find_matching_column(data, field_name):
        """Find column that matches the field name"""
        columns = data.columns if hasattr(data, 'columns') else data.index
        return next((col for col in columns if field_name.lower() in col.lower()), None)
    
    def calculate_field_score(user_val, match_val, field_name):
        """Calculate score for a single field with enhanced matching logic"""
        user_val = normalize_value(user_val)
        match_val = normalize_value(match_val)
        
        if not user_val or not match_val:
            return 0.0
        
        # If user doesn't have a preference, don't penalize
        if user_val in ['', 'any', 'open', 'flexible']:
            return 0.5  # Neutral score
        
        # Exact match gets full points
        if user_val == match_val:
            return 1.0
        
        # Special handling for hobbies and likes (comma-separated lists)
        if any(keyword in field_name.lower() for keyword in ['hobbies', 'likes', 'interests']):
            user_items = set(item.strip() for item in user_val.split(',') if item.strip())
            match_items = set(item.strip() for item in match_val.split(',') if item.strip())
            
            if user_items and match_items:
                common_items = user_items.intersection(match_items)
                if common_items:
                    # Score based on overlap percentage
                    return len(common_items) / len(user_items.union(match_items))
        
        # Handle dislikes (reverse matching)
        if 'dislike' in field_name.lower():
            user_dislikes = set(item.strip() for item in user_val.split(',') if item.strip())
            match_traits = set(item.strip() for item in match_val.split(',') if item.strip())
            
            if user_dislikes and match_traits:
                conflicts = user_dislikes.intersection(match_traits)
                if conflicts:
                    return 0.0  # Complete mismatch if dislikes found
                return 1.0  # No conflicts found
        
        # Boolean field matching
        if user_val in ['true', 'false'] and match_val in ['true', 'false']:
            return 1.0 if user_val == match_val else 0.0
        
        # Text similarity for other fields
        if len(user_val) > 0 and len(match_val) > 0:
            user_words = set(user_val.split())
            match_words = set(match_val.split())
            common_words = user_words.intersection(match_words)
            
            if common_words:
                # Calculate Jaccard similarity
                return len(common_words) / len(user_words.union(match_words))
        
        return 0.0
    
    def calculate_category_score(fields, category_name):
        """Calculate score for a category with proper weighting"""
        total_score = 0.0
        total_weight = 0.0
        field_details = []
        
        # Define field importance weights
        field_weights = {
            'Requirements & Preferences [Own house]': 1.3,
            'Requirements & Preferences [Own business]': 1.2,
            'Requirements & Preferences [Financially independent]': 1.4,
            'Requirements & Preferences [Qualified professional]': 1.2,
            'Requirements & Preferences [Highly educated]': 1.1,
            'Requirements & Preferences [Higher studies]': 1.1,
            'Requirements & Preferences [Hobbies match]': 1.5,
            'Requirements & Preferences [Likes]': 1.3,
            'Requirements & Preferences [Dislikes]': 1.4,  # High weight for dislikes
            'Requirements & Preferences [Kundli match]': 1.2,
            'Requirements & Preferences [Metro city]': 1.1
        }
        
        for field in fields:
            user_col = find_matching_column(new_user, field)
            match_col = find_matching_column(potential_match, field)
            
            if user_col and match_col:
                user_val = new_user[user_col].values[0] if hasattr(new_user, 'columns') else new_user[user_col]
                match_val = potential_match[match_col] if not hasattr(potential_match, 'columns') else potential_match[match_col].values[0]
                
                # Only calculate score if user has a meaningful preference
                normalized_user_val = normalize_value(user_val)
                if normalized_user_val and normalized_user_val not in ['', 'false']:
                    field_weight = field_weights.get(field, 1.0)
                    field_score = calculate_field_score(user_val, match_val, field)
                    
                    total_score += field_score * field_weight
                    total_weight += field_weight
                    
                    field_details.append({
                        'field': field,
                        'user_value': str(user_val),
                        'match_value': str(match_val),
                        'score': field_score,
                        'weight': field_weight
                    })
        
        # Calculate weighted percentage
        if total_weight > 0:
            category_score = (total_score / total_weight) * 100
            # Ensure minimum score of 5% if any fields matched
            return max(5.0, min(100.0, category_score)), field_details
        
        return 5.0, field_details  # Minimum score for categories with no valid comparisons
    
    # First, check critical requirements (hard filters)
    requirements_passed, failed_reqs = check_critical_requirements(new_user, potential_match)
    
    if not requirements_passed:
        return {
            'matches': [],
            'total_score': 0.0,
            'final_percentage': 0.0,
            'requirements_passed': False,
            'failed_requirements': failed_reqs,
            'category_scores': {
                'personal_professional_family': {'score': 0.0, 'weight': 0.40},
                'favorites_likes_hobbies': {'score': 0.0, 'weight': 0.35},
                'others': {'score': 0.0, 'weight': 0.25}
            }
        }
    
    # Calculate category scores
    ppf_score, ppf_details = calculate_category_score(ppf_fields, 'personal_professional_family')
    fav_score, fav_details = calculate_category_score(fav_likes_fields, 'favorites_likes_hobbies')
    others_score, others_details = calculate_category_score(others_fields, 'others')
    
    # Calculate weighted total
    category_weights = {'ppf': 0.40, 'fav': 0.35, 'others': 0.25}
    weighted_total = (ppf_score * category_weights['ppf'] + 
                     fav_score * category_weights['fav'] + 
                     others_score * category_weights['others'])
    
    # Apply bonus for high compatibility
    if weighted_total > 80:
        weighted_total = min(100, weighted_total * 1.05)  # 5% bonus for high compatibility
    
    # Ensure minimum score of 15% for passed requirements
    final_score = max(15.0, weighted_total)
    
    return {
        'matches': ppf_details + fav_details + others_details,
        'total_score': weighted_total,
        'total_weight': 1.0,
        'final_percentage': final_score,
        'requirements_passed': True,
        'failed_requirements': [],
        'category_scores': {
            'personal_professional_family': {
                'score': ppf_score,
                'weight': category_weights['ppf'],
                'details': ppf_details
            },
            'favorites_likes_hobbies': {
                'score': fav_score,
                'weight': category_weights['fav'],
                'details': fav_details
            },
            'others': {
                'score': others_score,
                'weight': category_weights['others'],
                'details': others_details
            }
        }
    }


def filter_potential_matches(user_data, all_potential_matches):
    """
    Pre-filter potential matches based on critical requirements before detailed scoring
    """
    filtered_matches = []
    
    for match in all_potential_matches:
        result = process_category_matches(user_data, match)
        
        # Only include matches that pass critical requirements and have reasonable compatibility
        if result['requirements_passed'] and result['final_percentage'] >= 20:
            filtered_matches.append({
                'match_data': match,
                'compatibility_score': result['final_percentage'],
                'match_details': result
            })
    
    # Sort by compatibility score
    filtered_matches.sort(key=lambda x: x['compatibility_score'], reverse=True)
    
    return filtered_matches


# Usage example:
"""
# Example usage
user = pd.DataFrame({...})  # User's data
potential_matches = [...]   # List of potential match DataFrames/Series

# Get filtered and scored matches
compatible_matches = filter_potential_matches(user, potential_matches)

# Process top matches
for match_info in compatible_matches[:10]:  # Top 10 matches
    match_data = match_info['match_data']
    score = match_info['compatibility_score']
    details = match_info['match_details']
    
    print(f"Compatibility: {score:.1f}%")
    print(f"Requirements passed: {details['requirements_passed']}")
    if not details['requirements_passed']:
        print(f"Failed requirements: {details['failed_requirements']}")
"""
def process_matrimonial_data(df):
    """Process matrimonial data with optimized matching"""
    # Clean up column names and trim string whitespace
    df.columns = df.columns.str.strip()
    df = df.apply(lambda x: x.map(lambda v: str(v).strip()) if x.dtype == "object" else x)
    # Find email column
    possible_email_cols = [col for col in df.columns if "email" in col.lower()]
    if not possible_email_cols:
        raise ValueError("No column containing 'email' found.")
    email_col = possible_email_cols[0]
    df[email_col] = df[email_col].astype(str).str.strip()
    if len(df) < 2:
        logger.error("Not enough data for matching.")
        return None
    # Separate new user and existing users
    new_user = df.iloc[-1:]
    existing_users = df.iloc[:-1]
    new_user_email = new_user[email_col].values[0]
    new_user_name = new_user["Full Name"].values[0] if "Full Name" in new_user.columns else "New User"
    # Extract WhatsApp number
    whatsapp_col = None
    for col in new_user.columns:
        if "whatsapp" in col.lower() and "number" in col.lower():
            whatsapp_col = col
            break
    new_user_whatsapp = ""
    if whatsapp_col:
        new_user_whatsapp = new_user[whatsapp_col].values[0] if pd.notna(new_user[whatsapp_col].values[0]) else ""
    else:
        logger.warning("WhatsApp number column not found in source data")
    # Extract Birth Date
    birth_date_col = None
    for col in new_user.columns:
        if "birth" in col.lower() and "date" in col.lower():
            birth_date_col = col
            break
    new_user_birth_date = ""
    if birth_date_col:
        new_user_birth_date = new_user[birth_date_col].values[0] if pd.notna(new_user[birth_date_col].values[0]) else ""
    else:
        logger.warning("Birth date column not found in source data")
    # Extract Location (City, State, Country)
    city_col = None
    state_col = None
    country_col = None
    # Find City column
    for col in new_user.columns:
        if (col.strip().lower() == "city" or "city" in col.lower()) and "preference" not in col.lower() and "metro" not in col.lower():
            city_col = col
            break
    # Find State column
    for col in new_user.columns:
        if col.strip().lower() == "state" or "state" in col.lower():
            state_col = col
            break
    # Find Country column
    for col in new_user.columns:
        if col.strip().lower() == "country" or "country" in col.lower():
            country_col = col
            break
    # Build location string
    location_parts = []
    if city_col:
        city_value = new_user[city_col].values[0] if pd.notna(new_user[city_col].values[0]) else ""
        if city_value and city_value.strip():
            # Clean up city value (remove prefixes like "City:", "Prefer", etc.)
            city_value = re.sub(r'^(City:|Prefer)\s*', '', city_value.strip(), flags=re.IGNORECASE)
            if city_value:
                location_parts.append(city_value)
    if state_col:
        state_value = new_user[state_col].values[0] if pd.notna(new_user[state_col].values[0]) else ""
        if state_value and state_value.strip():
            location_parts.append(state_value.strip())
    if country_col:
        country_value = new_user[country_col].values[0] if pd.notna(new_user[country_col].values[0]) else ""
        if country_value and country_value.strip():
            location_parts.append(country_value.strip())
    new_user_location = ", ".join(location_parts) if location_parts else ""
    if not new_user_location:
        logger.warning("No location information found in source data")
    # Filter by gender
    GENDER_COL = "Gender"
    filtered_users = existing_users
    if GENDER_COL in new_user.columns and GENDER_COL in existing_users.columns:
        new_user_gender = str(new_user[GENDER_COL].values[0]).strip().lower()
        existing_users_gender = existing_users[GENDER_COL].fillna("").astype(str).str.lower()
        if "male" in new_user_gender and "female" not in new_user_gender:
            filtered_users = existing_users[existing_users_gender.str.contains("female", na=False)]
        elif "female" in new_user_gender:
            filtered_users = existing_users[
                existing_users_gender.str.contains("male", na=False) & 
                ~existing_users_gender.str.contains("female", na=False)
            ]
    if filtered_users.empty:
        filtered_users = existing_users
    # Process matches
    matches = []
    match_percentages = []
    match_details_list = []
    ppf_scores = []
    fav_likes_scores = []
    others_scores = []
    for _, potential_match in filtered_users.iterrows():
        # Process matches using the enhanced category matching
        match_result = process_category_matches(new_user, potential_match, None)
        if match_result['total_weight'] > 0:
            matches.append(potential_match)
            match_percentages.append(match_result['final_percentage'])
            match_details_list.append(match_result)
            # Store category breakdowns for easy access in DataFrame
            ppf_scores.append(match_result['category_scores']['personal_professional_family']['score'])
            fav_likes_scores.append(match_result['category_scores']['favorites_likes_hobbies']['score'])
            others_scores.append(match_result['category_scores']['others']['score'])
    # Sort matches by percentage (DESCENDING ORDER - HIGHEST FIRST)
    sorted_indices = sorted(range(len(match_percentages)), key=lambda i: match_percentages[i], reverse=True)
    top_matches = [matches[i] for i in sorted_indices[:3]]
    top_percentages = [match_percentages[i] for i in sorted_indices[:3]]
    top_match_details = [match_details_list[i] for i in sorted_indices[:3]]
    top_ppf_scores = [ppf_scores[i] for i in sorted_indices[:3]]
    top_fav_likes_scores = [fav_likes_scores[i] for i in sorted_indices[:3]]
    top_others_scores = [others_scores[i] for i in sorted_indices[:3]]
    # Pad to 3 matches if needed
    while len(top_matches) < 3:
        # Create a Series with the same columns as your DataFrame
        empty_row = pd.Series({col: "" for col in df.columns})
        empty_row["Full Name"] = "No Match Found"
        empty_row["PPF %"] = 0
        empty_row["FavLikes %"] = 0
        empty_row["Others %"] = 0
        top_matches.append(empty_row)
        top_percentages.append(0)
        top_match_details.append({})
        top_ppf_scores.append(0)
        top_fav_likes_scores.append(0)
        top_others_scores.append(0)
    # Create DataFrame for top matches
    top_matches_df = pd.DataFrame(top_matches)
    top_matches_df['Match Percentage'] = top_percentages
    top_matches_df['Match Details'] = top_match_details
    top_matches_df['PPF %'] = top_ppf_scores
    top_matches_df['FavLikes %'] = top_fav_likes_scores
    top_matches_df['Others %'] = top_others_scores
    
    # CRITICAL: Sort the DataFrame by Match Percentage in DESCENDING order
    top_matches_df = top_matches_df.sort_values(by='Match Percentage', ascending=False)
    # Reset index to ensure proper ordering
    top_matches_df = top_matches_df.reset_index(drop=True)

    return (
        new_user,
        new_user_name,
        new_user_email,
        new_user_whatsapp,
        new_user_birth_date,
        new_user_location,
        top_matches_df,
        top_percentages,
        top_matches_df
    )
def create_last_response_pdf(new_user, new_user_name, email_col, match_percentage=None):
    """Create a PDF for the last response using the same format as match PDFs"""
    try:
        pdf = EnhancedSinglePageMatchesPDF()
        pdf.add_page()
        # Add match percentage display if provided (similar to match PDFs)
        if match_percentage is not None:
            # Add match percentage at top right corner
            pdf.set_font("Arial", "B", 12)
            pdf.set_text_color(220, 20, 60)  # Crimson color
            match_text = f"Match: {match_percentage}%"
            text_width = pdf.get_string_width(match_text)
            pdf.set_xy(pdf.w - text_width - 15, 15)  # Position at top right
            pdf.cell(text_width, 8, match_text, border=0, align='R')
        # Add vertical space after BIODATA
        current_y = 50  # Start below enhanced header
        current_y += 3  # Reduced extra vertical space after BIODATA
        # Add profile photo to the right side of the first page
        photo_added = add_enhanced_photo_to_pdf(pdf, new_user, email_col)
        if photo_added:
            pdf.left_column_width = 110  # Adjust for larger photo
        else:
            pdf.left_column_width = 140
        # First Page Sections
        # Personal Details Section
        current_y = add_compact_section(pdf, "Personal Details", current_y)
        personal_fields = [
            ("Name", "Full Name"),
            ("Birth Date", "Birth Date"),
            ("Birth Time", "Birth Time"),
            ("Birth Place", "Birth Place"),
            ("Height", "Height"),
            ("Weight", "Weight"),
            ("Religion", "Religion"),
            ("Caste / Community", "Caste / Community"),
            ("Mother Tongue", "Mother Tongue"),
            ("Nationality", "Nationality"),
        ]
        for display_name, field_name in personal_fields:
            matching_field = next(
                (
                    col
                    for col in new_user.columns
                    if field_name.lower() in col.lower()
                ),
                None,
            )
            if matching_field:
                current_y = add_compact_field(
                    pdf,
                    display_name,
                    new_user[matching_field].values[0],
                    current_y,
                )
        # Professional Details Section
        current_y += 5
        current_y = add_compact_section(pdf, "Professional Details", current_y)
        career_fields = [
            ("Education", "Education"),
            ("Qualification", "Qualification"),
            ("Occupation", "Occupation"),
            ("Organization / Company Name", "Organization / Company Name"),
        ]
        for display_name, field_name in career_fields:
            matching_field = next(
                (
                    col
                    for col in new_user.columns
                    if field_name.lower() in col.lower()
                ),
                None,
            )
            if matching_field:
                current_y = add_compact_field(
                    pdf,
                    display_name,
                    new_user[matching_field].values[0],
                    current_y,
                )
        # Family Information Section
        family_fields = [col for col in new_user.columns if "Family Information" in col]
        if family_fields:
            current_y += 5
            current_y = add_compact_section(pdf, "Family Information", current_y)
            current_y += 3
            family_count = 0
            for field in family_fields:
                if family_count >= 20 or current_y > 245:
                    break
                value = new_user[field].values[0]
                if pd.notna(value) and str(value).strip().lower() not in ["", "no", "n/a"]:
                    match = re.search(r"\[(.*?)\]", field)
                    if match:
                        label = match.group(1)[:35]
                        # --- Start of new logic ---
                        # Set font and position for the label
                        pdf.set_xy(15, current_y)
                        pdf.set_font("Arial", "B", 10)
                        pdf.set_text_color(50, 50, 50)
                        # Print the label in a cell of fixed width
                        pdf.cell(55, 5, f"{label}", border=0)
                        # Set font for the value
                        pdf.set_font("Arial", "", 10)
                        pdf.set_text_color(0, 0, 0)
                        # Calculate the remaining width on the page
                        start_x_for_value = 15 + 55  # Left margin + label width
                        right_margin = 15
                        available_width = pdf.w - start_x_for_value - right_margin
                        # Set the X position to be right after the label
                        pdf.set_x(start_x_for_value)
                        # Print the full value in a single line using the calculated width
                        pdf.cell(available_width, 5, str(value), border=0)
                        # Move to the next line for the next field
                        current_y += 5
                        family_count += 1
                        # --- End of new logic ---
        # Hobbies Section - SIMPLIFIED LOGIC (NO IMAGE WRAPPING)
        hobbies_col = next(
            (
                col
                for col in new_user.columns
                if "favorite" in col.lower() or "hobby" in col.lower()
            ),
            None,
        )
        if hobbies_col:
            current_y += 5
            current_y = add_compact_section(pdf, "Hobbies & Interests", current_y)
            current_y += 3
            hobbies = new_user[hobbies_col].values[0]
            if pd.notna(hobbies) and str(hobbies).strip():
                hobbies_text = str(hobbies)
                pdf.set_y(current_y)
                pdf.set_x(15)
                pdf.set_font("Arial", "", 10)
                pdf.set_text_color(0)
                # Calculate available width from left margin to right margin
                left_margin = 15
                right_margin = 15  # Standard right margin
                available_width = pdf.w - left_margin - right_margin
                # Check if text fits in single line
                text_width = pdf.get_string_width(hobbies_text)
                if text_width > available_width:
                    # Text is too long for single line, wrap it
                    pdf.multi_cell(available_width, 4, hobbies_text, border=0)
                    current_y = pdf.get_y()
                else:
                    # Text fits in single line
                    pdf.cell(available_width, 4, hobbies_text, border=0)
                    current_y += 4
        # Add 'Formal - Full length' photo at the right bottom of the first page (fixed size)
        add_formal_full_length_photo_to_pdf(pdf, new_user, email_col)
        # Second Page Sections
        pdf.add_page()
        # Place candid photo on second page, center right
        add_candid_photo_to_second_page(pdf, new_user, email_col)
        current_y = 45
        current_y += 8  # Add the same spacing as first page after BIODATA
        # Requirements & Preferences Section
        preference_fields = [col for col in new_user.columns if "Requirements & Preferences" in col]
        if preference_fields:
            current_y = add_compact_section(pdf, "Requirements & Preferences", current_y)
            current_y += 3
            # Enhanced Requirements extraction + Original Preferences logic
            all_requirements = []
            all_preferences = []  # Collect all preference values first (ORIGINAL LOGIC)
            logger.info(f"Processing {len(preference_fields)} preference fields")
            # Extract ALL requirement names where user has selected/filled values (NO FILTERING OR DUPLICATE REMOVAL)
            for field in preference_fields:
                value = new_user[field].values[0]
                # Check if user has provided any value (including "No" - we'll show all selected fields)
                if pd.notna(value) and str(value).strip() != "":
                    match = re.search(r"\[(.*?)\]", field)
                    if match:
                        label = match.group(1)[:35]
                        original_label = label
                        value_str = str(value).strip()
                        # Extract requirement name for ALL fields with values (no filtering)
                        clean_requirement = label.strip()
                        # Remove "Prefer" from the beginning if it exists
                        clean_requirement = re.sub(r'^Prefer\s+', '', clean_requirement, flags=re.IGNORECASE).strip()
                        if clean_requirement:
                            # Proper capitalization
                            clean_requirement = ' '.join(word.capitalize() for word in clean_requirement.split())
                            # Add ALL requirements without any filtering or duplicate removal
                            all_requirements.append(clean_requirement)
                            logger.info(f"Added requirement: '{clean_requirement}' from field: '{original_label}' with user value: '{value_str}'")
                        # ORIGINAL PREFERENCES LOGIC (UNCHANGED) - Only applies to preferences
                        # Only include if value is not empty, not 'no', not 'n/a', not 'no other preferences'
                        if str(value).strip().lower() not in ["", "no", "n/a", "no other preferences"]:
                            # Remove "Prefer" from the beginning of the label if it exists
                            pref_label = re.sub(r'^Prefer\s+', '', label, flags=re.IGNORECASE)
                            # Clean up the preference value
                            pref_value = str(value).strip()
                            # Remove "Prefer" and related words from anywhere in the value if they exist (more comprehensive)
                            pref_value = re.sub(r'\b(Prefer|Preferred|Preference)\b\s*', '', pref_value, flags=re.IGNORECASE)
                            # Clean up any extra whitespace that might be left
                            pref_value = re.sub(r'\s+', ' ', pref_value).strip()
                            # Capitalize the first letter of each word in the remaining text
                            if pref_value:
                                pref_value = ' '.join(word.capitalize() for word in pref_value.split())
                            if pref_value:  # Only add non-empty values
                                # Split by comma if the value contains multiple preferences
                                if "," in pref_value:
                                    individual_prefs = [p.strip() for p in pref_value.split(",") if p.strip()]
                                    for individual_pref in individual_prefs:
                                        # Filter out "No Other Preferences" from individual preferences
                                        if individual_pref.lower() != "no other preferences":
                                            all_preferences.append(individual_pref)
                                            logger.info(f"Added individual preference: '{individual_pref}' from field: '{field}'")
                                        else:
                                            logger.info(f"Skipped 'No Other Preferences' from field: '{field}'")
                                else:
                                    # Filter out "No Other Preferences" from single preference values
                                    if pref_value.lower() != "no other preferences":
                                        all_preferences.append(pref_value)
                                        logger.info(f"Added preference: '{pref_value}' from field: '{field}'")
                                    else:
                                        logger.info(f"Skipped 'No Other Preferences' from field: '{field}'")
            logger.info(f"Total preferences collected: {len(all_preferences)}")
            logger.info(f"All preferences before deduplication: {all_preferences}")
            # ORIGINAL PREFERENCES DEDUPLICATION LOGIC (UNCHANGED)
            # Second pass: remove duplicates while maintaining order
            seen_preferences = set()
            unique_preferences = []
            for pref in all_preferences:
                if pref not in seen_preferences:
                    seen_preferences.add(pref)
                    unique_preferences.append(pref)
                    logger.info(f"Added unique preference: '{pref}'")
                else:
                    logger.info(f"Skipped duplicate preference: '{pref}'")
            logger.info(f"Final unique preferences: {unique_preferences}")
            logger.info(f"Total requirements collected: {len(all_requirements)}")
            logger.info(f"All requirements: {all_requirements}")
            # Display Requirements section
            if all_requirements:
                pdf.set_y(current_y)
                pdf.set_x(15)
                pdf.set_font("Arial", "B", 10)
                pdf.set_text_color(50, 50, 50)
                pdf.cell(0, 5, "Requirements:", ln=1, border=0)  # ln=1 moves to next line
                pdf.set_font("Arial", "", 10)
                pdf.set_text_color(0, 0, 0)
                req_text = ", ".join(all_requirements)
                pdf.set_x(20)
                # Add right padding by reducing the width of the multi_cell so text doesn't touch the right edge
                right_padding = 15  # in mm, adjust as needed
                cell_width = pdf.w - 20 - right_padding
                pdf.multi_cell(cell_width, 5, req_text, border=0)
                current_y = pdf.get_y() + 2
            # ORIGINAL PREFERENCES DISPLAY LOGIC (UNCHANGED)
            # Only display Preferences section if there are actual preferences to show
            if unique_preferences:
                pdf.set_y(current_y)
                pdf.set_x(15)
                pdf.set_font("Arial", "B", 10)
                pdf.set_text_color(50, 50, 50)
                pdf.cell(0, 5, "Preferences:", ln=1, border=0)  # ln=1 moves to next line
                pdf.set_font("Arial", "", 10)
                pdf.set_text_color(0, 0, 0)
                pref_text = ", ".join(unique_preferences)  # Use the deduplicated list
                pdf.set_x(20)
                # Add right padding by reducing the width of the multi_cell so text doesn't touch the right edge
                right_padding = 15  # in mm, adjust as needed
                cell_width = pdf.w - 20 - right_padding
                pdf.multi_cell(cell_width, 5, pref_text, border=0)
                current_y = pdf.get_y() + 2
        # Any Other Specific Choice Section
        any_other_choice_col = next(
            (
                col
                for col in new_user.columns
                if "any other specific choice" in col.lower()
            ),
            None,
        )
        if any_other_choice_col:
            any_other_choice = new_user[any_other_choice_col].values[0]
            if pd.notna(any_other_choice) and str(any_other_choice).strip():
                current_y += 5
                current_y = add_compact_section(
                    pdf, "Any Other Specific Choice", current_y
                )
                current_y += 3
                pdf.set_y(current_y)
                pdf.set_x(15)
                pdf.set_font("Arial", "", 10)
                pdf.set_text_color(0)
                # Calculate available width from left margin to right margin
                left_margin = 15
                right_margin = 15  # Standard right margin
                available_width = pdf.w - left_margin - right_margin
                # Use multi_cell to handle text wrapping automatically
                pdf.multi_cell(available_width, 4, str(any_other_choice), border=0)
                current_y = pdf.get_y()
        # Location Section
        current_y += 5
        current_y = add_compact_section(pdf, "Current Location", current_y)
        # Get the city value directly from the City column - improved search
        city_col = None
        # First try exact match for "City"
        if "City" in new_user.columns:
            city_col = "City"
        # Then try case-insensitive search with stripped spaces (but avoid preference fields)
        elif any(col.strip().lower() == "city" and "preference" not in col.lower() for col in new_user.columns):
            city_col = next(col for col in new_user.columns if col.strip().lower() == "city" and "preference" not in col.lower())
        # Finally try partial match (but avoid preference fields)
        elif any("city" in col.lower() and "preference" not in col.lower() and "metro" not in col.lower() for col in new_user.columns):
            city_col = next(col for col in new_user.columns if "city" in col.lower() and "preference" not in col.lower() and "metro" not in col.lower())
        if city_col:
            city_value = new_user[city_col].values[0] if pd.notna(new_user[city_col].values[0]) else ""
            logger.info(f"DEBUG: Raw city value from column '{city_col}': '{city_value}'")
            if pd.notna(city_value) and str(city_value).strip():
                # Clean up the city value
                city_value = str(city_value).strip()
                logger.info(f"DEBUG: After strip city value: '{city_value}'")
                # Remove any prefixes like "City:" or "Prefer"
                city_value = re.sub(r'^(City:|Prefer)\s*', '', city_value, flags=re.IGNORECASE)
                logger.info(f"DEBUG: After regex cleanup city value: '{city_value}'")
                # Don't truncate city names - use the full cleaned value
                if city_value:
                    current_y = add_compact_field(pdf, "City", city_value, current_y)
                    logger.info(f"DEBUG: Added city field to PDF: '{city_value}'")
        else:
            logger.warning(f"DEBUG: No city column found in data. Available columns: {list(new_user.columns)}")
        # Handle other location fields
        location_fields = [("State", "State"), ("Country", "Country")]
        for display_name, field_name in location_fields:
            matching_field = next(
                (
                    col
                    for col in new_user.columns
                    if field_name.lower() in col.lower()
                ),
                None,
            )
            if matching_field:
                value = new_user[matching_field].values[0]
                if pd.notna(value) and str(value).strip():
                    value = str(value).strip()
                    current_y = add_compact_field(
                        pdf,
                        display_name,
                        value,
                        current_y,
                    )
        # Contact Information (Email only)
        current_y += 5  # Increased spacing between Current Location and Contact Details
        current_y = add_compact_section(pdf, "Contact Details", current_y)
        # Add email field with improved indentation
        current_y = add_compact_field(pdf, "Email", new_user[email_col].values[0], current_y, label_width=50)
        # Save the PDF
        output_filename = f"{new_user_name}.pdf"
        pdf.output(output_filename)
        logger.info(f"Created last response PDF: {output_filename}")
        return output_filename
    except Exception as e:
        logger.error(f"Failed to create last response PDF: {e}")
        return None
def extract_drive_id(link):
    if not link or not isinstance(link, str) or "drive.google.com" not in link:
        return None
    try:
        patterns = [
            r"/file/d/([a-zA-Z0-9_-]+)",  # Standard sharing link
            r"[?&]id=([a-zA-Z0-9_-]+)",  # Query parameter format
            r"/document/d/([a-zA-Z0-9_-]+)",  # Google Docs format
            r"drive\.google\.com/([a-zA-Z0-9_-]{25,})",  # Direct ID in URL
            r"([a-zA-Z0-9_-]{25,})",  # Last resort - any long alphanumeric string
        ]
        for pattern in patterns:
            match = re.search(pattern, link)
            if match:
                file_id = match.group(1)
                # Validate that it looks like a proper Google Drive file ID
                if len(file_id) >= 25:  # Google Drive IDs are typically 28+ characters
                    return file_id
    except Exception as e:
        logger.error(f"Error extracting Drive ID: {e}")
    return None
def download_drive_image(drive_link, save_filename="temp_image.jpg"):
    """
    Downloads an image from a Google Drive link and compresses it to reduce file size.
    """
    if not drive_link or "drive.google.com" not in drive_link:
        logger.warning(f"Invalid or missing Google Drive link: {drive_link}")
        return None
    try:
        file_id = extract_drive_id(drive_link)
        if not file_id:
            logger.error(f"Could not extract file ID from link: {drive_link}")
            return None
            
        download_url = f"https://drive.google.com/uc?id={file_id}&export=download"
        
        session = requests.Session()
        response = session.get(download_url, timeout=30)
        
        # Handle Google Drive virus scan warning page if it appears
        if "NID" in session.cookies:
            token_match = re.search(r'confirm=([a-zA-Z0-9_-]+)', response.text)
            if token_match:
                token = token_match.group(1)
                params = {"id": file_id, "export": "download", "confirm": token}
                response = session.get("https://drive.google.com/uc", params=params, timeout=30)

        if response.status_code == 200 and "image" in response.headers.get("Content-Type", ""):
            # Save the original downloaded image
            with open(save_filename, "wb") as f:
                f.write(response.content)

            # --- START: IMAGE COMPRESSION LOGIC ---
            try:
                with Image.open(save_filename) as img:
                    # Convert to RGB if it's RGBA (removes alpha channel for smaller size)
                    if img.mode == 'RGBA':
                        img = img.convert('RGB')
                    
                    # Resize the image to a maximum dimension (e.g., 1024x1024) while keeping aspect ratio
                    img.thumbnail((1024, 1024))
                    
                    # Save the image again with optimized quality
                    img.save(save_filename, "JPEG", quality=85, optimize=True)
                    logger.info(f"Successfully downloaded and compressed image: {save_filename}")
                return save_filename
            except Exception as e:
                logger.warning(f"Could not compress image {save_filename}. Using original. Error: {e}")
                return save_filename # Return original if compression fails
            # --- END: IMAGE COMPRESSION LOGIC ---
        else:
            logger.error(f"Failed to download a valid image for file ID: {file_id}. Status: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"An error occurred in download_drive_image: {e}")
        return None
class EnhancedSinglePageMatchesPDF(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=False)  # Disable auto page break for single page
        self.left_column_width = 120  # Width for text content
        self.right_column_x = 140  # X position for photo
        self.photo_width = 60  # Increased photo width
        self.photo_height = 100  # Significantly increased photo height
        self.current_y_pos = 45  # Track vertical position (moved down for header space)
        # Enhanced designer color scheme
        self.primary_color = (0, 51, 102)  # Dark blue
        self.accent_color = (220, 50, 50)  # Red
        self.gold_color = (184, 134, 11)  # Golden
        self.border_color = (100, 100, 100)  # Gray
        self.text_color = (0, 0, 0)  # Black
        self.light_blue = (173, 216, 235)  # Light blue
        self.cream_color = (255, 253, 240)  # Cream
    def add_corner_flourish(self, x, y, size, position):
        """Add decorative flourish elements at corners"""
        try:
            # Simple decorative element - just a small circle
            self.set_fill_color(200, 180, 140)  # Gold color
            self.set_draw_color(200, 180, 140)
            # Draw a small decorative circle
            if hasattr(self, "circle"):
                self.circle(x, y, 0.5, "F")
            else:
                # Fallback to rectangle if circle method not available
                self.rect(x - 0.5, y - 0.5, 1.0, 1.0, "F")
        except Exception as e:
            # Fail silently for decorative elements
            pass
    # ... rest of your existing methods ...
    def add_decorative_border(self):
        """Add comprehensive attractive decorative border"""
        # Multiple layer border design
        self.add_outer_frame()
        self.add_ornate_border_pattern()
        self.add_corner_medallions()
    def add_outer_frame(self):
        """Add the main outer frame with gradient effect"""
        # Outer thick border
        self.set_draw_color(*self.primary_color)
        self.set_line_width(3)
        self.rect(3, 3, self.w - 6, self.h - 6)
        # Secondary border with golden color
        self.set_draw_color(*self.gold_color)
        self.set_line_width(2)
        self.rect(6, 6, self.w - 12, self.h - 12)
        # Inner fine border
        self.set_draw_color(*self.primary_color)
        self.set_line_width(0.8)
        self.rect(9, 9, self.w - 18, self.h - 18)
    def add_ornate_border_pattern(self):
        """Add uniform ornate patterns on all borders"""
        self.set_draw_color(*self.gold_color)
        self.set_line_width(0.6)
        # All borders use the same diamond and scroll pattern
        self.add_uniform_border_pattern()
    def add_uniform_border_pattern(self):
        """Add the same decorative pattern to all four borders"""
        pattern_spacing = 15
        # Top border pattern
        y_pos = 7.5
        start_x = 25
        for x in range(int(start_x), int(self.w - 25), pattern_spacing):
            try:
                self.draw_diamond(x, y_pos, 3)
            except:
                pass
        # Bottom border pattern (same as top)
        y_pos = self.h - 7.5
        for x in range(int(start_x), int(self.w - 25), pattern_spacing):
            try:
                self.draw_diamond(x, y_pos, 3)
            except:
                pass
        # Left border pattern (rotated version of top pattern)
        x_pos = 7.5
        start_y = 25
        for y in range(int(start_y), int(self.h - 25), pattern_spacing):
            try:
                self.draw_diamond(x_pos, y, 3)
            except:
                pass
        # Right border pattern (same as left)
        x_pos = self.w - 7.5
        for y in range(int(start_y), int(self.h - 25), pattern_spacing):
            try:
                self.draw_diamond(x_pos, y, 3)
            except:
                pass
    def add_corner_medallions(self):
        """Add elaborate corner medallions"""
        try:
            self.set_draw_color(*self.primary_color)
            self.set_line_width(0.6)
            diamond_size = 2
            # TOP-LEFT diamond
            cx, cy = 15, 15
            self.line(cx, cy - diamond_size, cx + diamond_size, cy)  # Top to right
            self.line(cx + diamond_size, cy, cx, cy + diamond_size)  # Right to bottom
            self.line(cx, cy + diamond_size, cx - diamond_size, cy)  # Bottom to left
            self.line(cx - diamond_size, cy, cx, cy - diamond_size)  # Left to top
            # TOP-RIGHT diamond
            cx, cy = self.w - 15, 15
            self.line(cx, cy - diamond_size, cx + diamond_size, cy)
            self.line(cx + diamond_size, cy, cx, cy + diamond_size)
            self.line(cx, cy + diamond_size, cx - diamond_size, cy)
            self.line(cx - diamond_size, cy, cx, cy - diamond_size)
            # BOTTOM-LEFT diamond
            cx, cy = 15, self.h - 15
            self.line(cx, cy - diamond_size, cx + diamond_size, cy)
            self.line(cx + diamond_size, cy, cx, cy + diamond_size)
            self.line(cx, cy + diamond_size, cx - diamond_size, cy)
            self.line(cx - diamond_size, cy, cx, cy - diamond_size)
            # BOTTOM-RIGHT diamond
            cx, cy = self.w - 15, self.h - 15
            self.line(cx, cy - diamond_size, cx + diamond_size, cy)
            self.line(cx + diamond_size, cy, cx, cy + diamond_size)
            self.line(cx, cy + diamond_size, cx - diamond_size, cy)
            self.line(cx - diamond_size, cy, cx, cy - diamond_size)
        except Exception as e:
            # Fail silently for decorative elements
            pass
    def add_side_flourishes(self):
        """Add uniform decorative flourishes on all sides"""
        # All sides use the same flourish design
        flourish_size = 8
        # Center flourish on left side
        self.draw_uniform_flourish(15, self.h / 2, flourish_size)
        # Center flourish on right side
        self.draw_uniform_flourish(self.w - 15, self.h / 2, flourish_size)
        # Top center flourish
        self.draw_uniform_flourish(self.w / 2, 15, flourish_size)
        # Bottom center flourish
        self.draw_uniform_flourish(self.w / 2, self.h - 15, flourish_size)
    def draw_uniform_flourish(self, x, y, size):
        """Draw the same flourish design for all sides"""
        self.set_draw_color(*self.accent_color)
        self.set_line_width(0.8)
        # Central motif - circle with radiating elements
        self.circle(x, y, size / 4, style="D")
        # Radiating decorative lines in 4 directions
        import math
        for angle in [0, 90, 180, 270]:  # Cardinal directions
            rad = math.radians(angle)
            x1 = x + (size / 4) * math.cos(rad)
            y1 = y + (size / 4) * math.sin(rad)
            x2 = x + (size / 2) * math.cos(rad)
            y2 = y + (size / 2) * math.sin(rad)
            self.line(x1, y1, x2, y2)
            # Small decorative element at the end
            self.circle(x2, y2, size / 8, style="D")
    def add_inner_accent_border(self):
        """Add inner decorative accent border"""
        self.set_draw_color(*self.accent_color)
        self.set_line_width(0.5)
        self.set_dash(2, 2)  # Dotted pattern
        self.rect(12, 12, self.w - 24, self.h - 24)
        self.set_dash()  # Reset to solid
    def draw_diamond(self, x, y, size):
        """Draw a diamond shape"""
        try:
            self.line(x, y - size, x + size, y)
            self.line(x + size, y, x, y + size)
            self.line(x, y + size, x - size, y)
            self.line(x - size, y, x, y - size)
        except Exception as e:
            # Fail silently for decorative elements
            pass
    def draw_connecting_scroll(self, x1, y, x2, Y):
        """Draw connecting scroll between elements"""
        import math
        mid_x = (x1 + x2) / 2
        # Create a wavy line
        segments = 8
        for i in range(segments):
            t = i / segments
            x = x1 + t * (x2 - x1)
            wave_y = y + 1.5 * math.sin(t * math.pi * 2)
            if i == 0:
                start_x, start_y = x, wave_y
            else:
                self.line(start_x, start_y, x, wave_y)
                start_x, start_y = x, wave_y
    def draw_connecting_scroll_vertical(self, x, y1, X, y2):
        """Draw vertical connecting scroll between elements"""
        import math
        mid_y = (y1 + y2) / 2
        # Create a wavy vertical line
        segments = 8
        for i in range(segments):
            t = i / segments
            y = y1 + t * (y2 - y1)
            wave_x = x + 1.5 * math.sin(t * math.pi * 2)
            if i == 0:
                start_x, start_y = wave_x, y
            else:
                self.line(start_x, start_y, wave_x, y)
                start_x, start_y = wave_x, y
    def draw_small_flourish(self, x, y, size, angle):
        """Draw small decorative flourish at given angle"""
        import math
        rad = math.radians(angle + 90)  # Perpendicular to the line
        # Small decorative cross
        x1 = x + size * math.cos(rad)
        y1 = y + size * math.sin(rad)
        x2 = x - size * math.cos(rad)
        y2 = y - size * math.sin(rad)
        self.line(x1, y1, x2, y2)
    # Remove unused methods that are no longer needed
    # (Keeping only the essential utility methods)
    def draw_corner_medallion(self, x, y, size, position):
        """Draw elaborate corner medallions"""
        self.set_draw_color(*self.gold_color)
        self.set_line_width(1.0)
        # Main medallion circle
        self.circle(x, y, size / 2, style="D")
        # Inner decorative circle
        self.set_line_width(0.6)
        self.circle(x, y, size / 4, style="D")
        # Radiating decorative elements based on corner position
        import math
        if position == "top-left":
            angles = [225, 270, 315]  # Bottom-right quadrant
        elif position == "top-right":
            angles = [135, 180, 225]  # Bottom-left quadrant
        elif position == "bottom-left":
            angles = [315, 0, 45]  # Top-right quadrant
        else:  # bottom-right
            angles = [45, 90, 135]  # Top-left quadrant
        # Draw radiating decorative lines
        for angle in angles:
            rad = math.radians(angle)
            x1 = x + (size / 2) * math.cos(rad)
            y1 = y + (size / 2) * math.sin(rad)
            x2 = x + (size * 0.8) * math.cos(rad)
            y2 = y + (size * 0.8) * math.sin(rad)
            self.line(x1, y1, x2, y2)
        # Add corner-specific decorative flourishes
        self.add_corner_flourish(x, y, size, position)
    # Keep only essential utility methods
    def arc(self, x, y, r, start_angle, end_angle):
        """Simple arc drawing method"""
        import math
        start_rad = math.radians(start_angle)
        end_rad = math.radians(end_angle)
        segments = 10
        angle_step = (end_rad - start_rad) / segments
        for i in range(segments):
            angle1 = start_rad + i * angle_step
            angle2 = start_rad + (i + 1) * angle_step
            x1 = x + r * math.cos(angle1)
            y1 = y + r * math.sin(angle1)
            x2 = x + r * math.cos(angle2)
            y2 = y + r * math.sin(angle2)
            self.line(x1, y1, x2, y2)
    def circle(self, x, y, r, style="D"):
        """Draw a circle"""
        try:
            import math
            segments = 16
            angle_step = 2 * math.pi / segments
            for i in range(segments):
                angle1 = i * angle_step
                angle2 = (i + 1) * angle_step
                x1 = x + r * math.cos(angle1)
                y1 = y + r * math.sin(angle1)
                x2 = x + r * math.cos(angle2)
                y2 = y + r * math.sin(angle2)
                self.line(x1, y1, x2, y2)
        except Exception as e:
            # Fail silently for decorative elements
            pass
    def curve(self, x1, y1, x2, y2, x3, y3, x4, y4):
        """Draw a bezier curve using line segments"""
        segments = 10
        for i in range(segments + 1):
            t = i / segments
            x = (
                (1 - t) ** 3 * x1
                + 3 * (1 - t) ** 2 * t * x2
                + 3 * (1 - t) * t**2 * x3
                + t**3 * x4
            )
            y = (
                (1 - t) ** 3 * y1
                + 3 * (1 - t) ** 2 * t * y2
                + 3 * (1 - t) * t**2 * y3
                + t**3 * y4
            )
            if i == 0:
                start_x, start_y = x, y
            else:
                self.line(start_x, start_y, x, y)
                start_x, start_y = x, y
    def set_dash(self, dash_length=0, space_length=0):
        """Set dash pattern for lines"""
        if dash_length > 0 and space_length >= 0:
            dash_string = "[{0} {1}] 0 d".format(
                dash_length * self.k, space_length * self.k
            )
        else:
            dash_string = "[] 0 d"
        self._out(dash_string)
    def header(self):
        # Add the enhanced decorative border
        self.add_decorative_border()
        # Add Ganesh image at the top center
        image_path = "logo.png"
        image_width_mm = 16.0  # Increased size for the logo in mm
        page_width = self.w  # Get page width
        image_x = (page_width - image_width_mm) / 2
        image_y = 15  # Position from the top
        try:
            if os.path.exists(image_path):
                # Calculate image height to position the title correctly
                img = Image.open(image_path)
                aspect_ratio = img.height / img.width
                image_height_mm = image_width_mm * aspect_ratio
                self.image(image_path, x=image_x, y=image_y, w=image_width_mm, h=image_height_mm) # Explicitly set height as well
                # Adjust y position for the title based on image height + spacing
                title_y = image_y + image_height_mm + 3 # Maintain padding after image
            else:
                logger.warning(f"Ganesh image not found at {image_path}. Skipping image.")
                title_y = 20 # Fallback if image not found
        except Exception as e:
            logger.error(f"Error adding Ganesh image to PDF: {e}")
            title_y = 20 # Fallback on error
        # Main title with enhanced styling
        self.set_font("Arial", "B", 18)
        self.set_text_color(*self.primary_color)
        self.set_y(title_y) # Use calculated or fallback y position
        self.cell(0, 10, "Sapta.ai Digital Persona", ln=True, align="C")
        # Subtitle with accent color
        # self.set_font("Arial", "B", 16)
        # self.set_text_color(*self.accent_color)
        # self.set_y(self.get_y() + 1) # Removed to eliminate extra space
        # self.ln(3)  # Removed to eliminate extra space
    def footer(self):
        # Enhanced footer with decorative elements
        self.set_y(-20)
        # Footer text
        self.set_font("Arial", "I", 9)
        self.set_text_color(*self.border_color)
        self.cell(
            0,
            10,
            f"Page {self.page_no()}",
            0,
            0,
            "C",
        )
def add_enhanced_photo_to_pdf(pdf, user_data, email_col):
    """Add user photo to the right side of the PDF with enhanced styling
    Modified to work with both DataFrame (pandas Series) and dictionary data"""
    
    # Handle both DataFrame and dictionary input
    if hasattr(user_data, 'columns'):  # DataFrame
        columns_list = user_data.columns
        def get_value(key, default=""):
            return user_data[key].values[0] if key in user_data.columns else default
    else:  # Dictionary
        columns_list = user_data.keys()
        def get_value(key, default=""):
            return user_data.get(key, default)
    
    # Find photo column
    photo_col = [
        col
        for col in columns_list
        if "photo" in col.lower() and "upload" in col.lower()
    ]
    if not photo_col:
        photo_col = [col for col in columns_list if "photo" in col.lower()]
    if not photo_col:
        logger.warning("No photo column found in form data")
        return False
    
    photo_col = photo_col[0]
    photo_link = get_value(photo_col, "")

    if (
        not isinstance(photo_link, str)
        or not photo_link.strip()
        or "http" not in photo_link.lower()
    ):
        logger.warning(
            f"No valid photo link found for {get_value('Full Name', 'Unknown user')}"
        )
        return False
    
    # Create safe filename
    email = get_value(email_col, "unknown")
    safe_name = re.sub(r"[^\w\-_]", "_", email)
    photo_path = f"temp_{safe_name}_photo.jpg"
    
    # Try to download the image
    img_path = download_drive_image(photo_link, save_filename=photo_path)

    if not img_path or not os.path.exists(img_path):
        logger.warning(
            f"Failed to download image for {get_value('Full Name', 'Unknown user')}"
        )
        return False
    
    # Add image to PDF with enhanced styling
    try:
        with Image.open(img_path) as img:
            # Fixed photo dimensions for first page (smaller size)
            photo_width = PHOTO_WIDTH  # Reduced width
            photo_height = PHOTO_HEIGHT  # Reduced height
            # Position photo in top-right corner with proper margins
            photo_x = pdf.w - photo_width - 15  # 15mm from right edge
            photo_y = 57  # Increased vertical space from BIODATA text (was 55)
            # Add decorative border around photo
            border_margin = 2
            pdf.set_draw_color(*pdf.primary_color)
            pdf.set_line_width(1)
            pdf.rect(
                photo_x - border_margin,
                photo_y - border_margin,
                photo_width + 2 * border_margin,
                photo_height + 2 * border_margin,
            )
            # Add photo
            pdf.image(
                img_path,
                x=photo_x,
                y=photo_y,
                w=photo_width,
                h=photo_height,
            )
            logger.info("Enhanced photo added to PDF successfully")
            return True
    except Exception as e:
        logger.error(f"Error adding enhanced image to PDF: {e}")
        return False
    finally:
        # Clean up the temp file
        if os.path.exists(img_path):
            try:
                os.remove(img_path)
            except Exception as e:
                logger.error(f"Error removing temp image: {e}")
    return False
def add_enhanced_section(pdf, title, y_pos):
    """Add an enhanced section header with decorative elements"""
    pdf.set_y(y_pos)
    # Decorative line before section
    pdf.set_draw_color(*pdf.accent_color)
    pdf.set_line_width(1)
    pdf.set_dash(2, 1)
    pdf.line(10, y_pos + 1, 15, y_pos + 1)
    pdf.line(10, y_pos + 5, 15, y_pos + 5)
    pdf.set_dash()
    # Section title
    pdf.set_text_color(*pdf.primary_color)
    pdf.set_x(18)
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 6, title, ln=True)
    # Decorative underline
    title_width = pdf.get_string_width(title)
    pdf.set_draw_color(*pdf.accent_color)
    pdf.set_line_width(0.3)
    pdf.set_dash(1, 1)
    pdf.line(18, y_pos + 7, 18 + title_width, y_pos + 7)
    pdf.set_dash()
    return y_pos + 10
def add_enhanced_field(pdf, label, value, y_pos, label_width=35):
    """Add a field with enhanced styling"""
    if (
        pd.notna(value)
        and str(value).strip()
        and str(value).strip().lower() not in ["no", "n/a", "none", ""]
    ):
        pdf.set_y(y_pos)
        # Label with enhanced styling
        pdf.set_font("Arial", "B", 10)
        pdf.set_text_color(*pdf.primary_color)
        pdf.set_x(20)
        pdf.cell(label_width, 5, f"{label}:", border=0)
        # Value with regular styling
        pdf.set_font("Arial", "", 10)
        pdf.set_text_color(*pdf.text_color)
        value_str = str(value)
        available_width = pdf.left_column_width - label_width - 25
        # Calculate max characters based on font and available width
        char_width = pdf.get_string_width("A")
        max_chars = int(available_width / char_width) - 5
        if len(value_str) > max_chars:
            value_str = value_str[: max_chars - 3] + "..."
        pdf.set_x(20 + label_width)
        pdf.cell(available_width, 5, value_str, border=0)
        return y_pos + 6
    return y_pos
def add_family_information_enhanced(pdf, matched_user, current_y):
    """Add family information with enhanced styling"""
    family_fields = [col for col in matched_user.keys() if "Family Information" in col]
    if not family_fields:
        return current_y
    # Check if we have space for family section
    if current_y > 240:
        return current_y
    current_y = add_enhanced_section(pdf, "Family Information", current_y)
    # Organize family fields by category
    family_categories = {
        "Parents": ["father", "mother", "parent"],
        "Siblings": ["brother", "sister", "sibling"],
        "Other": [],
    }
    # Categorize fields
    categorized_fields = {cat: [] for cat in family_categories.keys()}
    for field in family_fields:
        field_lower = field.lower()
        categorized = False
        for category, keywords in family_categories.items():
            if any(keyword in field_lower for keyword in keywords):
                categorized_fields[category].append(field)
                categorized = True
                break
        if not categorized:
            categorized_fields["Other"].append(field)
    # Add family information by category
    fields_added = 0
    max_family_fields = 12  # Adjusted for enhanced layout
    for category, fields in categorized_fields.items():
        if fields_added >= max_family_fields or current_y > 250:
            break
        category_has_content = False
        for field in fields:
            if fields_added >= max_family_fields or current_y > 250:
                break
            value = matched_user.get(field, "")
            if (
                pd.notna(value)
                and str(value).strip()
                and str(value).strip().lower() not in ["no", "n/a", "none", ""]
            ):
                # Extract label from field name
                label = extract_family_field_label(field)
                if label:
                    if not category_has_content and len(fields) > 1:
                        # Add mini category header
                        pdf.set_y(current_y)
                        pdf.set_x(20)
                        pdf.set_font("Arial", "I", 9)
                        pdf.set_text_color(*pdf.border_color)
                        pdf.cell(0, 4, f"{category}:", border=0)
                        current_y += 4
                        category_has_content = True
                    current_y = add_enhanced_field(pdf, label, value, current_y, 30)
                    fields_added += 1
    # Add spacing after family section
    if fields_added > 0:
        current_y += 3
    return current_y
def extract_family_field_label(field_name):
    """Extract a clean label from family field name"""
    # Remove "Family Information [" and "]" parts
    match = re.search(r"\[(.*?)\]", field_name)
    if match:
        label = match.group(1)
        # Clean up common patterns
        label = re.sub(r"^\d+\.?\s*", "", label)  # Remove leading numbers
        label = re.sub(r"\s*\(.*?\)", "", label)  # Remove parenthetical info
        label = label.strip()
        # Truncate if too long
        if len(label) > 20:
            label = label[:17] + "..."
        return label
    return None
def add_compact_section(pdf, title, y_pos):
    """Add a compact section title with proper spacing"""
    pdf.set_y(y_pos)
    pdf.set_x(15)
    pdf.set_font("Arial", "B", 12)
    pdf.set_text_color(50, 50, 50)
    pdf.cell(0, 6, title, border=0)  # Title height
    return y_pos + 8  # Increased spacing after section title from 4 to 8
def add_compact_field(pdf, label, value, y_pos, label_width=50):  # Changed default label_width to 50
    """Add a field with label and value in a compact format."""
    if not value:
        return y_pos
    # Handle "same" values by looking up the actual value
    value_str = str(value).strip().lower()
    if value_str.startswith("same"):
        # Try to find the actual value in the data
        actual_value = None
        if "build" in value_str:
            actual_value = "Average"  # Default to Average if not specified
        elif "mother tongue" in value_str:
            actual_value = "Gujarati"  # Default to Gujarati if not specified
        elif "religion" in value_str:
            actual_value = "Hindu"  # Default to Hindu if not specified
        elif "caste" in value_str:
            actual_value = "General"  # Default to General if not specified
        elif "education" in value_str:
            actual_value = "Graduate"  # Default to Graduate if not specified
        elif "occupation" in value_str:
            actual_value = "Private Job"  # Default to Private Job if not specified
        elif "income" in value_str:
            actual_value = "5-10 Lakhs"  # Default to 5-10 Lakhs if not specified
        elif "city" in value_str and "caste" not in value_str:  # Only match city if it's not caste
            # Don't use hardcoded default - use the original value or try to extract from context
            # If it's "same as city", we should use the original value or leave it as is
            actual_value = None  # Don't override with hardcoded value
        elif "state" in value_str:
            actual_value = "Gujarat"  # Default to Gujarat instead of Maharashtra
        elif "country" in value_str:
            actual_value = "India"  # Default to India if not specified
        if actual_value:
            value = actual_value
    # Set font for label
    pdf.set_font("Arial", "B", 10)
    pdf.set_text_color(64, 64, 64)  # Dark gray for label
    # Special handling for Caste/Community/Tribe field
    if label == "Caste / Community / Tribe" or label == "Caste / Community":
        # Draw label
        pdf.set_xy(15, y_pos)
        pdf.cell(label_width, 5, label, 0, 0, 'L')  # Removed colon
        # Draw value with proper wrapping
        pdf.set_font("Arial", "", 10)
        pdf.set_text_color(0, 0, 0)  # Black for value
        # Calculate available width for value
        available_width = 190 - (15 + label_width + 5)
        # Split value into words and create wrapped lines
        words = str(value).split()
        current_line = []
        current_width = 0
        lines = []
        for word in words:
            word_width = pdf.get_string_width(word + " ")
            if current_width + word_width <= available_width:
                current_line.append(word)
                current_width += word_width
            else:
                lines.append(" ".join(current_line))
                current_line = [word]
                current_width = word_width
        if current_line:
            lines.append(" ".join(current_line))
        # Draw each line with consistent spacing
        current_y = y_pos
        for i, line in enumerate(lines):
            if i > 0:  # Move to next line for wrapped text
                current_y += 5
                # Align wrapped text with the first line
                pdf.set_xy(15 + label_width + 5, current_y)
            else:
                # First line aligned with label
                pdf.set_xy(15 + label_width + 5, current_y)
            pdf.cell(available_width, 5, line, 0, 0, 'L')
        return current_y + 5  # Return new y position with proper spacing
    # For all other fields
    pdf.set_xy(15, y_pos)
    pdf.cell(label_width, 5, label, 0, 0, 'L')  # Removed colon
    # Set font for value
    pdf.set_font("Arial", "", 10)
    pdf.set_text_color(0, 0, 0)  # Black for value
    # Draw value with consistent spacing
    pdf.set_xy(15 + label_width + 5, y_pos)
    pdf.cell(190 - (15 + label_width + 5), 5, str(value), 0, 0, 'L')
    return y_pos + 5  # Return new y position
def create_single_page_match_pdf(
    matched_user,
    match_percentage,
    new_user_name,
    email_col,
    profile_number,
):
    """Create an enhanced single-page PDF with designer elements"""
    try:
        pdf = EnhancedSinglePageMatchesPDF()
        pdf.add_page()
        # Add vertical space after BIODATA
        current_y = 50  # Start below enhanced header
        current_y += 3  # Reduced extra vertical space after BIODATA from 8 to 3
        # Add photo to the right side with enhanced styling
        photo_added = add_enhanced_photo_to_pdf(pdf, matched_user, email_col)
        if photo_added:
            pdf.left_column_width = 110  # Adjust for larger photo
        else:
            pdf.left_column_width = 140
        # First Page Sections
        # Personal Details Section
        current_y = add_compact_section(pdf, "Personal Details", current_y)
        personal_fields = [
            ("Name", "Full Name"),
            ("Birth Date", "Birth Date"),
            ("Birth Time", "Birth Time"),
            ("Birth Place", "Birth Place"),
            ("Height", "Height"),
            ("Weight", "Weight"),
            ("Religion", "Religion"),
            ("Caste / Community", "Caste / Community"),
            ("Mother Tongue", "Mother Tongue"),
            ("Nationality", "Nationality"),
        ]
        for display_name, field_name in personal_fields:
            matching_field = next(
                (
                    col
                    for col in matched_user.keys()
                    if field_name.lower() in col.lower()
                ),
                None,
            )
            if matching_field:
                current_y = add_compact_field(
                    pdf,
                    display_name,
                    matched_user.get(matching_field, "N/A"),
                    current_y,
                )
        # Professional Details Section
        current_y += 5
        current_y = add_compact_section(pdf, "Professional Details", current_y)
        career_fields = [
            ("Education", "Education"),
            ("Qualification", "Qualification"),
            ("Occupation", "Occupation"),
            ("Organization / Company Name", "Organization / Company Name"),
        ]
        for display_name, field_name in career_fields:
            matching_field = next(
                (
                    col
                    for col in matched_user.keys()
                    if field_name.lower() in col.lower()
                ),
                None,
            )
            if matching_field:
                current_y = add_compact_field(
                    pdf,
                    display_name,
                    matched_user.get(matching_field, "N/A"),
                    current_y,
                )
        # Family Information Section
        family_fields = [
            col for col in matched_user.keys() if "Family Information" in col
        ]
        if family_fields:
            current_y += 5
            current_y = add_compact_section(pdf, "Family Information", current_y)
            current_y += 3
            family_count = 0
            for field in family_fields:
                if family_count >= 20 or current_y > 245:
                    break
                value = matched_user.get(field, "")
                if pd.notna(value) and str(value).strip().lower() not in [
                    "",
                    "no",
                    "n/a",
                ]:
                    match = re.search(r"\[(.*?)\]", field)
                    if match:
                        label = match.group(1)[:35]
                        # --- Start of new logic ---
                        # Set font and position for the label
                        pdf.set_xy(15, current_y)
                        pdf.set_font("Arial", "B", 10)
                        pdf.set_text_color(50, 50, 50)
                        # Print the label in a cell of fixed width
                        pdf.cell(55, 5, f"{label}", border=0)
                        # Set font for the value
                        pdf.set_font("Arial", "", 10)
                        pdf.set_text_color(0, 0, 0)
                        # Calculate the remaining width on the page
                        start_x_for_value = 15 + 55  # Left margin + label width
                        right_margin = 15
                        available_width = pdf.w - start_x_for_value - right_margin
                        # Set the X position to be right after the label
                        pdf.set_x(start_x_for_value)
                        # Print the full value in a single line using the calculated width
                        pdf.cell(available_width, 5, str(value), border=0)
                        # Move to the next line for the next field
                        current_y += 5
                        family_count += 1
                        # --- End of new logic ---
        # Hobbies Section - SIMPLIFIED LOGIC (NO IMAGE WRAPPING)
        hobbies_col = next(
            (
                col
                for col in matched_user.keys()
                if "favorite" in col.lower() or "hobby" in col.lower()
            ),
            None,
        )
        if hobbies_col:
            current_y += 5
            current_y = add_compact_section(pdf, "Hobbies & Interests", current_y)
            current_y += 3
            hobbies = matched_user.get(hobbies_col, "")
            if pd.notna(hobbies) and str(hobbies).strip():
                hobbies_text = str(hobbies)
                pdf.set_y(current_y)
                pdf.set_x(15)
                pdf.set_font("Arial", "", 10)
                pdf.set_text_color(0)
                # Calculate available width from left margin to right margin
                left_margin = 15
                right_margin = 15  # Standard right margin
                available_width = pdf.w - left_margin - right_margin
                # Check if text fits in single line
                text_width = pdf.get_string_width(hobbies_text)
                if text_width > available_width:
                    # Text is too long for single line, wrap it
                    pdf.multi_cell(available_width, 4, hobbies_text, border=0)
                    current_y = pdf.get_y()
                else:
                    # Text fits in single line
                    pdf.cell(available_width, 4, hobbies_text, border=0)
                    current_y += 4
        # Add 'Formal - Full length' photo at the right bottom of the first page (fixed size)
        add_formal_full_length_photo_to_pdf(pdf, matched_user, email_col)
        # Second Page Sections
        pdf.add_page()
        # Place candid photo on second page, center right
        add_candid_photo_to_second_page(pdf, matched_user, email_col)
        current_y = 45
        current_y += 8  # Add the same spacing as first page after BIODATA
        # Enhanced Requirements & Preferences Section
        preference_fields = [
            col for col in matched_user.keys() if "Requirements & Preferences" in col
        ]
        if preference_fields:
            current_y = add_compact_section(
                pdf, "Requirements & Preferences", current_y
            )
            current_y += 3
            # Enhanced Requirements extraction + Original Preferences logic
            all_requirements = []
            all_preferences = []  # Collect all preference values first (ORIGINAL LOGIC)
            logger.info(f"Processing {len(preference_fields)} preference fields for match PDF")
            # Extract ALL requirement names where user has selected/filled values (NO FILTERING OR DUPLICATE REMOVAL)
            for field in preference_fields:
                value = matched_user.get(field, "")
                # Check if user has provided any value (including "No" - we'll show all selected fields)
                if pd.notna(value) and str(value).strip() != "":
                    match = re.search(r"\[(.*?)\]", field)
                    if match:
                        label = match.group(1)[:35]
                        original_label = label
                        value_str = str(value).strip()
                        # Extract requirement name for ALL fields with values (no filtering)
                        clean_requirement = label.strip()
                        # Remove "Prefer" from the beginning if it exists
                        clean_requirement = re.sub(
                            r"^Prefer\s+", "", clean_requirement, flags=re.IGNORECASE
                        ).strip()
                        if clean_requirement:
                            # Proper capitalization
                            clean_requirement = " ".join(
                                word.capitalize() for word in clean_requirement.split()
                            )
                            # Add ALL requirements without any filtering or duplicate removal
                            all_requirements.append(clean_requirement)
                            logger.info(
                                f"Added requirement: '{clean_requirement}' from field: '{original_label}' with user value: '{value_str}'"
                            )
                        # ORIGINAL PREFERENCES LOGIC (UNCHANGED) - Only applies to preferences
                        # Only include if value is not empty, not 'no', not 'n/a', not 'no other preferences'
                        if str(value).strip().lower() not in [
                            "",
                            "no",
                            "n/a",
                            "no other preferences",
                        ]:
                            # Remove "Prefer" from the beginning of the label if it exists
                            pref_label = re.sub(
                                r"^Prefer\s+", "", label, flags=re.IGNORECASE
                            )
                            # Clean up the preference value
                            pref_value = str(value).strip()
                            # Remove "Prefer" and related words from anywhere in the value if they exist (more comprehensive)
                            pref_value = re.sub(
                                r"\b(Prefer|Preferred|Preference)\b\s*",
                                "",
                                pref_value,
                                flags=re.IGNORECASE,
                            )
                            # Clean up any extra whitespace that might be left
                            pref_value = re.sub(r"\s+", " ", pref_value).strip()
                            # Capitalize the first letter of each word in the remaining text
                            if pref_value:
                                pref_value = " ".join(
                                    word.capitalize() for word in pref_value.split()
                                )
                            if pref_value:  # Only add non-empty values
                                # Split by comma if the value contains multiple preferences
                                if "," in pref_value:
                                    individual_prefs = [
                                        p.strip() for p in pref_value.split(",") if p.strip()
                                    ]
                                    for individual_pref in individual_prefs:
                                        # Filter out "No Other Preferences" from individual preferences
                                        if individual_pref.lower() != "no other preferences":
                                            all_preferences.append(individual_pref)
                                            logger.info(
                                                f"Added individual preference: '{individual_pref}' from field: '{field}'"
                                            )
                                        else:
                                            logger.info(
                                                f"Skipped 'No Other Preferences' from field: '{field}'"
                                            )
                                else:
                                    # Filter out "No Other Preferences" from single preference values
                                    if pref_value.lower() != "no other preferences":
                                        all_preferences.append(pref_value)
                                        logger.info(
                                            f"Added preference: '{pref_value}' from field: '{field}'"
                                        )
                                    else:
                                        logger.info(
                                            f"Skipped 'No Other Preferences' from field: '{field}'"
                                        )
            logger.info(f"Total preferences collected for match: {len(all_preferences)}")
            logger.info(
                f"All preferences before deduplication for match: {all_preferences}"
            )
            # ORIGINAL PREFERENCES DEDUPLICATION LOGIC (UNCHANGED)
            # Second pass: remove duplicates while maintaining order
            seen_preferences = set()
            unique_preferences = []
            for pref in all_preferences:
                if pref not in seen_preferences:
                    seen_preferences.add(pref)
                    unique_preferences.append(pref)
                    logger.info(f"Added unique preference for match: '{pref}'")
                else:
                    logger.info(f"Skipped duplicate preference for match: '{pref}'")
            logger.info(f"Final unique preferences for match: {unique_preferences}")
            logger.info(
                f"Total requirements collected for match: {len(all_requirements)}"
            )
            logger.info(f"All requirements for match: {all_requirements}")
            # Display Requirements section
            if all_requirements:
                pdf.set_y(current_y)
                pdf.set_x(15)
                pdf.set_font("Arial", "B", 10)
                pdf.set_text_color(50, 50, 50)
                pdf.cell(0, 5, "Requirements:", ln=1, border=0)  # ln=1 moves to next line
                pdf.set_font("Arial", "", 10)
                pdf.set_text_color(0, 0, 0)
                req_text = ", ".join(all_requirements)
                pdf.set_x(20)
                # Add right padding by reducing the width of the multi_cell so text doesn't touch the right edge
                right_padding = 15  # in mm, adjust as needed
                cell_width = pdf.w - 20 - right_padding
                pdf.multi_cell(cell_width, 5, req_text, border=0)
                current_y = pdf.get_y() + 2
            # ORIGINAL PREFERENCES DISPLAY LOGIC (UNCHANGED)
            # Only display Preferences section if there are actual preferences to show
            if unique_preferences:
                pdf.set_y(current_y)
                pdf.set_x(15)
                pdf.set_font("Arial", "B", 10)
                pdf.set_text_color(50, 50, 50)
                pdf.cell(0, 5, "Preferences:", ln=1, border=0)  # ln=1 moves to next line
                pdf.set_font("Arial", "", 10)
                pdf.set_text_color(0, 0, 0)
                pref_text = ", ".join(unique_preferences)  # Use the deduplicated list
                pdf.set_x(20)
                # Add right padding by reducing the width of the multi_cell so text doesn't touch the right edge
                right_padding = 15  # in mm, adjust as needed
                cell_width = pdf.w - 20 - right_padding
                pdf.multi_cell(cell_width, 5, pref_text, border=0)
                current_y = pdf.get_y() + 2

        # NEW SECTION: Any Other Specific Choice
        # DEBUG: Print all available columns to help identify the correct column name
        logger.info(f"DEBUG: All available columns: {list(matched_user.keys())}")
        
        # Search for "Any Other Specific Choice" column with multiple variations
        specific_choice_col = None
        
        # Try multiple possible column name variations
        possible_names = [
            "Any Other Specific Choice",
            "any other specific choice",
            "Any other specific choice",
            "Other Specific Choice",
            "Specific Choice",
            "Other Choice"
        ]
        
        # First try exact matches
        for name in possible_names:
            if name in matched_user.keys():
                specific_choice_col = name
                break
        
        # If not found, try partial matches
        if not specific_choice_col:
            for col in matched_user.keys():
                if any(keyword in col.lower() for keyword in ["specific choice", "other choice", "other specific"]):
                    specific_choice_col = col
                    break
        
        logger.info(f"DEBUG: Found specific choice column: '{specific_choice_col}'")
        
        if specific_choice_col:
            specific_choice_value = matched_user.get(specific_choice_col, "")
            logger.info(f"DEBUG: Raw specific choice value: '{specific_choice_value}'")
            logger.info(f"DEBUG: Value type: {type(specific_choice_value)}")
            logger.info(f"DEBUG: Is not null: {pd.notna(specific_choice_value)}")
            logger.info(f"DEBUG: Stripped value: '{str(specific_choice_value).strip()}'")
            
            # More lenient content validation - show the section even with minimal content
            if pd.notna(specific_choice_value) and str(specific_choice_value).strip():
                choice_text = str(specific_choice_value).strip()
                
                # Only skip if it's clearly empty or a negative response
                skip_values = ["", "no", "n/a", "none", "nil", "not applicable", "-"]
                should_skip = choice_text.lower() in skip_values
                
                logger.info(f"DEBUG: Should skip section: {should_skip}")
                
                if not should_skip:
                    current_y += 5
                    current_y = add_compact_section(pdf, "Any Other Specific Choice", current_y)
                    current_y += 3
                    
                    # Set font and position
                    pdf.set_y(current_y)
                    pdf.set_x(15)
                    pdf.set_font("Arial", "", 10)
                    pdf.set_text_color(0, 0, 0)
                    
                    # Calculate available width
                    left_margin = 15
                    right_margin = 15
                    available_width = pdf.w - left_margin - right_margin
                    
                    # Check if text fits in single line
                    text_width = pdf.get_string_width(choice_text)
                    if text_width > available_width:
                        # Text is too long for single line, wrap it
                        pdf.multi_cell(available_width, 5, choice_text, border=0)
                        current_y = pdf.get_y() + 2
                    else:
                        # Text fits in single line
                        pdf.cell(available_width, 5, choice_text, border=0)
                        current_y += 7
                    
                    logger.info(f"SUCCESS: Added 'Any Other Specific Choice' section with content: '{choice_text}'")
                else:
                    logger.info(f"SKIPPED: 'Any Other Specific Choice' contains skip value: '{choice_text}'")
            else:
                logger.info("SKIPPED: 'Any Other Specific Choice' field is empty or null")
        else:
            logger.error("ERROR: 'Any Other Specific Choice' column not found in the data")
            # Force add the section for testing (remove this after debugging)
            logger.info("FORCE ADDING: Adding test section for debugging")
            current_y += 5
            current_y = add_compact_section(pdf, "Any Other Specific Choice", current_y)
            current_y += 3
            pdf.set_y(current_y)
            pdf.set_x(15)
            pdf.set_font("Arial", "", 10)
            pdf.set_text_color(0, 0, 0)
            pdf.cell(0, 5, "Column not found - check logs for available columns", border=0)
            current_y += 7

        # Location Section
        current_y += 5
        current_y = add_compact_section(pdf, "Current Location", current_y)
        # Get the city value directly from the City column - improved search
        city_col = None
        # First try exact match for "City"
        if "City" in matched_user.keys():
            city_col = "City"
        # Then try case-insensitive search with stripped spaces (but avoid preference fields)
        elif any(
            col.strip().lower() == "city" and "preference" not in col.lower()
            for col in matched_user.keys()
        ):
            city_col = next(
                col
                for col in matched_user.keys()
                if col.strip().lower() == "city" and "preference" not in col.lower()
            )
        # Finally try partial match (but avoid preference fields)
        elif any(
            "city" in col.lower()
            and "preference" not in col.lower()
            and "metro" not in col.lower()
            for col in matched_user.keys()
        ):
            city_col = next(
                col
                for col in matched_user.keys()
                if "city" in col.lower()
                and "preference" not in col.lower()
                and "metro" not in col.lower()
            )
        if city_col:
            city_value = matched_user.get(city_col, "")
            logger.info(f"DEBUG: Raw city value from column '{city_col}': '{city_value}'")
            if pd.notna(city_value) and str(city_value).strip():
                # Clean up the city value
                city_value = str(city_value).strip()
                logger.info(f"DEBUG: After strip city value: '{city_value}'")
                # Remove any prefixes like "City:" or "Prefer"
                city_value = re.sub(
                    r"^(City:|Prefer)\s*", "", city_value, flags=re.IGNORECASE
                )
                logger.info(f"DEBUG: After regex cleanup city value: '{city_value}'")
                # Don't truncate city names - use the full cleaned value
                if city_value:
                    current_y = add_compact_field(pdf, "City", city_value, current_y)
                    logger.info(f"DEBUG: Added city field to PDF: '{city_value}'")
        else:
            logger.warning(
                f"DEBUG: No city column found in data. Available columns: {list(matched_user.keys())}"
            )
        # Handle other location fields
        location_fields = [("State", "State"), ("Country", "Country")]
        for display_name, field_name in location_fields:
            matching_field = next(
                (
                    col
                    for col in matched_user.keys()
                    if field_name.lower() in col.lower()
                ),
                None,
            )
            if matching_field:
                value = matched_user.get(matching_field, "N/A")
                if pd.notna(value) and str(value).strip():
                    value = str(value).strip()
                    current_y = add_compact_field(
                        pdf,
                        display_name,
                        value,
                        current_y,
                    )
        # Contact Information (Email only)
        current_y += 5  # Increased spacing between Current Location and Contact Details
        current_y = add_compact_section(pdf, "Contact Details", current_y)
        # Add email field with improved indentation
        current_y = add_compact_field(
            pdf, "Email", matched_user.get(email_col, "N/A"), current_y, label_width=50
        )
        # Save the PDF
        matched_user_name = matched_user.get("Full Name", "Unknown").replace(" ", "_")
        output_filename = f"{matched_user_name}.pdf"
        pdf.output(output_filename)
        logger.info(f"Single-page PDF created: {output_filename}")
        return output_filename
    except Exception as e:
        logger.error(f"Single-page PDF creation failed: {e}", exc_info=True)
        return None
def create_sorted_pdfs_and_email(df):
    """
    Main function to process matches, create PDFs, and email in correct sequence.
    This ensures PDFs and email follow the same sorting order.
    """
    try:
        # Process matrimonial data
        result = process_matrimonial_data(df)
        if not result:
            logger.error("Failed to process matrimonial data")
            return None
            
        (new_user, new_user_name, new_user_email, new_user_whatsapp, 
         new_user_birth_date, new_user_location, top_matches_df, 
         top_percentages, final_matches_df) = result
        
        # Find email column
        possible_email_cols = [col for col in df.columns if "email" in col.lower()]
        if not possible_email_cols:
            raise ValueError("No column containing 'email' found.")
        email_col = possible_email_cols[0]
        
        # Calculate overall compatibility scores for proper sorting
        top_matches_df = top_matches_df.copy()
        top_matches_df['Overall_Score'] = (
            top_matches_df.get('PPF %', 0) + 
            top_matches_df.get('FavLikes %', 0) + 
            top_matches_df.get('Others %', 0)
        ) / 3
        
        # Sort by Overall_Score in DESCENDING order (highest percentage first)
        sorted_matches = top_matches_df.sort_values(by='Overall_Score', ascending=False).reset_index(drop=True)
        
        # Create PDFs in the correct sequence (same as email)
        pdf_files = []
        valid_matches_for_email = pd.DataFrame()
        
        profile_counter = 1
        for _, match_row in sorted_matches.iterrows():
            match_name = match_row.get('Full Name', 'Unknown')
            overall_score = match_row.get('Overall_Score', 0)
            
            # Skip "No Match Found" entries and zero scores
            if match_name != "No Match Found" and overall_score > 0:
                # Create PDF with sequential profile number
                pdf_file = create_single_page_match_pdf(
                    match_row, 
                    overall_score, 
                    new_user_name, 
                    email_col,
                    profile_counter
                )
                
                if pdf_file:
                    pdf_files.append(pdf_file)
                    
                # Add to valid matches for email (maintains the sorted order)
                if valid_matches_for_email.empty:
                    valid_matches_for_email = pd.DataFrame([match_row])
                else:
                    valid_matches_for_email = pd.concat([valid_matches_for_email, pd.DataFrame([match_row])], ignore_index=True)
                
                profile_counter += 1
                
                # Only process top 3 matches
                if profile_counter > 3:
                    break
        
        # Create email message using the same sorted order
        email_html = create_email_message(new_user_name, valid_matches_for_email)
        
        logger.info(f"Created {len(pdf_files)} PDFs in sorted order (highest compatibility first)")
        logger.info("Email message created with same sorting order")
        
        return {
            'pdf_files': pdf_files,
            'email_html': email_html,
            'new_user_email': new_user_email,
            'sorted_matches': valid_matches_for_email
        }
        
    except Exception as e:
        logger.error(f"Error in create_sorted_pdfs_and_email: {e}", exc_info=True)
        return None
def create_individual_match_pdfs(
    matched_users,
    match_percentages,
    new_user_name,
    email_col,
):
    """Create individual single-page PDFs for each matched user (up to 3)"""
    pdf_files = []
    if matched_users is None or len(matched_users) == 0:
        logger.warning("No matched users to create PDFs for")
        return pdf_files
    # Create individual PDFs for each match (up to 3)
    for i, (idx, user) in enumerate(matched_users.iterrows()):
        if i >= 3:  # Limit to 3 profiles
            break
        profile_number = i + 1
        match_percent = match_percentages[i]
        pdf_filename = create_single_page_match_pdf(
            user, match_percent, new_user_name, email_col, profile_number
        )
        if pdf_filename:
            pdf_files.append(pdf_filename)
            logger.info(f"Created single-page PDF {profile_number}: {pdf_filename}")
        else:
            logger.error(f"Failed to create PDF for profile {profile_number}")
    return pdf_files
# Function to send email with multiple PDF attachments
def send_email_with_multiple_pdfs(recipient_email, message, pdf_files, user_name, 
                                whatsapp_number, email_address, birth_date, location, 
                                user_profile_url=None, top_match_urls=None, email_text=None):
    """Send email with multiple PDF attachments and write to target sheet"""
    try:
        # Check if email functionality is enabled
        if not ENABLE_EMAIL:    
            logger.warning("Email functionality is disabled. Skipping email sending.")
            return False
        
        # Check if required email configuration is available
        if not SENDER_EMAIL or not SENDER_PASSWORD:
            logger.error("Email configuration not properly set. Please configure SENDER_EMAIL and SENDER_PASSWORD.")
            return False
        
        logger.info(f"Preparing to send email to {recipient_email} with {len(pdf_files)} PDF attachments")
        
        # Create the container email message
        msg = MIMEMultipart('mixed')
        # Updated subject line for clarity
        msg["Subject"] = f"Admin Copy: Sapta.ai Persona Matches for {user_name}"
        msg["From"] = SENDER_EMAIL
        msg["To"] = recipient_email
        
        # Create the HTML body with embedded image
        related = MIMEMultipart('related')
        html_part = MIMEText(message, 'html')
        related.attach(html_part)
        
        # Embed the logo image
        try:
            if os.path.exists('logo.png'):
                with open('logo.png', 'rb') as f:
                    img = MIMEImage(f.read())
                    img.add_header('Content-ID', '<logoimage>')
                    related.attach(img)
                    logger.info("Logo image successfully embedded in email")
            else:
                logger.warning("logo.png not found, email will be sent without logo.")
        except Exception as e:
            logger.warning(f"Failed to embed logo in email: {e}, continuing without logo")
        
        msg.attach(related)
        
        # Attach all PDF files
        logger.info(f"Attaching {len(pdf_files)} PDF files to email...")
        attached_count = 0
        for pdf_path in pdf_files:
            if not os.path.exists(pdf_path):
                logger.warning(f"PDF file not found: {pdf_path}")
                continue
            try:
                with open(pdf_path, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename= {os.path.basename(pdf_path)}",
                )
                msg.attach(part)
                attached_count += 1
                logger.info(f"Successfully attached PDF: {os.path.basename(pdf_path)}")
            except Exception as e:
                logger.error(f"Failed to attach PDF {pdf_path}: {e}")
        
        logger.info(f"Total PDFs attached to email: {attached_count}")
        
        # Send the email
        try:
            logger.info(f"Attempting to send email to {recipient_email} from {SENDER_EMAIL}")
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                logger.info("SMTP connection established, attempting login...")
                server.login(SENDER_EMAIL, SENDER_PASSWORD)
                logger.info("SMTP login successful, sending message...")
                server.send_message(msg)
                logger.info(f"Email sent successfully to {recipient_email}")
                
                # After successful email sending, write to target sheet
                logger.info("Writing user data to target sheet...")
                
                # Debug the parameters before calling the function
                logger.info("Parameters for target sheet:")
                logger.info(f"user_name: {user_name}")
                logger.info(f"whatsapp_number: {whatsapp_number}")  
                logger.info(f"email_address: {email_address}")
                logger.info(f"birth_date: {birth_date}")
                logger.info(f"location: {location}")
                logger.info(f"user_profile_url: {user_profile_url}")
                logger.info(f"top_match_urls: {top_match_urls}")
                logger.info(f"email_text type: {type(email_text)}, length: {len(email_text) if email_text else 0}")
                
                # Call the target sheet function
                sheet_write_success = write_name_to_target_sheet(
                    user_name=user_name,
                    whatsapp_number=whatsapp_number,
                    email_address=email_address,
                    birth_date=birth_date,
                    location=location,
                    pdf_url=user_profile_url,
                    top_match_urls=top_match_urls,
                    email_text=email_text
                )
                
                if sheet_write_success:
                    logger.info("Successfully wrote user data to target sheet")
                else:
                    logger.error("Failed to write user data to target sheet")
                
                return True
                
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP Authentication failed: {e}")
            logger.error("Please check your Gmail App Password")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error sending email: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending email: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            return False
        
    except Exception as e:
        logger.error(f"Error in send_email_with_multiple_pdfs: {str(e)}", exc_info=True)
        return False
def cleanup_pdf_files(pdf_files):
    """Clean up temporary PDF files after sending email"""
    for pdf_file in pdf_files:
        try:
            if os.path.exists(pdf_file):
                os.remove(pdf_file)
                logger.info(f"Cleaned up PDF file: {pdf_file}")
        except Exception as e:
            logger.error(f"Failed to remove PDF file {pdf_file}: {e}")
def send_admin_notification(
    user, matches_sent=True, match_lines="No matches", pdf_count=0, pdf_files=None
):
    # Check if email functionality is enabled
    if not ENABLE_EMAIL:
        logger.info("Email functionality is disabled. Skipping admin notification.")
        return True
    
    # Check if required email configuration is available
    if SENDER_EMAIL == "yourname@gmail.com" or SENDER_PASSWORD == "your_app_password_here" or ADMIN_EMAIL == "admin@yourcompany.com":
        logger.warning("Email configuration not properly set. Please configure SENDER_EMAIL, SENDER_PASSWORD, and ADMIN_EMAIL in environment variables or modify the default values in the code.")
        logger.info("Skipping admin notification due to missing email configuration.")
        return False
    
    if pdf_files is None:
        pdf_files = []
    
    # Try to get user details
    name = (
        user.get("Full Name", "N/A")
        if isinstance(user, dict) or hasattr(user, "get")
        else "N/A"
    )
    
    # Find email column
    email_value = "N/A"
    if isinstance(user, dict) or hasattr(user, "get"):
        possible_email_keys = [k for k in user.keys() if "email" in k.lower()]
        if possible_email_keys:
            email_value = user.get(possible_email_keys[0], "N/A")
    
    # Find gender
    gender_value = "N/A"
    if isinstance(user, dict) or hasattr(user, "get"):
        possible_gender_keys = [k for k in user.keys() if "gender" in k.lower()]
        if possible_gender_keys:
            gender_value = user.get(possible_gender_keys[0], "N/A")
    
    # Check if PDF files exist
    valid_pdf_files = []
    for pdf_path in pdf_files:
        if os.path.exists(pdf_path):
            valid_pdf_files.append(pdf_path)
        else:
            logger.warning(f"PDF file not found: {pdf_path}")
    
    if not valid_pdf_files:
        logger.error("No valid PDF files found to attach")
        return False
    
    subject = (
        f"Admin Copy - Match Email for {name}" if matches_sent else "Match Email Failed"
    )
    body = f"""
[ADMIN COPY] This is a copy of the email sent to the user.
User Details:
Name: {name}
Email: {email_value}
Gender: {gender_value}
PDFs Generated: {pdf_count}
Status: {"Successfully sent" if matches_sent else "Failed to send"}
Top matches:
{match_lines}
---
Original email content sent to user is attached below along with match PDFs.
"""
    
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = ADMIN_EMAIL
    msg.set_content(body)
    
    # Attach all PDF files
    for i, pdf_path in enumerate(valid_pdf_files, 1):
        try:
            with open(pdf_path, "rb") as f:
                file_data = f.read()
                filename = f"Profile_{i}_Match.pdf"
                msg.add_attachment(
                    file_data,
                    maintype="application",
                    subtype="pdf",
                    filename=filename,
                )
            logger.info(f"Attached PDF: {pdf_path} as {filename}")
        except Exception as e:
            logger.error(f"Failed to attach PDF {pdf_path}: {e}")
            continue
    
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
            logger.info(f"Admin notified at {ADMIN_EMAIL}")
            return True
    except Exception as e:
        logger.error(f"Failed to send admin notification: {e}")
        return False
def send_admin_copy_of_user_email(user_name, user_email, email_message, pdf_files):
    """Send admin a copy of the HTML email with embedded logo and attachments."""
    from datetime import datetime
    
    # Check if email functionality is enabled
    if not ENABLE_EMAIL:
        logger.warning("Admin email copy skipped: email functionality is disabled")
        return False
    
    # Use the constants defined at the top of the file instead of os.getenv
    admin_email = ADMIN_EMAIL
    sender_email = SENDER_EMAIL
    sender_password = SENDER_PASSWORD

    if not admin_email or not sender_email or not sender_password:
        logger.warning(f"Admin email copy skipped: missing email credentials")
        logger.warning(f"ADMIN_EMAIL: {admin_email}")
        logger.warning(f"SENDER_EMAIL: {sender_email}")
        logger.warning(f"SENDER_PASSWORD: {'*' * len(sender_password) if sender_password else 'None'}")
        return False

    # Create the container email message.
    msg = MIMEMultipart('mixed')
    msg["Subject"] = f"Sapta.ai Persona Matches for {user_name}"
    msg["From"] = sender_email
    msg["To"] = admin_email
    
    logger.info(f"Created admin email message: Subject='{msg['Subject']}', From='{msg['From']}', To='{msg['To']}'")

    # --- Create the HTML Body for the Admin ---
    admin_header = f"""
    <html><body>
    <p>[ADMIN NOTIFICATION]<br>
    This is a copy of the email sent to: {user_name} ({user_email})<br>
    Sent on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    <hr>
    """
    # Combine admin header with the original user message HTML
    full_admin_html = admin_header + email_message

    # Create the body with embedded image
    related = MIMEMultipart('related')
    html_part = MIMEText(full_admin_html, 'html')
    related.attach(html_part)

    # Embed the logo image
    try:
        if os.path.exists('logo.png'):
            with open('logo.png', 'rb') as f:
                img = MIMEImage(f.read())
                img.add_header('Content-ID', '<logoimage>')
                related.attach(img)
                logger.info("Logo image successfully embedded in admin email")
        else:
            logger.warning("logo.png not found, admin email will be sent without logo.")
    except Exception as e:
        logger.warning(f"Failed to embed logo in admin email: {e}, continuing without logo")

    msg.attach(related)

    # Attach all PDF files (same as sent to user)
    logger.info(f"Attaching {len(pdf_files)} PDF files to admin email...")
    attached_count = 0
    for pdf_path in pdf_files:
        if not os.path.exists(pdf_path):
            logger.warning(f"PDF file not found for admin email: {pdf_path}")
            continue
        try:
            with open(pdf_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {os.path.basename(pdf_path)}",
            )
            msg.attach(part)
            attached_count += 1
            logger.info(f"Successfully attached PDF to admin email: {os.path.basename(pdf_path)}")
        except Exception as e:
            logger.error(f"Failed to attach PDF to admin email {pdf_path}: {e}")
    
    logger.info(f"Total PDFs attached to admin email: {attached_count}")

    # Send the email
    try:
        logger.info(f"Attempting to send admin copy to {admin_email} from {sender_email}")
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            logger.info("SMTP connection established, attempting login...")
            server.login(sender_email, sender_password)
            logger.info("SMTP login successful, sending message...")
            server.send_message(msg)
            logger.info(f"Admin copy sent successfully to {admin_email} for user {user_name}")
            return True
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP Authentication failed for admin copy: {e}")
        logger.error("Please check your Gmail App Password")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending admin copy: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending admin copy: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        return False
def create_email_message(new_user_name, top_matches):
    """Create an HTML email message with an embedded logo and detailed match breakdowns."""
    # Ensure the user's name is in title case
    name_title_case = str(new_user_name).title() if new_user_name else "User"

    # Start of the HTML message. The <img src="cid:logoimage"> is a special reference to the image we will attach.
    message_html = f"""
    <html>
      <body>
        <div style="text-align: center;">
          <img src="cid:logoimage" alt="Logo" width="150">
        </div>
        <p>Dear {name_title_case},</p>
        <p>Congratulations on creating your Sapta.ai Digital Persona.</p>
        <p>Here are your closest matches with Compatibility scores:</p>
    """
    
    if isinstance(top_matches, pd.DataFrame):
        # Calculate overall compatibility score for proper sorting
        top_matches_copy = top_matches.copy()
        top_matches_copy['Overall_Score'] = (
            top_matches_copy.get('PPF %', 0) + 
            top_matches_copy.get('FavLikes %', 0) + 
            top_matches_copy.get('Others %', 0)
        ) / 3
        
        # Sort by Overall_Score in DESCENDING order (highest percentage first)
        sorted_matches = top_matches_copy.sort_values(by='Overall_Score', ascending=False).reset_index(drop=True)
        
        match_counter = 1
        for _, row in sorted_matches.iterrows():
            match_name = row.get('Full Name', 'Unknown')
            ppf_score = row.get('PPF %', 0)
            fav_likes_score = row.get('FavLikes %', 0)
            others_score = row.get('Others %', 0)
            overall_score = row.get('Overall_Score', 0)
            
            # Skip "No Match Found" entries
            if match_name == "No Match Found" or overall_score == 0:
                continue

            # Append each match's details as HTML
            message_html += f"""
            <p>
              <b>{match_counter}. {match_name} - Overall Compatibility Score : {overall_score:.1f}%</b><br>
              &nbsp;&nbsp;&nbsp;Compatibility Breakdown:<br>
              &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Personal, Professional & Family Details: {ppf_score:.1f}%<br>
              &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Favorites, Likes & Hobbies: {fav_likes_score:.1f}%<br>
              &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Other Requirements and Preferences: {others_score:.1f}%<br>
            </p>
            """
            match_counter += 1
            
    message_html += """
        <p>The Sapta.ai Digital Persona of everyone is attached with this email for your reference.</p>
        <p>Best Wishes<br><br>Team Sapta.ai</p>
      </body>
    </html>
    """
    return message_html
def log_match_results(new_user_name, new_user_email, top_matches):
    """Log the matching results for record keeping"""
    logger.info(f"=== MATCH RESULTS FOR {new_user_name} ({new_user_email}) ===")
    logger.info(f"Total matches found: {len(top_matches)}")
    for i, (_, match) in enumerate(top_matches.iterrows(), 1):
        name = match.get("Name", "Unknown")
        email = match.get("Email", "Unknown")
        match_percent = match.get("Match %", 0)
        logger.info(f"Match {i}: {name} ({email}) - {match_percent:.1f}% overall compatibility")
    logger.info("=" * 50)
def handle_errors_gracefully(func):
    """Decorator to handle errors gracefully"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            return None
    return wrapper
@handle_errors_gracefully
def process_new_matrimonial_registration():
    """Main function to process a new matrimonial registration"""
    try:
        # Step 1: Fetch data from Google Sheets
        logger.info("Starting matrimonial matching process...")
        df = fetch_data_from_google_sheets()
        if df is None or df.empty:
            logger.error("No data retrieved from Google Sheets")
            return False
        logger.info(f"Retrieved {len(df)} records from Google Sheets")
        logger.info(f"Columns in dataset: {df.columns.tolist()}")
        
        # Step 2: Process the data and find matches
        logger.info("Processing matrimonial data...")
        result = process_matrimonial_data(df)
        if not result or len(result) < 6:
            logger.error("Failed to process matrimonial data or insufficient results")
            return False
        (
            new_user,
            new_user_name,
            new_user_email,
            new_user_whatsapp,
            new_user_birth_date,
            new_user_location,
            top_matches_df,
            top_percentages,
            top_matches_df
        ) = result
        logger.info(f"Found matches for user: {new_user_name} ({new_user_email})")
        logger.info(f"Number of matches found: {len(top_matches_df)}")

        if top_matches_df is None or len(top_matches_df) == 0:
            logger.warning(f"No matches found for {new_user_name}")
            return True
            
        # Step 3: Log the match results
        log_match_results(new_user_name, new_user_email, top_matches_df)
        
        # Step 4: Find email column
        possible_email_cols = [col for col in df.columns if "email" in col.lower()]
        email_col = possible_email_cols[0] if possible_email_cols else "Email"
        logger.info(f"Using email column: {email_col}")
        
        # Step 5: Create last response PDF first
        logger.info("Creating last response PDF...")
        last_response_pdf = create_last_response_pdf(new_user, new_user_name,email_col)
        if not last_response_pdf:
            logger.error("Failed to create last response PDF")
            return False
        logger.info("Successfully created last response PDF")

        # Step 6: Create individual PDFs for each match
        logger.info("Creating individual PDF profiles...")
        match_pdfs = create_individual_match_pdfs(
            top_matches_df, top_percentages, new_user_name, email_col
        )
        if not match_pdfs:
            logger.error("Failed to create any PDF files")
            # Still have the user's PDF, so continue
            pdf_files = [last_response_pdf]
        else:
            pdf_files = match_pdfs + [last_response_pdf]
        
        logger.info(f"Successfully created {len(pdf_files)} PDF files (including last response)")
        
        # **NEW**: Step 6a - Upload PDFs to Drive and get URLs
        logger.info("Uploading PDFs to Google Drive to get URLs...")
        user_profile_url = upload_pdf_to_drive_and_get_url(last_response_pdf, new_user_name)
        top_match_urls = upload_multiple_pdfs_to_drive_and_get_urls(match_pdfs, new_user_name)

        # Step 7: Create personalized email message
        logger.info("Creating email message...")
        email_message = create_email_message(new_user_name, top_matches_df)
        
        if email_message:
            logger.info("Email message created successfully")
            logger.debug(f"Email message preview (first 200 chars): {str(email_message)[:200]}...")
        else:
            logger.error("Email message is empty or None")
            email_message = "Email content could not be generated"
        
        # Step 7a: Create compatibility text directly from match data (IMPROVED APPROACH)
        logger.info("Creating compatibility text directly from match data...")
        email_text = create_compatibility_text_directly(top_matches_df,new_user_name)
        
        if email_text and email_text != "No matches found":
            logger.info(f"Successfully created compatibility text ({len(email_text)} characters)")
            logger.debug(f"Compatibility text preview: {email_text[:200]}...")
        else:
            logger.warning("Direct compatibility text creation failed, trying extraction from email")
            # Fallback to extraction method
            email_text = extract_compatibility_text_from_email(email_message)
            if not email_text or email_text.startswith("Error"):
                logger.warning("Email text extraction also failed, using truncated email message")
                email_text = str(email_message)[:1000] if email_message else "No email content available"

        # Step 8: Send email with PDF attachments to admin only
        logger.info(f"Sending email to admin only at {ADMIN_EMAIL}...")
        
        # Ensure all parameters are properly passed
        email_sent = send_email_with_multiple_pdfs(
            ADMIN_EMAIL, 
            email_message, 
            pdf_files, 
            new_user_name, 
            new_user_whatsapp, 
            new_user_email, 
            new_user_birth_date, 
            new_user_location,
            user_profile_url,  # user_profile_url
            top_match_urls,  # top_match_urls
            email_text  # email_text
        )
        
        if email_sent:
            logger.info(
                f"Successfully sent email with {len(pdf_files)} PDF attachments to admin only at {ADMIN_EMAIL}"
            )
            # The original logic for sending an admin copy is now redundant, as the main email is sent to the admin.
            # The subsequent send_admin_last_response_and_matches is also redundant as the main email now contains all the PDFs.
        else:
            logger.error(f"Failed to send email to admin at {ADMIN_EMAIL}")
        
        # Step 9: Clean up temporary files
        logger.info("Cleaning up temporary PDF files...")
        cleanup_pdf_files(pdf_files)
        return email_sent
        
    except Exception as e:
        logger.error(
            f"Critical error in matrimonial processing: {str(e)}", exc_info=True
        )
        return False
def upload_pdf_to_drive_and_get_url(pdf_filename, user_name):
    """Upload PDF to Google Drive using OAuth and return a shareable URL"""
    try:
        if not os.path.exists(pdf_filename):
            logger.error(f"PDF file not found: {pdf_filename}")
            return None
        logger.info(f"Uploading PDF '{pdf_filename}' to Google Drive for user '{user_name}' (OAuth)")
        credentials = get_oauth_drive_creds()
        service = build('drive', 'v3', credentials=credentials)
        file_metadata = {'name': os.path.basename(pdf_filename)}
        from googleapiclient.http import MediaFileUpload
        media = MediaFileUpload(pdf_filename, mimetype='application/pdf')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        file_id = file.get('id')
        service.permissions().create(
            fileId=file_id,
            body={'type': 'anyone', 'role': 'reader'}
        ).execute()
        shareable_url = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
        logger.info(f"Created shareable URL for PDF (OAuth): {shareable_url}")
        return shareable_url
    except Exception as e:
        logger.error(f"Error uploading PDF to Drive (OAuth): {str(e)}", exc_info=True)
        return None
def create_local_file_backup(pdf_files, user_name):
    """Create a local backup file with PDF information when Google Drive upload fails"""
    try:
        backup_filename = f"backup_{user_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(backup_filename, 'w') as f:
            f.write(f"PDF Backup for User: {user_name}\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 50 + "\n\n")
            for i, pdf_file in enumerate(pdf_files, 1):
                if os.path.exists(pdf_file):
                    f.write(f"PDF {i}: {os.path.abspath(pdf_file)}\n")
                    f.write(f"Size: {os.path.getsize(pdf_file)} bytes\n")
                    f.write(f"Created: {datetime.fromtimestamp(os.path.getctime(pdf_file))}\n")
                    f.write("-" * 30 + "\n")
        logger.info(f"Created local backup file: {backup_filename}")
        return backup_filename
    except Exception as e:
        logger.error(f"Failed to create backup file: {e}")
        return None
def upload_multiple_pdfs_to_drive_and_get_urls(pdf_files, user_name):
    """Upload multiple PDFs to Google Drive using OAuth and return their shareable URLs"""
    try:
        if not pdf_files:
            logger.warning("No PDF files provided for upload")
            return []
        credentials = get_oauth_drive_creds()
        service = build('drive', 'v3', credentials=credentials)
        from googleapiclient.http import MediaFileUpload
        urls = []
        for i, pdf_filename in enumerate(pdf_files, 1):
            try:
                if not os.path.exists(pdf_filename):
                    logger.warning(f"PDF file not found: {pdf_filename}")
                    urls.append("")
                    continue
                file_metadata = {'name': os.path.basename(pdf_filename)}
                media = MediaFileUpload(pdf_filename, mimetype='application/pdf')
                file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                file_id = file.get('id')
                service.permissions().create(
                    fileId=file_id,
                    body={'type': 'anyone', 'role': 'reader'}
                ).execute()
                shareable_url = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
                urls.append(shareable_url)
                logger.info(f"Created shareable URL for PDF {i} (OAuth): {shareable_url}")
            except Exception as upload_error:
                logger.error(f"Failed to upload PDF {i} to Drive (OAuth): {upload_error}")
                urls.append("")
        return urls
    except Exception as e:
        logger.error(f"Error in upload_multiple_pdfs_to_drive_and_get_urls (OAuth): {str(e)}", exc_info=True)
        return []
def add_formal_full_length_photo_to_pdf(pdf, user_data, email_col):
    """Add formal full-length photo to the bottom right of the first page"""
    
    # Handle both DataFrame and dictionary input
    if hasattr(user_data, 'columns'):  # DataFrame
        columns_list = user_data.columns
        def get_value(key, default=""):
            return user_data[key].values[0] if key in user_data.columns else default
    else:  # Dictionary
        columns_list = user_data.keys()
        def get_value(key, default=""):
            return user_data.get(key, default)
    
    # Find formal full-length photo column
    formal_photo_col = next(
        (col for col in columns_list if "formal" in col.lower() and "full" in col.lower()),
        None,
    )
    
    if not formal_photo_col:
        logger.warning("No formal full-length photo column found")
        return False
    
    photo_link = get_value(formal_photo_col, "")
    
    if (
        not isinstance(photo_link, str)
        or not photo_link.strip()
        or "http" not in photo_link.lower()
        or photo_link.strip().lower() in ["", "no", "n/a"]
    ):
        logger.warning("No valid formal full-length photo link found")
        return False
    
    # Create safe filename
    email = get_value(email_col, "unknown")
    safe_name = re.sub(r"[^\w\-_]", "_", email)
    photo_path = f"temp_{safe_name}_formal_full.jpg"
    
    # Try to download the image
    img_path = download_drive_image(photo_link, save_filename=photo_path)
    
    if not img_path or not os.path.exists(img_path):
        logger.warning("Failed to download formal full-length image")
        return False
    
    # Add image to PDF
    try:
        with Image.open(img_path) as img:
            # Fixed dimensions for formal full-length photo
            photo_width = PHOTO_WIDTH  # Width in mm
            photo_height = PHOTO_HEIGHT  # Height in mm
            
            # Position at bottom right of first page
            photo_x = pdf.w - photo_width - 15  # 15mm from right edge
            photo_y = pdf.h - photo_height - 103  # 20mm from bottom
            
            # Add decorative border
            border_margin = 2
            pdf.set_draw_color(*pdf.primary_color)
            pdf.set_line_width(1)
            pdf.rect(
                photo_x - border_margin,
                photo_y - border_margin,
                photo_width + 2 * border_margin,
                photo_height + 2 * border_margin,
            )
            
            # Add photo
            pdf.image(
                img_path,
                x=photo_x,
                y=photo_y,
                w=photo_width,
                h=photo_height,
            )
            logger.info("Formal full-length photo added to PDF successfully")
            return True
    except Exception as e:
        logger.error(f"Error adding formal full-length image to PDF: {e}")
        return False
    finally:
        # Clean up the temp file
        if os.path.exists(img_path):
            try:
                os.remove(img_path)
            except Exception as e:
                logger.error(f"Error removing temp formal image: {e}")
    return False
def add_candid_photo_to_second_page(pdf, user_data, email_col):
    """Add candid photo to the center-right of the second page"""
    
    # Handle both DataFrame and dictionary input
    if hasattr(user_data, 'columns'):  # DataFrame
        columns_list = user_data.columns
        def get_value(key, default=""):
            return user_data[key].values[0] if key in user_data.columns else default
    else:  # Dictionary
        columns_list = user_data.keys()
        def get_value(key, default=""):
            return user_data.get(key, default)
    
    # Find candid photo column
    candid_photo_col = next(
        (col for col in columns_list if "candid" in col.lower()),
        None,
    )
    
    if not candid_photo_col:
        logger.warning("No candid photo column found")
        return False
    
    photo_link = get_value(candid_photo_col, "")
    
    if (
        not isinstance(photo_link, str)
        or not photo_link.strip()
        or "http" not in photo_link.lower()
        or photo_link.strip().lower() in ["", "no", "n/a"]
    ):
        logger.warning("No valid candid photo link found")
        return False
    
    # Create safe filename
    email = get_value(email_col, "unknown")
    safe_name = re.sub(r"[^\w\-_]", "_", email)
    photo_path = f"temp_{safe_name}_candid.jpg"
    
    # Try to download the image
    img_path = download_drive_image(photo_link, save_filename=photo_path)
    
    if not img_path or not os.path.exists(img_path):
        logger.warning("Failed to download candid image")
        return False
    
    # Add image to PDF
    try:
        with Image.open(img_path) as img:
            # Fixed dimensions for candid photo
            photo_width = PHOTO_WIDTH  # Width in mm
            photo_height = PHOTO_HEIGHT  # Height in mm
            
            # Position at center-right of second page, moved upward
            photo_x = pdf.w - photo_width - 15  # 15mm from right edge
            # Adjusted positioning: moved up from center by reducing the Y position
            photo_y = (pdf.h - photo_height) / 2 - 25  # Moved 30mm upward from center
            
            # Ensure photo doesn't go above the header area
            min_y = 70  # Minimum Y position to avoid header overlap
            if photo_y < min_y:
                photo_y = min_y
            
            # Add decorative border
            border_margin = 2
            pdf.set_draw_color(*pdf.primary_color)
            pdf.set_line_width(1)
            pdf.rect(
                photo_x - border_margin,
                photo_y - border_margin,
                photo_width + 2 * border_margin,
                photo_height + 2 * border_margin,
            )
            
            # Add photo
            pdf.image(
                img_path,
                x=photo_x,
                y=photo_y,
                w=photo_width,
                h=photo_height,
            )
            logger.info("Candid photo added to second page successfully")
            return True
    except Exception as e:
        logger.error(f"Error adding candid image to PDF: {e}")
        return False
    finally:
        # Clean up the temp file
        if os.path.exists(img_path):
            try:
                os.remove(img_path)
            except Exception as e:
                logger.error(f"Error removing temp candid image: {e}")
    return False
def send_admin_last_response_and_matches(new_user, new_user_name, new_user_email, pdf_files):
    """Send last response and matches to admin"""
    
    # Check if email functionality is enabled
    if not ENABLE_EMAIL:
        logger.info("Email functionality is disabled. Skipping admin notification.")
        return True
    
    # Check if required email configuration is available
    if SENDER_EMAIL == "yourname@gmail.com" or SENDER_PASSWORD == "fyour_app_password_here" or ADMIN_EMAIL == "admin@yourcompany.com":
        logger.warning("Email configuration not properly set. Please configure SENDER_EMAIL, SENDER_PASSWORD, and ADMIN_EMAIL in environment variables or modify the default values in the code.")
        logger.info("Skipping admin notification due to missing email configuration.")
        return False
    
    try:
        # Create email message
        subject = f"New Matrimonial Registration: {new_user_name}"
        body = f"""
        New matrimonial registration received from {new_user_name} ({new_user_email}).

        Attached files:
        1. Last Response Profile (includes Requirements & Preferences)
        2. Top 5 Match Profiles

        Please review the registration and matches.
        """
        
        # Create message
        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = ADMIN_EMAIL
        msg["Subject"] = subject
        
        # Add body
        msg.attach(MIMEText(body, "plain"))
        
        # Attach PDFs
        for pdf_file in pdf_files:
            if os.path.exists(pdf_file):
                with open(pdf_file, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f'attachment; filename="{os.path.basename(pdf_file)}"',
                    )
                    msg.attach(part)
            else:
                logger.warning(f"PDF file not found: {pdf_file}")
        
        # Send email
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        logger.info(f"Successfully sent last response and matches to admin: {ADMIN_EMAIL}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending admin notification: {str(e)}", exc_info=True)
        return False

def extract_drive_id(link):
    if not link or not isinstance(link, str) or "drive.google.com" not in link:
        return None
    try:
        patterns = [
            r"/file/d/([a-zA-Z0-9_-]+)",  # Standard sharing link
            r"[?&]id=([a-zA-Z0-9_-]+)",  # Query parameter format
            r"/document/d/([a-zA-Z0-9_-]+)",  # Google Docs format
            r"drive\.google\.com/([a-zA-Z0-9_-]{25,})",  # Direct ID in URL
            r"([a-zA-Z0-9_-]{25,})",  # Last resort - any long alphanumeric string
        ]
        for pattern in patterns:
            match = re.search(pattern, link)
            if match:
                file_id = match.group(1)
                # Validate that it looks like a proper Google Drive file ID
                if len(file_id) >= 25:  # Google Drive IDs are typically 28+ characters
                    return file_id
    except Exception as e:
        logger.error(f"Error extracting Drive ID: {e}")
    return None

if __name__ == "__main__":
    try:
        # Test admin email first
        logger.info("=" * 50)
        logger.info("TESTING ADMIN EMAIL FUNCTIONALITY")
        logger.info("=" * 50)
        
        logger.info("=" * 50)
        logger.info("RUNNING MAIN MATRIMONIAL PROCESS")
        logger.info("=" * 50)
        
        # Add a small delay to prevent rapid processing
        import time
        time.sleep(2)
        
        # Run the main process with better error handling
        result = process_new_matrimonial_registration()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error in main process: {str(e)}", exc_info=True)

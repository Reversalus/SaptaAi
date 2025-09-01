Sapta.ai Matrimonial Profile Matching System
This project is a Python-based backend system designed to automate matrimonial profile matching, PDF generation, and email communication. It is built to integrate with a Google Form for new user registrations, a Google Sheet for data storage, and Google Drive for PDF management.

The system features:

Automated Matching: A custom algorithm to match new users with existing profiles based on a set of criteria.

Dynamic PDF Generation: Creates a "Digital Persona" PDF profile for each user and their top matches.

Email Notifications: Sends a personalized email to the new user with their profile and the matched profiles attached as PDFs.

Google Sheets Integration: Fetches and updates data directly from a Google Sheet.

Google Drive Integration: Uploads generated PDF profiles to Google Drive for secure storage and shareable URLs.

Webhook Server: A lightweight Flask server to listen for new form submissions and trigger the matching process in real-time.

Project Structure
app.py: The core application logic. This script contains the functions for data processing, the matching algorithm, PDF creation, and email sending. It is designed to be called by the webhook_server.py.

webhook_server.py: A Flask application that acts as the entry point for the system. It listens for webhooks from Google Forms, manages the processing status, and triggers the app.py script to run the main logic asynchronously.

service_account2.json: A Google Cloud service account key file used for authentication to Google Sheets and Google Drive. This file is critical and must be kept secure.

.env: An environment variable file to store sensitive information like email credentials and webhook secrets, preventing them from being hard-coded in the script.

Setup and Installation
Clone the Repository

git clone [your-repository-url]
cd [your-project-directory]

Install Dependencies
You need to install all the required Python packages. It is recommended to use a virtual environment.

pip install -r requirements.txt

(Note: You'll need to create a requirements.txt file based on the imports in your scripts.)

Google Cloud Setup

Go to the Google Cloud Console.

Create a new project.

Enable the Google Sheets API and Google Drive API for your project.

Create a service account and generate a JSON key file. Rename this file to service_account2.json and place it in the project directory.

Share your Google Sheets spreadsheet and Google Drive folder with the email address of the service account.

Google Account Setup

The system uses Gmail to send emails. You need to enable 2-Step Verification and create an App Password for the sender email address.

Do not use your regular Gmail password.

Environment Configuration
Create a .env file in the project's root directory and add the following details, replacing the placeholder values with your own.

SENDER_EMAIL="your-email@gmail.com"
SENDER_PASSWORD="your_16_digit_app_password"
ADMIN_EMAIL="admin-email@yourcompany.com"
WEBHOOK_SECRET="a_strong_secret_key"

Google Forms Webhook

You can set up a webhook to trigger the webhook_server.py endpoint whenever a new form is submitted. This requires a public-facing URL for your server (e.g., using a service like ngrok).

Usage
The application is designed to run continuously as a server. The primary way to trigger its functionality is via a form submission webhook.

To start the server:

python webhook_server.py

The server will start on http://0.0.0.0:5000 (or the port specified in your environment).

Endpoints:

POST /webhook: The main endpoint for receiving Google Forms submission notifications.

GET /status: Provides the current status of the processing queue.

POST /trigger: Manually triggers a new processing cycle.

GET /health: A basic health check endpoint.

GET /: Returns basic service information.

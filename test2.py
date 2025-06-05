from dotenv import load_dotenv
import os
import json
import logging
import traceback
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pytz
from urllib.parse import urlencode
import csv


# Load environment variables
load_dotenv()

# Hardcoded Authentication Credentials
USERNAME = 'bala+skyview_admin@ecosuite.io'
PASSWORD = 'Vanaja@13882'

# Hardcoded inputs
PROJECT_CODES = ['0193', '0191']
START_DATE = '2025-05-01'
END_DATE = '2025-05-15'
TOKEN = "eyJraWQiOiJRYjZZNEVhSVNseXJoTzdFaVFPeFJjbE0xaTdDTWZuRXQ2VXBaUGhUa21JPSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoiYTZXWjRTRHpyNEtJVnNRNC1zREdtZyIsInN1YiI6IjAyNWExYWQ2LWE4NDQtNGQ2MC1hMzRlLThhODMzYmQ5MDY5MSIsImN1c3RvbTpyZXN0cmljdFByb2plY3RzIjoibm8iLCJjdXN0b206dGltZXpvbmUiOiJBbWVyaWNhXC9OZXdfWW9yayIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJjdXN0b206bGFzdE5hbWUiOiJBZG1pbiIsImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC51cy1lYXN0LTEuYW1hem9uYXdzLmNvbVwvdXMtZWFzdC0xXzdCQkJoR2YxcCIsImN1c3RvbTpwcmV2ZW50RG93bmxvYWQiOiJubyIsImNvZ25pdG86dXNlcm5hbWUiOiIwMjVhMWFkNi1hODQ0LTRkNjAtYTM0ZS04YTgzM2JkOTA2OTEiLCJjdXN0b206c3RhcnREYXRlIjoiMjAyNC0xMi0xNiIsImN1c3RvbTpvcmdhbml6YXRpb25JZCI6Im9yZ2FuaXphdGlvbi04MDJlNjZlOS03YWU3LTQwYTktOTg4MC0xMjU3ZTUwZTNiZTIiLCJjdXN0b206dXNlclR5cGUiOiJ1c2VyLXR5cGUtZWY2NTMwMGQtNDQ5ZC00YmFiLTgzZDEtYmRiOWM0OTA4M2IwIiwiYXVkIjoiMW11bHNkZzZkMWlxY2djZGFkdm1iODVpMDgiLCJldmVudF9pZCI6Ijk5NDFlNzAxLTUxMzUtNDMwYy05NmQyLTdiYzcxYzFmODJmNCIsImN1c3RvbTpmaXJzdE5hbWUiOiJCYWxhLlNreXZpZXciLCJ0b2tlbl91c2UiOiJpZCIsImF1dGhfdGltZSI6MTc0NjMxMDUxNiwiZXhwIjoxNzQ5MTAwNDUxLCJpYXQiOjE3NDkwOTY4NTIsImVtYWlsIjoiYmFsYStza3l2aWV3X2FkbWluQGVjb3N1aXRlLmlvIn0.UJ_W3dF4C9z5_c1ZhaK1pN-Lj9RAhsvok5UQDKQ5769BT4i5-1a46d0D0sAPcdSl5qsbUlHT-CXrmHEGYcu-5_wS61f20hnbeeq-npO5hZDy6BZ4MA-sX2KLtcvVl0AoaKCe-ikxq3HZzLNksToDry9Y-tM0tJca9iC4spxEcXQCVi7aZhwBMmUBG0oxlWGAi5v0QaTwWEK8hlO7-GyAKmly6WYQQP8razf9YtAWerN8bv9goM9sXbRxsjbzx2j2wZfxhyAWVLDUOWbVEvT8nTF3T3pMlabDwlqasv_IQpmN_Zj16N9PeecGGN1cauVkpQALvXvNB7pwt13JpcouOA"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('ecosuite_data_extractor.log', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EcosuiteDataExtractorDebugger:
    def __init__(self, api_key):
        self.api_key = api_key
        self.debug_info = {
            'api_calls': [],
            'errors': []
        }

    def log_api_call(self, endpoint, method, status_code, response_time):
        api_call = {
            'endpoint': endpoint,
            'method': method,
            'status_code': status_code,
            'timestamp': datetime.now().isoformat(),
            'response_time': response_time
        }
        
        self.debug_info['api_calls'].append(api_call)

    def log_error(self, context, error):
        error_entry = {
            'context': context,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc(),
            'timestamp': datetime.now().isoformat()
        }
        self.debug_info['errors'].append(error_entry)
        logger.error(f"Error in {context}: {error}")

    def export_debug_report(self, output_file='ecosuite_debug_report.json'):
        try:
            with open(output_file, 'w') as f:
                json.dump(self.debug_info, f, indent=2)
            logger.info(f"Debug report exported to {output_file}")
        except Exception as e:
            logger.error(f"Failed to export debug report: {e}")

def save_to_json(filename: str, data: dict, project_id: str, start_date: str, end_date: str, folder_path: str) -> str:
    """Save data to JSON file with project-specific naming"""
    filename = f"{project_id}_{start_date}_{end_date}_{filename}"
    full_path = os.path.join(folder_path, filename)
    
    try:
        with open(full_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        logger.info(f"Data successfully saved to {full_path}")
        return full_path
    except Exception as e:
        logger.error(f"Failed to save data to {full_path}: {str(e)}")
        return None
    

def format_number(value):
    """
    Format a numeric value with commas as thousand separators
    preserving full decimal precision
    """
    if isinstance(value, (int, float)):
        # Format with commas but preserve all decimal places
        if value == int(value):
            # For whole numbers, show no decimal places
            return f"{value:,.0f}"
        else:
            # For decimal numbers, show full precision
            return f"{value:,}"
    else:
        # Return as is if it's not a number
        return value
    
def create_project_folder(project_name: str, start_date: str, end_date: str) -> str:
    """Create project folder with date-based naming inside the output folder"""
    # Create output folder if it doesn't exist
    output_folder = os.path.join(os.getcwd(), "output")
    os.makedirs(output_folder, exist_ok=True)
    
    # Create project folder inside output folder
    project_folder = os.path.join(output_folder, f"{project_name}_{start_date}_{end_date}")
    raw_data_folder = os.path.join(project_folder, "raw_data")
    os.makedirs(raw_data_folder, exist_ok=True)
    return raw_data_folder

def create_reports_folder() -> str:
    """Create Reports folder for CSV and Excel files"""
    reports_folder = os.path.join(os.getcwd(), "Reports")
    os.makedirs(reports_folder, exist_ok=True)
    return reports_folder

def fetch_project_details(project_id: str, api_token: str, debugger: EcosuiteDataExtractorDebugger) -> Optional[Dict]:
    """Fetch project details from Ecosuite API"""
    base_url = "https://api.ecosuite.io/projects"
    endpoint = f"{base_url}/{project_id}"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(endpoint, headers=headers)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched project details")
            return data
        else:
            debugger.log_error("fetch_project_details", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_project_details", e)
        return None

def fetch_price_data(project_id: str, api_token: str, debugger: EcosuiteDataExtractorDebugger) -> Optional[Dict]:
    """Fetch price data from Ecosuite API"""
    base_url = "https://api.ecosuite.io/projects"
    endpoint = f"{base_url}/{project_id}/pro-forma"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(endpoint, headers=headers)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched price data")
            return data
        else:
            debugger.log_error("fetch_price_data", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_price_data", e)
        return None

def adjust_end_date(end_date: str) -> str:
    """
    Adjust end date by adding one day to include the specified end date in results
    """
    try:
        # Parse the date string
        date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Add one day
        adjusted_date = date_obj + timedelta(days=1)
        
        # Return in the same format
        return adjusted_date.strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"Error adjusting end date: {e}")
        return end_date  # Return original if there's an error

def fetch_ecosuite_energy_datums(project_id: str, start_date: str, end_date: str, 
                                  aggregation: str, api_token: str, 
                                  raw_data_folder: str,
                                  debugger: EcosuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch energy datums from the Ecosuite API and save to file
    """
    base_url = "https://api.ecosuite.io/energy/datums"
    endpoint = f"{base_url}/projects/{project_id}"
    
    # Adjust end date to include the specified end date in results
    adjusted_end_date = adjust_end_date(end_date)
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "start": start_date,
        "end": adjusted_end_date,  # Use adjusted end date
        "aggregation": aggregation
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(endpoint, headers=headers, params=params)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched energy datums")
            
            # Save the raw energy datums to a JSON file
            filename = f"{project_id}_{start_date}_{end_date}_energy_datums.json"
            full_path = os.path.join(raw_data_folder, filename)
            
            try:
                with open(full_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                logger.info(f"Energy datums saved to {full_path}")
                print(f"Energy datums saved to {full_path}")
            except Exception as save_error:
                logger.error(f"Failed to save energy datums: {save_error}")
                print(f"Failed to save energy datums: {save_error}")
            
            return data
        else:
            debugger.log_error("fetch_ecosuite_energy_datums", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_ecosuite_energy_datums", e)
        return None
    except json.JSONDecodeError as e:
        debugger.log_error("fetch_ecosuite_energy_datums", 
                         Exception(f"JSON Decode Error: {e}"))
        return None

def fetch_expected_generation_with_project_ids(project_id: str, start_date: str, end_date: str, 
                               aggregation: str, api_token: str, 
                               raw_data_folder: str,
                               debugger: EcosuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch expected generation data from the Ecosuite API using projectIds parameter and save to file
    """
    base_url = "https://api.ecosuite.io/energy/datums/generation/expected"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    # Adjust end date to include the specified end date in results
    adjusted_end_date = adjust_end_date(end_date)
    
    params = {
        "start": start_date,
        "end": adjusted_end_date,
        "projectIds": project_id
    }
    
    # Add aggregation only if it's provided
    if aggregation:
        params["aggregation"] = aggregation
    
    # Log the API call endpoint for debugging
    endpoint = f"{base_url}?start={start_date}&end={adjusted_end_date}&projectIds={project_id}"
    if aggregation:
        endpoint += f"&aggregation={aggregation}"
    
    start_time = datetime.now()
    try:
        response = requests.get(base_url, headers=headers, params=params)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully fetched expected generation data with projectIds={project_id}")
            
            # Save the raw expected generation data to a JSON file
            filename = f"expected_generation_{project_id}.json"
            full_path = os.path.join(raw_data_folder, filename)
            
            try:
                with open(full_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                logger.info(f"Expected generation data with projectIds saved to {full_path}")
                print(f"Expected generation data with projectIds saved to {full_path}")
            except Exception as save_error:
                logger.error(f"Failed to save expected generation data with projectIds: {save_error}")
                print(f"Failed to save expected generation data with projectIds: {save_error}")
            
            return data
        else:
            debugger.log_error("fetch_expected_generation_with_project_ids", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_expected_generation_with_project_ids", e)
        return None
    except json.JSONDecodeError as e:
        debugger.log_error("fetch_expected_generation_with_project_ids", 
                         Exception(f"JSON Decode Error: {e}"))
        return None


def fetch_forecast_generation(project_id: str, start_date: str, end_date: str, token: str, raw_data_folder: str) -> dict:
    """
    Fetch forecast generation data and save it to raw folder
    """
    adjusted_end = adjust_end_date(end_date)  # ADD THIS LINE

    url = f"https://api.ecosuite.io/energy/datums/generation/predicted/projects/{project_id}"
    params = {"start": start_date, "end": adjusted_end, "aggregation": "day"}
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch forecast generation: {response.status_code} - {response.text}")

    forecast_data = response.json()
    file_path = os.path.join(raw_data_folder, f"{project_id}_{start_date}_{end_date}_forecast_generation.json")
    with open(file_path, "w") as f:
        json.dump(forecast_data, f, indent=2)
    return forecast_data




def fetch_ecosuite_weather_datums(project_id: str, start_date: str, end_date: str, 
                                  aggregation: str, api_token: str, 
                                  raw_data_folder: str,
                                  debugger: EcosuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch weather datums from the Ecosuite API and save to file
    """
    base_url = "https://api.ecosuite.io/weather/datums"
    endpoint = f"{base_url}/projects/{project_id}"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "start": start_date,
        "end": end_date,
        "aggregation": aggregation
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(endpoint, headers=headers, params=params)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched weather datums")
            
            # Save the raw weather datums to a JSON file
            filename = f"{project_id}_{start_date}_{end_date}_weather_datums.json"
            full_path = os.path.join(raw_data_folder, filename)
            
            try:
                with open(full_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                logger.info(f"Weather datums saved to {full_path}")
                print(f"Weather datums saved to {full_path}")
            except Exception as save_error:
                logger.error(f"Failed to save weather datums: {save_error}")
                print(f"Failed to save weather datums: {save_error}")
            
            return data
        else:
            debugger.log_error("fetch_ecosuite_weather_datums", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_ecosuite_weather_datums", e)
        return None
    except json.JSONDecodeError as e:
        debugger.log_error("fetch_ecosuite_weather_datums", 
                         Exception(f"JSON Decode Error: {e}"))
        return None

def read_project_codes_from_csv(csv_file_path: str) -> List[str]:
    """
    Read project codes from a CSV file with improved error handling
    """
    project_codes = []
    try:
        with open(csv_file_path, 'r') as csvfile:
            # First try to read the file content
            content = csvfile.read()
            print(f"CSV file content preview: {content[:200]}...")
            
            # Reset file pointer
            csvfile.seek(0)
            
            # Try with common delimiters instead of auto-detection
            for delimiter in [',', ';', '\t']:
                try:
                    csvfile.seek(0)  # Reset file pointer
                    reader = csv.reader(csvfile, delimiter=delimiter)
                    
                    # Read first row to check
                    first_row = next(reader, None)
                    if first_row and len(first_row) > 0:
                        print(f"Successfully read CSV with delimiter '{delimiter}'. First row: {first_row}")
                        
                        # Assume first column contains project codes
                        # Reset file pointer
                        csvfile.seek(0)
                        reader = csv.reader(csvfile, delimiter=delimiter)
                        
                        # Skip header if present
                        header = next(reader, None)
                        if header:
                            print(f"CSV header: {header}")
                        
                        # Read project codes from first column
                        for row in reader:
                            if row and len(row) > 0 and row[0].strip():
                                project_codes.append(row[0].strip())
                        
                        print(f"Read {len(project_codes)} project codes from {csv_file_path}")
                        return project_codes
                except Exception as e:
                    print(f"Failed with delimiter '{delimiter}': {e}")
                    continue
            
            # If we get here, none of the common delimiters worked
            # Try a more manual approach
            csvfile.seek(0)
            lines = csvfile.readlines()
            for line in lines:
                # Remove any newline characters and white space
                line = line.strip()
                if line:
                    # Just take the first word as the project code
                    project_code = line.split()[0].strip()
                    if project_code:
                        project_codes.append(project_code)
            
            if project_codes:
                print(f"Read {len(project_codes)} project codes using fallback method")
                return project_codes
            
            print(f"Could not read project codes from CSV file: {csv_file_path}")
            return []
    
    except Exception as e:
        logger.error(f"Error reading project codes from CSV: {e}")
        print(f"Error reading project codes from CSV: {e}")
        return []

def get_x_sn_date(now):
    """Generate x-sn-date header value in required format"""
    return now.strftime("%Y%m%d%H%M%S")

def generate_auth_header(token, secret, method, path, query_params, headers, request_body, now):
    """Generate authentication header for SolarNetwork API"""
    # This function should contain the SolarNetwork auth logic
    # Simplified implementation for example purposes
    return f"SolarNetworkWS {token}:{secret}"

class SolarNetworkClient:
    def __init__(self, token: str, secret: str, debugger=None) -> None:
        self.token = token
        self.secret = secret
        self.debugger = debugger

    def list(self, nodeIds: List[int], sourceIds: List[str], startDate: str, endDate: str, projectId: str, aggregation: str = "") -> Dict:
        """
        Fetch data from SolarNetwork API
        """
        now = datetime.now(pytz.UTC)
        date = get_x_sn_date(now)
        path = "/solarquery/api/v1/sec/datum/list"
        start_time = datetime.now()

        headers = {"host": "data.solarnetwork.net", "x-sn-date": date}
        
        nodeIds_str = ','.join(map(str, nodeIds))
        sourceIds_str = ','.join(f"/{projectId}{source}" for source in sourceIds)
        
        # Use same aggregation logic for all data types
        query_params = {
            "aggregation": aggregation if aggregation else None,
            "endDate": endDate,
            "nodeIds": nodeIds_str,
            "sourceIds": sourceIds_str,
            "startDate": startDate,
        }

        query_params = {k: v for k, v in query_params.items() if v is not None}
        encoded_params = urlencode(query_params)

        auth = generate_auth_header(
            self.token, self.secret, "GET", path, encoded_params, headers, "", now
        )

        url = f"https://data.solarnetwork.net{path}?{encoded_params}"
        
        logger.debug(f"Making API call to {url}")
        
        try:
            resp = requests.get(
                url=url,
                headers={
                    "host": "data.solarnetwork.net",
                    "x-sn-date": date,
                    "Authorization": auth,
                },
            )

            response_time = (datetime.now() - start_time).total_seconds()
            
            if self.debugger:
                self.debugger.log_api_call(url, "GET", resp.status_code, response_time)

            v = resp.json()
            if v.get("success") != True:
                error_msg = f"Unsuccessful API call: {v}"
                if self.debugger:
                    self.debugger.log_error("SolarNetwork API", Exception(error_msg))
                raise Exception("Unsuccessful API call")

            return v.get("data", {})
            
        except Exception as e:
            if self.debugger:
                self.debugger.log_error("SolarNetwork API", e)
            raise e

def convert_to_utc(date_str: str, tz_str: str, is_start: bool = True) -> str:
    """
    Convert a local date to UTC ISO format for API requests
    """
    local_tz = pytz.timezone(tz_str)
    local_date = datetime.strptime(date_str, "%Y-%m-%d")
    
    if is_start:
        local_date = local_date.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        local_date = local_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    local_dt = local_tz.localize(local_date)
    utc_dt = local_dt.astimezone(pytz.UTC)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]+'Z'

def fetch_solarnetwork_weather_data(project_id: str, 
                                  project_details: Dict,
                                  start_date: str, 
                                  end_date: str,
                                  aggregation: str,
                                  raw_data_folder: str,
                                  debugger: EcosuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch weather data from SolarNetwork API and save to file
    """
    try:
        # Get credentials
        sn_token = "mrqiO-fM0ocfG2ca_V9G"
        sn_secret = "zXrLA2hMS73kX3wFoavkl-uBE4k-jMw"
        
        # Create SolarNetwork client
        sn_client = SolarNetworkClient(sn_token, sn_secret, debugger)
        
        # Get project timezone
        project_tz = project_details.get('project', {}).get('sites', {}).get('S1', {}).get('timezone', 'UTC')
        
        # Use original end date for file naming but adjusted date for the API call
        adjusted_end_date = adjust_end_date(end_date)
        
        # Convert dates to UTC for SolarNetwork API
        utc_start_date = convert_to_utc(start_date, project_tz, True)
        utc_end_date = convert_to_utc(adjusted_end_date, project_tz, False)
        
        # Get number of systems
        sites = project_details.get('project', {}).get('sites', {})
        first_site_key = next(iter(sites), None)
        systems = sites.get(first_site_key, {}).get('systems', {}) if first_site_key else {}

        num_systems = len(systems)
        
        if num_systems == 0:
            logger.error(f"No systems found for project {project_id}")
            return None
        
        # Initialize all_weather_data
        all_weather_data = {
            "project_id": project_id,
            "start_date": start_date,
            "end_date": end_date,
            "systems": {}
        }
        
        # For each system, fetch weather data
        for system_num in range(1, num_systems + 1):
            system_id = f"R{system_num}"
            
            try:
                # Fetch weather data from SolarNetwork
                weather_result = sn_client.list(
                    [739],  # Node ID for weather data
                    [f"/{first_site_key}/{system_id}/WEATHER/1"],  # Source ID pattern
                    utc_start_date, 
                    utc_end_date, 
                    project_id, 
                    aggregation
                )
                
                # Store in all_weather_data
                all_weather_data["systems"][system_id] = {
                    "weather_data": weather_result.get("results", [])
                }
                
                logger.info(f"Successfully fetched SolarNetwork weather data for system {system_id}")
                
            except Exception as e:
                logger.error(f"Error fetching SolarNetwork weather data for system {system_id}: {e}")
                all_weather_data["systems"][system_id] = {
                    "weather_data": [],
                    "error": str(e)
                }
        
        # Save to JSON file
        sn_weather_filename = f"{project_id}_{start_date}_{end_date}_solarnetwork_weather.json"
        sn_weather_path = os.path.join(raw_data_folder, sn_weather_filename)
        
        with open(sn_weather_path, 'w') as json_file:
            json.dump(all_weather_data, json_file, indent=4)
        
        logger.info(f"SolarNetwork weather data saved to {sn_weather_path}")
        print(f"SolarNetwork weather data saved to {sn_weather_path}")
        
        return all_weather_data
        
    except Exception as e:
        logger.error(f"Error in fetch_solarnetwork_weather_data: {e}")
        debugger.log_error("fetch_solarnetwork_weather_data", e)
        return None

def fetch_expected_generation(project_id: str, start_date: str, end_date: str, 
                             aggregation: str, api_token: str, 
                             raw_data_folder: str,
                             debugger: EcosuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch expected generation data from the Ecosuite API and save to file
    """
    base_url = "https://api.ecosuite.io/energy/datums/generation/expected"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "start": start_date,
        "end": end_date,
        "aggregation": aggregation
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(base_url, headers=headers, params=params)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(base_url, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched expected generation data")
            
            # Save the raw expected generation data to a JSON file
            filename = f"{project_id}_{start_date}_{end_date}_expected_generation.json"
            full_path = os.path.join(raw_data_folder, filename)
            
            try:
                with open(full_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                logger.info(f"Expected generation data saved to {full_path}")
                print(f"Expected generation data saved to {full_path}")
            except Exception as save_error:
                logger.error(f"Failed to save expected generation data: {save_error}")
                print(f"Failed to save expected generation data: {save_error}")
            
            return data
        else:
            debugger.log_error("fetch_expected_generation", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_expected_generation", e)
        return None
    except json.JSONDecodeError as e:
        debugger.log_error("fetch_expected_generation", 
                         Exception(f"JSON Decode Error: {e}"))
        return None

def aggregate_forecast_generation(forecast_data: dict) -> float:
    """
    Dynamically sum predictedGeneration across all systems/sites using the most detailed available data.
    Falls back to projectDatums.aggregatedTotals if needed.
    Returns total forecast generation in kWh.
    """
    total_wh = 0

    try:
        sites = forecast_data.get("projectDatums", {}).get("sites", {})

        for site_data in sites.values():
            systems = site_data.get("systems", {})
            for system_data in systems.values():
                agg_totals = system_data.get("aggregatedTotals", {})
                for day_data in agg_totals.values():
                    total_wh += day_data.get("predictedGeneration", 0)

        # Fallback if no detailed system-level data is present
        if total_wh == 0:
            top_agg = forecast_data.get("projectDatums", {}).get("aggregatedTotals", {})
            for day_data in top_agg.values():
                total_wh += day_data.get("predictedGeneration", 0)

        return round(total_wh / 1000, 2)  # Convert Wh to kWh
    except Exception as e:
        logger.warning(f"Failed to aggregate forecast generation: {e}")
        return 0.0

def calculate_actual_insolation(expected_gen_data, project_details, project_id):
    sites_expected = expected_gen_data.get('projects', {}).get(project_id, {}).get('sites', {})
    sites_metadata = project_details.get('project', {}).get('sites', {})

    total_weighted_irradiance = 0
    total_dc_size = 0

    for site_id, site_data in sites_expected.items():
        systems_expected = site_data.get('systems', {})
        systems_metadata = sites_metadata.get(site_id, {}).get('systems', {})

        for system_id, system_data in systems_expected.items():
            irradiance_wh = system_data.get('irradianceHours', 0)
            dc_kw = systems_metadata.get(system_id, {}).get('dcSize', 0)

            irradiance_kwh = irradiance_wh / 1000
            total_weighted_irradiance += irradiance_kwh * dc_kw
            total_dc_size += dc_kw

    return round(total_weighted_irradiance / total_dc_size, 3) if total_dc_size else 0

def calculate_total_actual_generation(energy_data: dict) -> float:
    """
    Calculate total actual generation from energy data
    """
    total_actual_gen = 0
    try:
        sites = energy_data.get('project', {}).get('sites', {})
        for site_id, site_data in sites.items():
            systems = site_data.get('systems', {})
            for system_id, system_data in systems.items():
                aggregated_totals = system_data.get('aggregatedTotals', {})
                for timestamp, values in aggregated_totals.items():
                    generation = values.get('generation', 0)
                    total_actual_gen += generation / 1000  # Convert Wh to kWh
        logger.info(f"✅ Total actual generation: {total_actual_gen:,.2f} kWh")
    except Exception as e:
        logger.error(f"❌ Error calculating actual generation: {str(e)}")
    return total_actual_gen

def generate_project_data(project_id: str, start_date: str, end_date: str, 
                          raw_data_folder: str, aggregation: str) -> dict:
    """
    Generate data for a single project for the consolidated report
    """
    try:
        import calendar
        
        # Load all JSON files
        energy_path = os.path.join(raw_data_folder, f"{project_id}_{start_date}_{end_date}_energy_datums.json")
        expected_gen_path = os.path.join(raw_data_folder, f"expected_generation_{project_id}.json")
        price_path = os.path.join(raw_data_folder, f"{project_id}_{start_date}_{end_date}_price_data.json")
        details_path = os.path.join(raw_data_folder, f"{project_id}_{start_date}_{end_date}_project_details.json")
        forecast_path = os.path.join(raw_data_folder, f"{project_id}_{start_date}_{end_date}_forecast_generation.json")
        
        with open(forecast_path, 'r') as f:
            forecast_data = json.load(f)
        forecast_generation = aggregate_forecast_generation(forecast_data)
        
        with open(energy_path, 'r') as f:
            energy_data = json.load(f)
        with open(expected_gen_path, 'r') as f:
            expected_gen_data = json.load(f)
        with open(price_path, 'r') as f:
            price_data = json.load(f)
        with open(details_path, 'r') as f:
            project_details = json.load(f)
            actual_insolation = calculate_actual_insolation(expected_gen_data, project_details, project_id)
            
        # Get project metadata
        project_name = project_details.get('project', {}).get('name', 'Unknown')
        project_state = project_details.get('project', {}).get('state', '')
        cod_date = project_details.get('project', {}).get('milestones', {}).get('operational', '') or project_details.get('project', {}).get('productionStartDate', '')
        system_size = project_details.get('project', {}).get('dcSize', 0)

        # Calculate actual and expected generation
        total_actual_gen = calculate_total_actual_generation(energy_data)
        
        total_expected_gen = 0
        try:
            project_data = expected_gen_data.get('projects', {}).get(project_id, {})
            if 'expectedGeneration' in project_data:
                total_expected_gen = project_data['expectedGeneration']
            else:
                for date_key, data in project_data.get('aggregatedTotals', {}).items():
                    if date_key.startswith(start_date[:7]):
                        total_expected_gen += data.get('expectedGeneration', 0)
            total_expected_gen /= 1000
        except Exception:
            pass

        # Calculate variances
        gen_variance_pct = ((total_actual_gen - total_expected_gen) / total_expected_gen * 100) if total_expected_gen else 0
        lost_gen_availability = 0  # No availability loss calculation for now
        total_gen = total_actual_gen + lost_gen_availability
        combined_variance_pct = ((total_gen - total_expected_gen) / total_expected_gen * 100) if total_expected_gen else 0

        # Calculate weather adjustments
        report_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        month_name = report_date_obj.strftime('%B').lower()
        last_day = calendar.monthrange(report_date_obj.year, report_date_obj.month)[1]

        # Get insolation data
        monthly_irradiance = {}
        try:
            sites = project_details.get('project', {}).get('sites', {})
            first_site = next(iter(sites.values()), {})
            systems = first_site.get('systems', {})
            first_system = next(iter(systems.values()), {})
            monthly_irradiance = first_system.get('forecast', {}).get('irradiance', {}).get('monthlyIrradiance', {})
        except Exception as e:
            logger.warning(f"Could not extract forecast irradiance: {e}")

        forecast_insolation = monthly_irradiance.get(month_name, 0) * last_day if monthly_irradiance else 0
        insolation_variance_pct = ((actual_insolation - forecast_insolation) / forecast_insolation * 100) if forecast_insolation else 0
        weather_normalized_expected = forecast_generation * (actual_insolation / forecast_insolation) if forecast_insolation and actual_insolation else total_expected_gen
        weather_normalized_variance = total_actual_gen - weather_normalized_expected

        # Get PPA and REC rates
        ppa_rate = 0
        rec_rate = 0
        for cashflow in price_data.get('proForma', {}).get('cashFlows', []):
            if cashflow.get('category') == 'Income':
                if cashflow.get('account') == 'PPA/FIT':
                    for payment in cashflow.get('payments', []):
                        ppa_rate = payment.get('recurrence', {}).get('startRate', ppa_rate)
                elif cashflow.get('account') == 'SREC Revenue':
                    for payment in cashflow.get('payments', []):
                        rec = payment.get('recurrence', {})
                        if rec.get('rateType') == 'fixed':
                            rec_rate = rec.get('startRate', rec_rate)
                        elif 'rates' in rec:
                            try:
                                years_diff = report_date_obj.year - datetime.strptime(rec.get('startDate', '2018-01-01'), '%Y-%m-%d').year
                                rec_rate = rec['rates'][years_diff].get(month_name, 0)
                            except Exception:
                                pass

        # Calculate revenues
        actual_revenue = total_actual_gen * ppa_rate
        expected_revenue = total_expected_gen * ppa_rate
        revenue_variance = actual_revenue - expected_revenue

        actual_recs = total_actual_gen / 1000
        expected_recs = total_expected_gen / 1000
        rec_rate_display = rec_rate * 1000
        actual_rec_revenue = actual_recs * rec_rate_display
        expected_rec_revenue = expected_recs * rec_rate_display
        rec_revenue_variance = actual_rec_revenue - expected_rec_revenue

        total_actual_revenue = actual_revenue + actual_rec_revenue
        total_expected_revenue = expected_revenue + expected_rec_revenue
        total_revenue_variance = total_actual_revenue - total_expected_revenue

        return {
            'Project Code': project_id,
            'Project Name': project_name,
            'State': project_state,
            'COD': cod_date,
            'Size (kW)': format_number(system_size),
            'Actual Generation (kWh)': format_number(total_actual_gen),
            'Expected Generation (kWh)': format_number(total_expected_gen),
            'Forecast Generation (kWh)': format_number(forecast_generation),
            'Variance with Expected Generation (%)': f"{gen_variance_pct:.2f}%",
            'Availability Loss (kWh)': format_number(lost_gen_availability),
            'Actual + Availability Loss (kWh)': format_number(total_gen),
            'Total Variance with Expected Generation (%)': f"{combined_variance_pct:.2f}%",
            'Actual Insolation (kWh/m2)': format_number(actual_insolation),
            'Forecast Insolation (kWh/m2)': format_number(forecast_insolation),
            'Variance Insolation (%)': f"{insolation_variance_pct:.2f}%",
            'Weather Adjusted Forecast Generation (kWh)': format_number(weather_normalized_expected),
            'Weather Adjusted Generation Variance (kWh)': format_number(weather_normalized_variance),
            'Av PPA Price ($/kWh)': format_number(ppa_rate),
            'Anticipated PPA Revenue ($)': format_number(actual_revenue),
            'Expected PPA Revenue ($)': format_number(expected_revenue),
            'PPA Revenue Variance ($)': format_number(revenue_variance),
            'Av REC Sale Price ($/MWh)': format_number(rec_rate_display),
            'Actual RECs Generated': format_number(actual_recs),
            'Expected RECs': format_number(expected_recs),
            'Anticipated RECs Revenue ($)': format_number(actual_rec_revenue),
            'Expected RECs Revenue ($)': format_number(expected_rec_revenue),
            'REC Revenue Variance ($)': format_number(rec_revenue_variance),
            'Total Anticipated Revenue ($)': format_number(total_actual_revenue),
            'Total Expected Revenue ($)': format_number(total_expected_revenue),
            'Total Revenue Variance ($)': format_number(total_revenue_variance)
        }

    except Exception as e:
        logger.error(f"Error generating project data: {e}", exc_info=True)
        return None

#---------------------------------------------------


def calculate_total_actual_generation(energy_data: dict) -> float:
    """
    Calculate total actual generation from energy data
    """
    total_actual_gen = 0
    try:
        sites = energy_data.get('project', {}).get('sites', {})
        for site_id, site_data in sites.items():
            systems = site_data.get('systems', {})
            for system_id, system_data in systems.items():
                aggregated_totals = system_data.get('aggregatedTotals', {})
                for timestamp, values in aggregated_totals.items():
                    generation = values.get('generation', 0)
                    total_actual_gen += generation / 1000  # Convert Wh to kWh
        logger.info(f"✅ Total actual generation: {total_actual_gen:,.2f} kWh")
    except Exception as e:
        logger.error(f"❌ Error calculating actual generation: {str(e)}")
    return total_actual_gen



import csv
import calendar
from datetime import datetime




def generate_csv_report(project_id: str, start_date: str, end_date: str, 
                        raw_data_folder: str, aggregation: str) -> str:
    """
    Generate a CSV report from the extracted data with specific fields
    """
    try:
        # Create Reports folder for storing CSV files
        reports_folder = create_reports_folder()
        
        # Define the output CSV path in Reports folder
        csv_filename = f"{project_id}_{start_date}_{end_date}_performance_report.csv"
        csv_path = os.path.join(reports_folder, csv_filename)

        # Load all the necessary JSON files
        try:
            with open(os.path.join(raw_data_folder, f"{project_id}_{start_date}_{end_date}_energy_datums.json"), 'r') as f:
                energy_data = json.load(f)
            logger.info(f"Loaded energy data successfully")

            with open(os.path.join(raw_data_folder, f"expected_generation_{project_id}.json"), 'r') as f:
                expected_gen_data = json.load(f)
            logger.info(f"Loaded expected generation data successfully")

            with open(os.path.join(raw_data_folder, f"{project_id}_{start_date}_{end_date}_price_data.json"), 'r') as f:
                price_data = json.load(f)
            logger.info(f"Loaded price data successfully")

            with open(os.path.join(raw_data_folder, f"{project_id}_{start_date}_{end_date}_project_details.json"), 'r') as f:
                project_details = json.load(f)
                actual_insolation = calculate_actual_insolation(expected_gen_data, project_details, project_id)
            logger.info(f"Loaded project details successfully")
        except FileNotFoundError as e:
            logger.error(f"Required data file not found: {e}")
            return None
        
        # ✅ Load and aggregate forecast generation from API JSON (NOT project_details)
        # ✅ Parse date for month name
        report_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        month_name = report_date_obj.strftime('%B').lower()

        # ✅ Load and aggregate forecast generation from API JSON (NOT project_details)
        forecast_path = os.path.join(raw_data_folder, f"{project_id}_{start_date}_{end_date}_forecast_generation.json")
        with open(forecast_path, 'r') as f:
            forecast_data = json.load(f)
        forecast_generation = aggregate_forecast_generation(forecast_data)

        # ✅ Load forecast irradiance (optional but supported)
        monthly_irradiance = {}
        try:
            sites = project_details.get('project', {}).get('sites', {})
            first_site = next(iter(sites.values()), {})
            systems = first_site.get('systems', {})
            first_system = next(iter(systems.values()), {})
            monthly_irradiance = first_system.get('forecast', {}).get('irradiance', {}).get('monthlyIrradiance', {})
            logger.info(f"Found forecast generation for {month_name}: {forecast_generation} kWh")
        except Exception as e:
            logger.warning(f"Could not extract forecast irradiance: {e}")



            logger.info(f"Found forecast generation for {month_name}: {forecast_generation} kWh")
        except Exception as e:
            logger.warning(f"Could not extract forecast generation or irradiance: {e}")

        # Create CSV file
        with open(csv_path, 'w', newline='') as csvfile:
            csvfile.write("CSV file generated by the Ecosuite Variance Report tool\n")
            csvfile.write("For documentation on how these metrics are calculated, visit: https://docs.google.com/document/d/e/2PACX-1vTeN8uIWsHdDujwMb_2GRVfBn_EnYLOyvYal2a7wIY9utzMAcaD7lyKflud1hVW1qaTJRUYxori_ocV/pub\n\n")

            headers = [
                'Project Code', 'Project Name', 'State', 'COD', 'Size (kW)', 'Start Date (YYYY-MM-DD)', 'End Date (YYYY-MM-DD)',
                'Actual Generation (kWh)', 'Expected Generation (kWh)', 'Forecast Generation (kWh)',
                'Variance with Expected Generation (%)', 'Availability Loss (kWh)', 'Actual + Availability Loss (kWh)',
                'Total Variance with Expected Generation (%)', 'Actual Insolation (kWh/m2)', 'Forecast Insolation (kWh/m2)',
                'Variance Insolation (%)', 'Weather Adjusted Forecast Generation (kWh)', 'Weather Adjusted Generation Variance (kWh)',
                'Av PPA Price ($/kWh)', 'Anticipated PPA Revenue ($)', 'Expected PPA Revenue ($)', 'PPA Revenue Variance ($)',
                'Av REC Sale Price ($/MWh)', 'Actual RECs Generated', 'Expected RECs', 'Anticipated RECs Revenue ($)',
                'Expected RECs Revenue ($)', 'REC Revenue Variance ($)', 'Total Anticipated Revenue ($)',
                'Total Expected Revenue ($)', 'Total Revenue Variance ($)'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()

            # Extract metadata
            project_name = project_details.get('project', {}).get('name', 'Unknown')
            project_state = project_details.get('project', {}).get('state', '')
            cod_date = project_details.get('project', {}).get('milestones', {}).get('operational', '') or project_details.get('project', {}).get('productionStartDate', '')
            system_size = project_details.get('project', {}).get('dcSize', 0)

            # PPA and REC
            ppa_rate, rec_rate = 0, 0
            for cashflow in price_data.get('proForma', {}).get('cashFlows', []):
                if cashflow.get('category') == 'Income' and cashflow.get('account') == 'PPA/FIT':
                    for payment in cashflow.get('payments', []):
                        if 'recurrence' in payment and 'startRate' in payment['recurrence']:
                            ppa_rate = payment['recurrence']['startRate']
                            break
                if cashflow.get('category') == 'Income' and cashflow.get('account') == 'SREC Revenue':
                    for payment in cashflow.get('payments', []):
                        recurrence = payment.get('recurrence', {})
                        if recurrence.get('rateType') == 'fixed':
                            rec_rate = recurrence.get('startRate', 0)
                        elif 'rates' in recurrence:
                            try:
                                start_dt = datetime.strptime(recurrence.get('startDate', '2018-01-01'), '%Y-%m-%d')
                                years_diff = datetime.strptime(start_date, '%Y-%m-%d').year - start_dt.year
                                if years_diff < len(recurrence['rates']):
                                    rec_rate = recurrence['rates'][years_diff].get(month_name, 0)
                            except Exception:
                                pass
                        break

            # Expected Generation
            total_expected_gen = 0
            try:
                project_data = expected_gen_data.get('projects', {}).get(project_id, {})
                if 'expectedGeneration' in project_data:
                    total_expected_gen = project_data['expectedGeneration']
                else:
                    for date_key, data in project_data.get('aggregatedTotals', {}).items():
                        if date_key.startswith(start_date[:7]):
                            total_expected_gen += data.get('expectedGeneration', 0)
                total_expected_gen /= 1000
            except Exception:
                pass

            # Actual Generation
            total_actual_gen = calculate_total_actual_generation(energy_data)
            logger.info(f"Total actual generation: {total_actual_gen} kWh")

            # Irradiance
            actual_insolation = calculate_actual_insolation(expected_gen_data, project_details, project_id)
            logger.info(f"Calculated actual insolation: {actual_insolation} kWh/m²")
            logger.info(f"Actual insolation: {actual_insolation} kWh/m²")
            last_day = calendar.monthrange(report_date_obj.year, report_date_obj.month)[1]
            forecast_insolation = monthly_irradiance.get(month_name, 0) * last_day if monthly_irradiance else 0
            insolation_variance_pct = ((actual_insolation - forecast_insolation) / forecast_insolation) * 100 if forecast_insolation else 0

            # Weather adjusted forecast
            weather_normalized_expected = forecast_generation * (actual_insolation / forecast_insolation) if forecast_insolation and actual_insolation else total_expected_gen
            weather_normalized_variance = total_actual_gen - weather_normalized_expected

            # Financials
            actual_revenue = total_actual_gen * ppa_rate
            expected_revenue = total_expected_gen * ppa_rate
            revenue_variance = actual_revenue - expected_revenue

            actual_recs = total_actual_gen / 1000
            expected_recs = total_expected_gen / 1000
            rec_rate_display = rec_rate * 1000
            actual_rec_revenue = actual_recs * rec_rate_display
            expected_rec_revenue = expected_recs * rec_rate_display
            rec_revenue_variance = actual_rec_revenue - expected_rec_revenue

            total_actual_revenue = actual_revenue + actual_rec_revenue
            total_expected_revenue = expected_revenue + expected_rec_revenue
            total_revenue_variance = total_actual_revenue - total_expected_revenue

            # Write data row
            writer.writerow({
                'Project Code': project_id,
                'Project Name': project_name,
                'State': project_state,
                'COD': cod_date,
                'Size (kW)': format_number(system_size),
                'Actual Generation (kWh)': format_number(total_actual_gen),
                'Expected Generation (kWh)': format_number(total_expected_gen),
                'Forecast Generation (kWh)': format_number(forecast_generation),
                'Variance with Expected Generation (%)': f"{gen_variance_pct:.2f}%",
                'Availability Loss (kWh)': format_number(lost_gen_availability),
                'Actual + Availability Loss (kWh)': format_number(total_gen),
                'Total Variance with Expected Generation (%)': f"{combined_variance_pct:.2f}%",
                'Actual Insolation (kWh/m2)': format_number(actual_insolation),
                'Forecast Insolation (kWh/m2)': format_number(forecast_insolation),
                'Variance Insolation (%)': f"{insolation_variance_pct:.2f}%",
                'Weather Adjusted Forecast Generation (kWh)': format_number(weather_normalized_expected),
                'Weather Adjusted Generation Variance (kWh)': format_number(weather_normalized_variance),
                'Av PPA Price ($/kWh)': format_number(ppa_rate),
                'Anticipated PPA Revenue ($)': format_number(actual_revenue),
                'Expected PPA Revenue ($)': format_number(expected_revenue),
                'PPA Revenue Variance ($)': format_number(revenue_variance),
                'Av REC Sale Price ($/MWh)': format_number(rec_rate_display),
                'Actual RECs Generated': format_number(actual_recs),
                'Expected RECs': format_number(expected_recs),
                'Anticipated RECs Revenue ($)': format_number(actual_rec_revenue),
                'Expected RECs Revenue ($)': format_number(expected_rec_revenue),
                'REC Revenue Variance ($)': format_number(rec_revenue_variance),
                'Total Anticipated Revenue ($)': format_number(total_actual_revenue),
                'Total Expected Revenue ($)': format_number(total_expected_revenue),
                'Total Revenue Variance ($)': format_number(total_revenue_variance)
            })
            # Write a blank row for spacing
            writer.writerow({})

            # Write totals row (same as single row, but labeled)
            writer.writerow({
                'Project Code': '',
                'Project Name': 'TOTALS',
                'State': '',
                'COD': '',
                'Size (kW)': format_number(system_size),
                'Actual Generation (kWh)': format_number(total_actual_gen),
                'Expected Generation (kWh)': format_number(total_expected_gen),
                'Forecast Generation (kWh)': format_number(forecast_generation),
                'Variance with Expected Generation (%)': f"{gen_variance_pct:.2f}%",
                'Availability Loss (kWh)': format_number(lost_gen_availability),
                'Actual + Availability Loss (kWh)': format_number(total_gen),
                'Total Variance with Expected Generation (%)': f"{combined_variance_pct:.2f}%",
                'Actual Insolation (kWh/m2)': format_number(actual_insolation),
                'Forecast Insolation (kWh/m2)': format_number(forecast_insolation),
                'Variance Insolation (%)': f"{insolation_variance_pct:.2f}%",
                'Weather Adjusted Forecast Generation (kWh)': format_number(weather_normalized_expected),
                'Weather Adjusted Generation Variance (kWh)': format_number(weather_normalized_variance),
                'Av PPA Price ($/kWh)': '',  # Leave empty for totals
                'Anticipated PPA Revenue ($)': format_number(actual_revenue),
                'Expected PPA Revenue ($)': format_number(expected_revenue),
                'PPA Revenue Variance ($)': format_number(revenue_variance),
                'Av REC Sale Price ($/MWh)': '',
                'Actual RECs Generated': format_number(actual_recs),
                'Expected RECs': format_number(expected_recs),
                'Anticipated RECs Revenue ($)': format_number(actual_rec_revenue),
                'Expected RECs Revenue ($)': format_number(expected_rec_revenue),
                'REC Revenue Variance ($)': format_number(rec_revenue_variance),
                'Total Anticipated Revenue ($)': format_number(total_actual_revenue),
                'Total Expected Revenue ($)': format_number(total_expected_revenue),
                'Total Revenue Variance ($)': format_number(total_revenue_variance)
            })

            logger.info(f"CSV report generated successfully: {csv_path}")
            return csv_path

    except Exception as e:
        logger.error(f"Error generating CSV report: {e}", exc_info=True)
        return None




def export_project_summary(successful_projects, failed_projects, filename="project_status_summary.csv"):
    with open(filename, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Project ID", "Status"])
        
        for proj in successful_projects:
            writer.writerow([proj, "Success"])
        for proj in failed_projects:
            writer.writerow([proj, "Failed"])
        
    print(f"Project summary report saved to: {filename}")


# Modify the process_bulk_projects function to add a totals row in the consolidated report

def process_bulk_projects(project_codes: List[str], start_date: str, end_date: str, aggregation: str, 
                        debugger: EcosuiteDataExtractorDebugger, token: str, csv_input_filename: str = None) -> bool:
    try:
        # Create output filename
        if csv_input_filename:
            base_filename = os.path.basename(csv_input_filename)
            base_filename = os.path.splitext(base_filename)[0]
            consolidated_csv_filename = f"{base_filename}_{start_date}_{end_date}_consolidated_report.csv"
        else:
            consolidated_csv_filename = f"consolidated_report_{start_date}_{end_date}.csv"
        
        with open(consolidated_csv_filename, 'w', newline='') as consolidated_file:
            # Define CSV headers
            headers = [
                'Project Code', 'Project Name', 'State', 'COD', 'Size (kW)',
                'Actual Generation (kWh)', 'Expected Generation (kWh)', 'Forecast Generation (kWh)',
                'Variance with Expected Generation (%)', 'Availability Loss (kWh)', 'Actual + Availability Loss (kWh)',
                'Total Variance with Expected Generation (%)', 'Actual Insolation (kWh/m2)', 'Forecast Insolation (kWh/m2)',
                'Variance Insolation (%)', 'Weather Adjusted Forecast Generation (kWh)', 'Weather Adjusted Generation Variance (kWh)',
                'Av PPA Price ($/kWh)', 'Anticipated PPA Revenue ($)', 'Expected PPA Revenue ($)', 'PPA Revenue Variance ($)',
                'Av REC Sale Price ($/MWh)', 'Actual RECs Generated', 'Expected RECs', 'Anticipated RECs Revenue ($)',
                'Expected RECs Revenue ($)', 'REC Revenue Variance ($)', 'Total Anticipated Revenue ($)',
                'Total Expected Revenue ($)', 'Total Revenue Variance ($)'
            ]

            import csv
            consolidated_writer = csv.DictWriter(consolidated_file, fieldnames=headers)
            consolidated_writer.writeheader()
            
            # Process each project
            successful_projects = []
            failed_projects = []
            
            # Initialize totals for all numeric fields
            totals = {
                'size': 0,
                'actual_gen': 0,
                'expected_gen': 0,
                'forecast_gen': 0,
                'availability_loss': 0,
                'actual_plus_availability': 0,
                'actual_insolation': 0,
                'forecast_insolation': 0,
                'weather_adjusted_forecast': 0,
                'weather_adjusted_variance': 0,
                'anticipated_revenue': 0,
                'expected_revenue': 0,
                'revenue_variance': 0,
                'actual_recs': 0,
                'expected_recs': 0,
                'actual_rec_revenue': 0,
                'expected_rec_revenue': 0,
                'rec_revenue_variance': 0,
                'total_anticipated_revenue': 0,
                'total_expected_revenue': 0,
                'total_revenue_variance': 0,
                # Sum for calculating weighted averages
                'weighted_ppa_rate_sum': 0,
                'weighted_rec_rate_sum': 0,
                'project_count': 0
            }
            
            for project_id in project_codes:
                print(f"\nProcessing project: {project_id}")
                
                try:
                    # Fetch project details
                    project_details = fetch_project_details(project_id, token, debugger)
                    if not project_details:
                        logger.error(f"Failed to fetch project details for project {project_id}")

                        # Try to get minimal fallback data if possible
                        project_name = ''
                        project_state = ''
                        cod_date = ''
                        ppa_rate = ''

                        # Try again to fetch at least the basic info
                        try:
                            project_details = fetch_project_details(project_id, token, debugger)
                            if project_details:
                                project_name = project_details.get('project', {}).get('name', '')
                                project_state = project_details.get('project', {}).get('state', '')
                                cod_date = project_details.get('project', {}).get('milestones', {}).get('operational', '')
                                if not cod_date:
                                    cod_date = project_details.get('project', {}).get('productionStartDate', '')

                                price_data = fetch_price_data(project_id, token, debugger)
                                if price_data and 'proForma' in price_data and 'cashFlows' in price_data['proForma']:
                                    for cashflow in price_data['proForma']['cashFlows']:
                                        if cashflow.get('category') == 'Income' and cashflow.get('account') == 'PPA/FIT':
                                            for payment in cashflow.get('payments', []):
                                                if 'recurrence' in payment and 'startRate' in payment['recurrence']:
                                                    ppa_rate = payment['recurrence']['startRate']
                                                    break
                        except Exception as fallback_error:
                            logger.warning(f"Could not retrieve basic info for failed project {project_id}: {fallback_error}")

                        # Write to consolidated CSV with blanks for all other fields
                        # Log and flush for confirmation
                        print(f"[INFO] Writing fallback row for failed project: {project_id}")

                        # Update the fallback row structure
                        fallback_row = dict.fromkeys(headers, '')
                        # Remove the Start/End date fields from fallback_row update
                        fallback_row.update({
                            'Project Code': project_id,
                            'Project Name': project_name,
                            'State': project_state,
                            'COD': cod_date
                        })

                        # Write the row and flush to disk
                        consolidated_writer.writerow(fallback_row)
                        consolidated_file.flush()



                        failed_projects.append(project_id)
                        continue

                    
                    # Create project folder
                    project_name = project_details.get('project', {}).get('name', project_id)
                    raw_data_folder = create_project_folder(project_name, start_date, end_date)
                    
                    # Save project details
                    save_to_json("project_details.json", project_details, project_id, start_date, end_date, raw_data_folder)
                    
                    # Fetch and save price data
                    price_data = fetch_price_data(project_id, token, debugger)
                    if price_data:
                        save_to_json("price_data.json", price_data, project_id, start_date, end_date, raw_data_folder)
                    
                    # Fetch and save energy datums
                    energy_data = fetch_ecosuite_energy_datums(
                        project_id, 
                        start_date, 
                        end_date, 
                        aggregation.lower(), 
                        token, 
                        raw_data_folder,
                        debugger
                    )
                    
                    # Fetch and save expected generation with project IDs
                    expected_gen_data = fetch_expected_generation_with_project_ids(
                        project_id,
                        start_date,
                        end_date,
                        aggregation.lower(),
                        token,
                        raw_data_folder,
                        debugger
                    )
                    
                    forecast_data = fetch_forecast_generation(project_id, start_date, end_date, token, raw_data_folder)
                    forecast_generation = aggregate_forecast_generation(forecast_data)

                    # Fetch Ecosuite weather data
                    ecosuite_weather = fetch_ecosuite_weather_datums(
                        project_id, 
                        start_date, 
                        end_date, 
                        aggregation.lower(), 
                        token, 
                        raw_data_folder,
                        debugger
                    )
                    
                    # Fetch SolarNetwork weather data
                    solarnetwork_weather = fetch_solarnetwork_weather_data(
                        project_id,
                        project_details,
                        start_date,
                        end_date,
                        aggregation,
                        raw_data_folder,
                        debugger
                    )
                    
                    # Save a metadata file with info about fetched data
                    metadata = {
                        "project_id": project_id,
                        "project_name": project_name,
                        "start_date": start_date,
                        "end_date": end_date,
                        "aggregation": aggregation,
                        "data_fetched": {
                            "project_details": project_details is not None,
                            "price_data": price_data is not None,
                            "energy_data": energy_data is not None,
                            "expected_generation_with_ids": expected_gen_data is not None,
                            "ecosuite_weather": ecosuite_weather is not None,
                            "solarnetwork_weather": solarnetwork_weather is not None
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    save_to_json("metadata.json", metadata, project_id, start_date, end_date, raw_data_folder)
                    
                    # Generate data for the project
                    project_data = generate_project_data(
                        project_id,
                        start_date,
                        end_date,
                        raw_data_folder,
                        aggregation
                    )
                    
                    if project_data:
                        # Add row to consolidated CSV
                        consolidated_writer.writerow(project_data)
                        successful_projects.append(project_id)
                        
                        # Extract numeric values from the project data and add to totals
                        try:
                            # System size
                            size_str = project_data.get('Size (kW)', '0').replace(',', '')
                            size = float(size_str) if size_str else 0
                            totals['size'] += size
                            
                            # Generation data
                            actual_gen_str = project_data.get('Actual Generation (kWh)', '0').replace(',', '')
                            expected_gen_str = project_data.get('Expected Generation (kWh)', '0').replace(',', '')
                            forecast_gen_str = project_data.get('Forecast Generation (kWh)', '0').replace(',', '')
                            
                            actual_gen = float(actual_gen_str) if actual_gen_str else 0
                            expected_gen = float(expected_gen_str) if expected_gen_str else 0
                            forecast_gen = float(forecast_gen_str) if forecast_gen_str else 0
                            
                            totals['actual_gen'] += actual_gen
                            totals['expected_gen'] += expected_gen
                            totals['forecast_gen'] += forecast_gen
                            
                            # Availability and losses
                            availability_loss_str = project_data.get('Availability Loss (kWh)', '0').replace(',', '')
                            actual_plus_avail_str = project_data.get('Actual + Availability Loss (kWh)', '0').replace(',', '')
                            
                            availability_loss = float(availability_loss_str) if availability_loss_str else 0
                            actual_plus_avail = float(actual_plus_avail_str) if actual_plus_avail_str else 0
                            
                            totals['availability_loss'] += availability_loss
                            totals['actual_plus_availability'] += actual_plus_avail
                            
                            # Insolation data
                            actual_insolation_str = project_data.get('Actual Insolation (kWh/m2)', '0').replace(',', '')
                            forecast_insolation_str = project_data.get('Forecast Insolation (kWh/m2)', '0').replace(',', '')
                            
                            actual_insolation = float(actual_insolation_str) if actual_insolation_str else 0
                            forecast_insolation = float(forecast_insolation_str) if forecast_insolation_str else 0
                            
                            totals['actual_insolation'] += actual_insolation
                            totals['forecast_insolation'] += forecast_insolation
                            
                            # Weather adjusted data
                            weather_adjusted_forecast_str = project_data.get('Weather Adjusted Forecast Generation (kWh)', '0').replace(',', '')
                            weather_adjusted_variance_str = project_data.get('Weather Adjusted Generation Variance (kWh)', '0').replace(',', '')
                            
                            weather_adjusted_forecast = float(weather_adjusted_forecast_str) if weather_adjusted_forecast_str else 0
                            weather_adjusted_variance = float(weather_adjusted_variance_str) if weather_adjusted_variance_str else 0
                            
                            totals['weather_adjusted_forecast'] += weather_adjusted_forecast
                            totals['weather_adjusted_variance'] += weather_adjusted_variance
                            
                            # PPA data
                            ppa_rate_str = project_data.get('Av PPA Price ($/kWh)', '0').replace(',', '')
                            anticipated_revenue_str = project_data.get('Anticipated PPA Revenue ($)', '0').replace(',', '')
                            expected_revenue_str = project_data.get('Expected PPA Revenue ($)', '0').replace(',', '')
                            revenue_variance_str = project_data.get('PPA Revenue Variance ($)', '0').replace(',', '')
                            
                            ppa_rate = float(ppa_rate_str) if ppa_rate_str else 0
                            anticipated_revenue = float(anticipated_revenue_str) if anticipated_revenue_str else 0
                            expected_revenue = float(expected_revenue_str) if expected_revenue_str else 0
                            revenue_variance = float(revenue_variance_str) if revenue_variance_str else 0
                            
                            # For weighted average calculation of PPA rate
                            totals['weighted_ppa_rate_sum'] += ppa_rate * size
                            
                            totals['anticipated_revenue'] += anticipated_revenue
                            totals['expected_revenue'] += expected_revenue
                            totals['revenue_variance'] += revenue_variance
                            
                            # REC data
                            rec_rate_str = project_data.get('Av REC Sale Price ($/MWh)', '0').replace(',', '')
                            actual_recs_str = project_data.get('Actual RECs Generated', '0').replace(',', '')
                            expected_recs_str = project_data.get('Expected RECs', '0').replace(',', '')
                            actual_rec_revenue_str = project_data.get('Anticipated RECs Revenue ($)', '0').replace(',', '')
                            expected_rec_revenue_str = project_data.get('Expected RECs Revenue ($)', '0').replace(',', '')
                            rec_revenue_variance_str = project_data.get('REC Revenue Variance ($)', '0').replace(',', '')
                            
                            rec_rate = float(rec_rate_str) if rec_rate_str else 0
                            actual_recs = float(actual_recs_str) if actual_recs_str else 0
                            expected_recs = float(expected_recs_str) if expected_recs_str else 0
                            actual_rec_revenue = float(actual_rec_revenue_str) if actual_rec_revenue_str else 0
                            expected_rec_revenue = float(expected_rec_revenue_str) if expected_rec_revenue_str else 0
                            rec_revenue_variance = float(rec_revenue_variance_str) if rec_revenue_variance_str else 0
                            
                            # For weighted average calculation of REC rate
                            totals['weighted_rec_rate_sum'] += rec_rate * actual_recs
                            
                            totals['actual_recs'] += actual_recs
                            totals['expected_recs'] += expected_recs
                            totals['actual_rec_revenue'] += actual_rec_revenue
                            totals['expected_rec_revenue'] += expected_rec_revenue
                            totals['rec_revenue_variance'] += rec_revenue_variance
                            
                            # Total revenue
                            total_anticipated_revenue_str = project_data.get('Total Anticipated Revenue ($)', '0').replace(',', '')
                            total_expected_revenue_str = project_data.get('Total Expected Revenue ($)', '0').replace(',', '')
                            total_revenue_variance_str = project_data.get('Total Revenue Variance ($)', '0').replace(',', '')
                            
                            total_anticipated_revenue = float(total_anticipated_revenue_str) if total_anticipated_revenue_str else 0
                            total_expected_revenue = float(total_expected_revenue_str) if total_expected_revenue_str else 0
                            total_revenue_variance = float(total_revenue_variance_str) if total_revenue_variance_str else 0
                            
                            totals['total_anticipated_revenue'] += total_anticipated_revenue
                            totals['total_expected_revenue'] += total_expected_revenue
                            totals['total_revenue_variance'] += total_revenue_variance
                            
                            # Increment project count for averages
                            totals['project_count'] += 1
                            
                        except Exception as e:
                            logger.error(f"Error extracting numeric values from project data: {e}")
                    else:
                        failed_projects.append(project_id)
                    
                except Exception as e:
                    logger.error(f"Error processing project {project_id}: {e}")
                    failed_projects.append(project_id)
            
            # Add a blank row for separation
            consolidated_writer.writerow({})
            
            # Calculate averages and percentages for the totals row
            if totals['project_count'] > 0:
                avg_ppa_rate = totals['weighted_ppa_rate_sum'] / totals['size'] if totals['size'] > 0 else 0
                avg_rec_rate = totals['weighted_rec_rate_sum'] / totals['actual_recs'] if totals['actual_recs'] > 0 else 0
                
                # Calculate overall variance percentages
                variance_pct = ((totals['actual_gen'] - totals['expected_gen']) / totals['expected_gen'] * 100) if totals['expected_gen'] > 0 else 0
                total_variance_pct = ((totals['actual_plus_availability'] - totals['expected_gen']) / totals['expected_gen'] * 100) if totals['expected_gen'] > 0 else 0
                insolation_variance_pct = ((totals['actual_insolation'] - totals['forecast_insolation']) / totals['forecast_insolation'] * 100) if totals['forecast_insolation'] > 0 else 0
                
                # Update the totals row structure
                consolidated_writer.writerow({
                    'Project Code': '',
                    'Project Name': 'TOTALS',
                    'State': '',
                    'COD': '',
                    'Size (kW)': format_number(totals['size']),
                    'Actual Generation (kWh)': format_number(totals['actual_gen']),
                    'Expected Generation (kWh)': format_number(totals['expected_gen']),
                    'Forecast Generation (kWh)': format_number(totals['forecast_gen']),
                    'Variance with Expected Generation (%)': f"{variance_pct:.2f}%",
                    'Availability Loss (kWh)': format_number(totals['availability_loss']),
                    'Actual + Availability Loss (kWh)': format_number(totals['actual_plus_availability']),
                    'Total Variance with Expected Generation (%)': f"{total_variance_pct:.2f}%",
                    'Actual Insolation (kWh/m2)': format_number(totals['actual_insolation']),
                    'Forecast Insolation (kWh/m2)': format_number(totals['forecast_insolation']),
                    'Variance Insolation (%)': f"{insolation_variance_pct:.2f}%",
                    'Weather Adjusted Forecast Generation (kWh)': format_number(totals['weather_adjusted_forecast']),
                    'Weather Adjusted Generation Variance (kWh)': format_number(totals['weather_adjusted_variance']),
                    'Av PPA Price ($/kWh)': format_number(avg_ppa_rate),
                    'Anticipated PPA Revenue ($)': format_number(totals['anticipated_revenue']),
                    'Expected PPA Revenue ($)': format_number(totals['expected_revenue']),
                    'PPA Revenue Variance ($)': format_number(totals['revenue_variance']),
                    'Av REC Sale Price ($/MWh)': format_number(avg_rec_rate),
                    'Actual RECs Generated': format_number(totals['actual_recs']),
                    'Expected RECs': format_number(totals['expected_recs']),
                    'Anticipated RECs Revenue ($)': format_number(totals['actual_rec_revenue']),
                    'Expected RECs Revenue ($)': format_number(totals['expected_rec_revenue']),
                    'REC Revenue Variance ($)': format_number(totals['rec_revenue_variance']),
                    'Total Anticipated Revenue ($)': format_number(totals['total_anticipated_revenue']),
                    'Total Expected Revenue ($)': format_number(totals['total_expected_revenue']),
                    'Total Revenue Variance ($)': format_number(totals['total_revenue_variance'])
                })
            
            # Print summary
            print("\n--- Processing Summary ---")
            print(f"Total Projects: {len(project_codes)}")
            print(f"Successful Projects: {len(successful_projects)} ({successful_projects})")
            print(f"Failed Projects: {len(failed_projects)} ({failed_projects})")
            print(f"Report saved to: {consolidated_csv_filename}")
            
            return len(failed_projects) == 0
            
    except Exception as e:
        logger.error(f"An unexpected error occurred in bulk processing: {str(e)}", exc_info=True)
        debugger.log_error("Bulk Project Processing", e)
        return False


def merge_csv_files_to_excel(csv_files: List[str], output_excel_path: str) -> bool:
    """
    Merge multiple CSV files into a single Excel file with multiple sheets
    
    Args:
        csv_files (List[str]): List of paths to CSV files
        output_excel_path (str): Path to save the Excel file
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not csv_files:
        logger.error("No CSV files provided to merge")
        print("No CSV files provided to merge")
        return False
    
    try:
        # First check that all CSV files exist
        missing_files = []
        for csv_file in csv_files:
            if not os.path.exists(csv_file):
                missing_files.append(csv_file)
                logger.error(f"CSV file does not exist: {csv_file}")
                print(f"CSV file does not exist: {csv_file}")
        
        if missing_files:
            logger.error(f"Some CSV files are missing: {missing_files}")
            print(f"Some CSV files are missing: {missing_files}")
            # Remove missing files from the list
            for missing_file in missing_files:
                csv_files.remove(missing_file)
        
        if not csv_files:
            logger.error("No valid CSV files to merge")
            print("No valid CSV files to merge")
            return False
        
        # Make sure directory exists
        reports_folder = create_reports_folder()
        # If output_excel_path doesn't include the reports folder, add it
        if not output_excel_path.startswith(reports_folder):
            output_excel_path = os.path.join(reports_folder, os.path.basename(output_excel_path))
            
        os.makedirs(os.path.dirname(output_excel_path), exist_ok=True)
        
        # Create an Excel writer with xlsxwriter
        import xlsxwriter
        workbook = xlsxwriter.Workbook(output_excel_path)
        four_decimal_format = workbook.add_format({'num_format': '0.0000'})
        # Set default column width
        
        # Add each CSV file as a sheet
        for csv_file in csv_files:
            # Extract sheet name from filename (limited to 31 chars for Excel compatibility)
            import re
            sheet_name = os.path.basename(csv_file)
            sheet_name = re.sub(r'_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}_consolidated_report\.csv$', '', sheet_name)
           
            sheet_name = sheet_name[:31]  # Excel sheet name limit

            # Limit sheet name to 31 characters (Excel limitation)
            sheet_name = sheet_name[:31]
            
            try:
                # Read first few lines to detect metadata header
                with open(csv_file, 'r') as f:
                    header_lines = []
                    line_count = 0
                    for line in f:
                        header_lines.append(line)
                        line_count += 1
                        if line_count >= 5:  # Check first 5 lines
                            break
                
                # Determine if there's a metadata header by checking for empty lines
                skip_rows = 0
                for line in header_lines:
                    if line.strip() == "" or line.startswith("CSV file generated") or line.startswith("For documentation"):
                        skip_rows += 1
                    else:
                        break
                
                # Read CSV, skipping metadata header if present
                with open(csv_file, 'r') as f:
                    # Skip metadata header lines
                    for _ in range(skip_rows):
                        next(f)
                    
                    # Use csv module to parse the file
                    import csv
                    reader = csv.reader(f)
                    headers = next(reader)  # Get headers
                    
                    # Create worksheet
                    worksheet = workbook.add_worksheet(sheet_name)
                    
                    # Add header formatting
                    header_format = workbook.add_format({
                        'bold': True,
                        'text_wrap': True,
                        'valign': 'top',
                        'bg_color': '#D9D9D9',
                        'border': 1
                    })
                    
                    # Write headers
                    for col_num, header in enumerate(headers):
                        worksheet.write(0, col_num, header, header_format)
                    
                    # Write data rows
                    for row_num, row in enumerate(reader, start=1):
                        for col_num, cell in enumerate(row):
                            try:
                                val = float(cell.replace(',', '').replace('$', '').replace('%', ''))
                                worksheet.write_number(row_num, col_num, round(val, 4), four_decimal_format)
                            except:
                                worksheet.write(row_num, col_num, cell)

                    
                    # Auto-adjust column widths based on content
                    for col_num, header in enumerate(headers):
                        # Set width based on column header length
                        worksheet.set_column(col_num, col_num, len(header) + 2)
                
                logger.info(f"Added {csv_file} as sheet '{sheet_name}'")
                print(f"Added {csv_file} as sheet '{sheet_name}'")
                
            except Exception as e:
                logger.error(f"Error adding {csv_file} to Excel: {e}")
                print(f"Error adding {csv_file} to Excel: {e}")
                continue
        
        # Add a summary sheet
        try:
            create_summary_sheet_xlsxwriter(csv_files, workbook)
        except Exception as e:
            logger.error(f"Error creating summary sheet: {e}")
            print(f"Error creating summary sheet: {e}")
        
        # Save the Excel file
        workbook.close()
        
        logger.info(f"Excel file saved to {output_excel_path}")
        print(f"Excel file saved to {output_excel_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating Excel file: {e}")
        print(f"Error creating Excel file: {e}")
        return False


import re

def create_summary_sheet_xlsxwriter(csv_files: List[str], workbook) -> bool:
    try:
        summary_sheet = workbook.add_worksheet('Summary')

        header_format = workbook.add_format({
            'bold': True, 'text_wrap': True, 'valign': 'top',
            'bg_color': '#D9D9D9', 'border': 1
        })
        total_format = workbook.add_format({
            'bold': True, 'bg_color': '#F2F2F2', 'border': 1
        })

        headers = [
            'Portfolio', 'Number of Projects', 'Total Size (kW)',
            'Total Actual Generation (kWh)', 'Total Expected Generation (kWh)',
            'Overall Variance (%)', 'Total Actual Revenue ($)',
            'Total Expected Revenue ($)', 'Total Revenue Variance ($)'
        ]

        for col, title in enumerate(headers):
            summary_sheet.write(0, col, title, header_format)
        summary_sheet.set_column(0, 0, 35)

        row_num = 1
        grand = {'projects': 0, 'size': 0, 'actual': 0, 'expected': 0, 'a_rev': 0, 'e_rev': 0, 'r_var': 0}

        for csv_file in csv_files:
            with open(csv_file, 'r') as f:
                lines = f.readlines()

            # Skip only metadata lines
            content_lines = [l for l in lines if not l.startswith("CSV file generated") and not l.startswith("For documentation") and l.strip()]
            if not content_lines:
                continue

            reader = csv.DictReader(content_lines)
            project_count = 0
            totals_row = None

            for row in reader:
                if not any(row.values()):
                    continue
                name = row.get('Project Name', '').strip()
                if name.upper().startswith('TOTAL'):
                    totals_row = row
                elif name and name.upper() not in ['TOTAL', 'TOTALS']:
                    project_count += 1

            if not totals_row:
                continue

            file_name = os.path.basename(csv_file)
            portfolio_name = re.sub(r'_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}_consolidated_report\.csv$', '', file_name)

            def clean(val):
                try: return float(val.replace(',', '').replace('$', '').replace('%', ''))
                except: return 0.0

            size = clean(totals_row.get('Size (kW)', 0))
            actual = clean(totals_row.get('Actual Generation (kWh)', 0))
            expected = clean(totals_row.get('Expected Generation (kWh)', 0))
            variance_pct = clean(totals_row.get('Variance with Expected Generation (%)', '0'))
            a_rev = clean(totals_row.get('Total Anticipated Revenue ($)', 0))
            e_rev = clean(totals_row.get('Total Expected Revenue ($)', 0))
            r_var = clean(totals_row.get('Total Revenue Variance ($)', 0))

            grand['projects'] += project_count
            grand['size'] += size
            grand['actual'] += actual
            grand['expected'] += expected
            grand['a_rev'] += a_rev
            grand['e_rev'] += e_rev
            grand['r_var'] += r_var

            summary_sheet.write(row_num, 0, portfolio_name)
            summary_sheet.write(row_num, 1, project_count)
            summary_sheet.write(row_num, 2, size)
            summary_sheet.write(row_num, 3, actual)
            summary_sheet.write(row_num, 4, expected)
            summary_sheet.write(row_num, 5, variance_pct)
            summary_sheet.write(row_num, 6, a_rev)
            summary_sheet.write(row_num, 7, e_rev)
            summary_sheet.write(row_num, 8, r_var)
            row_num += 1

        # Grand total row
        overall_variance = ((grand['actual'] - grand['expected']) / grand['expected'] * 100) if grand['expected'] else 0
        summary_sheet.write(row_num + 1, 0, 'GRAND TOTAL', total_format)
        summary_sheet.write(row_num + 1, 1, grand['projects'], total_format)
        summary_sheet.write(row_num + 1, 2, grand['size'], total_format)
        summary_sheet.write(row_num + 1, 3, grand['actual'], total_format)
        summary_sheet.write(row_num + 1, 4, grand['expected'], total_format)
        summary_sheet.write(row_num + 1, 5, overall_variance, total_format)
        summary_sheet.write(row_num + 1, 6, grand['a_rev'], total_format)
        summary_sheet.write(row_num + 1, 7, grand['e_rev'], total_format)
        summary_sheet.write(row_num + 1, 8, grand['r_var'], total_format)

        print(f"✅ Summary sheet created with {row_num} portfolios.")
        return True

    except Exception as e:
        print(f"❌ Failed to create summary sheet: {e}")
        return False



def main():
    """
    Main function to orchestrate the data extraction process.
    """
    # Initialize debugger
    debugger = EcosuiteDataExtractorDebugger(None)
    
    try:
        # Use hardcoded values instead of reading files
        token = TOKEN
        start_date = START_DATE
        end_date = END_DATE
        project_codes = PROJECT_CODES
        
        # Set default aggregation to 'Day'
        aggregation = 'Day'
        
        # Process multiple projects
        success = process_bulk_projects(project_codes, start_date, end_date, aggregation, debugger, token)
        if success:
            print("\nAll projects processed successfully")
        else:
            print("\nSome projects failed to process. Check the summary for details.")
        
        # Export debug report
        debugger.export_debug_report()
        
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)
        debugger.log_error("Main Execution", e)
        print(f"\nAn error occurred: {str(e)}")
        print("Check the log file for details.")

if __name__ == "__main__":
    main()

import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
import calendar

# # Hardcoded inputs
# PROJECT_CODES = [
#     '0193','0194'
# ]

# Power BI passes parameter table as 'dataset'
TOKEN = dataset['Token'][0]
START_DATE = dataset['StartDate'][0]
END_DATE = dataset['EndDate'][0]
PROJECT_CODES = dataset['ProjectCodes'][0]  # already a list


# START_DATE = '2025-05-01'
# END_DATE = '2025-05-31'
# TOKEN = "eyJraWQiOiJRYjZZNEVhSVNseXJoTzdFaVFPeFJjbE0xaTdDTWZuRXQ2VXBaUGhUa21JPSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoibklsMUZBZmJzZ2NmOGRCbEJQZ01TUSIsInN1YiI6IjAyNWExYWQ2LWE4NDQtNGQ2MC1hMzRlLThhODMzYmQ5MDY5MSIsImN1c3RvbTpyZXN0cmljdFByb2plY3RzIjoibm8iLCJjdXN0b206dGltZXpvbmUiOiJBbWVyaWNhXC9OZXdfWW9yayIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJjdXN0b206bGFzdE5hbWUiOiJBZG1pbiIsImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC51cy1lYXN0LTEuYW1hem9uYXdzLmNvbVwvdXMtZWFzdC0xXzdCQkJoR2YxcCIsImN1c3RvbTpwcmV2ZW50RG93bmxvYWQiOiJubyIsImNvZ25pdG86dXNlcm5hbWUiOiIwMjVhMWFkNi1hODQ0LTRkNjAtYTM0ZS04YTgzM2JkOTA2OTEiLCJjdXN0b206c3RhcnREYXRlIjoiMjAyNC0xMi0xNiIsImN1c3RvbTpvcmdhbml6YXRpb25JZCI6Im9yZ2FuaXphdGlvbi04MDJlNjZlOS03YWU3LTQwYTktOTg4MC0xMjU3ZTUwZTNiZTIiLCJjdXN0b206dXNlclR5cGUiOiJ1c2VyLXR5cGUtZWY2NTMwMGQtNDQ5ZC00YmFiLTgzZDEtYmRiOWM0OTA4M2IwIiwiYXVkIjoiMW11bHNkZzZkMWlxY2djZGFkdm1iODVpMDgiLCJldmVudF9pZCI6Ijk5NDFlNzAxLTUxMzUtNDMwYy05NmQyLTdiYzcxYzFmODJmNCIsImN1c3RvbTpmaXJzdE5hbWUiOiJCYWxhLlNreXZpZXciLCJ0b2tlbl91c2UiOiJpZCIsImF1dGhfdGltZSI6MTc0NjMxMDUxNiwiZXhwIjoxNzQ5Mjg5OTY4LCJpYXQiOjE3NDkyODYzNjgsImVtYWlsIjoiYmFsYStza3l2aWV3X2FkbWluQGVjb3N1aXRlLmlvIn0.S3x8ppqTO_mZsFuDOUEaCwQVxbChhDcEHAMCaA5KZe0R2lXWP05Muru7z0Xd4bPV9DXw9qIdk-oXWNS3lSeuFXX2cpOgsWr-Uw6v2Gu4AiebOyKoxqyYRwJKGrN9RAnnksXNO_oqYM7DUuuC1cSNLxI3nQkUBl4LwaQuWG0v-0WRAE_6l-kyUguhRdSYFkt7sp-EyBl53L88xIBQQzgLI6CCIftABbNYlOpg07frBp6wwq5PQX3YZ2qy_B7Y-L_1VpOJO-q-Yv540XUL11g_YnKyHq2Uw0Xj-Ws-CWANM9BMuaCDyoCd3X0MG33v9Ylp4gwZy9y9DVWXRjFXYUtlJQ"

def adjust_end_date(end_date: str) -> str:
    """Adjust end date by adding one day to include the specified end date in results"""
    date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    adjusted_date = date_obj + timedelta(days=1)
    return adjusted_date.strftime("%Y-%m-%d")

def fetch_project_details(project_id: str, token: str) -> Optional[Dict]:
    """Fetch project details from Ecosuite API"""
    endpoint = f"https://api.ecosuite.io/projects/{project_id}"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(endpoint, headers=headers)
    return response.json() if response.status_code == 200 else None

def fetch_price_data(project_id: str, token: str) -> Optional[Dict]:
    """Fetch price data from Ecosuite API"""
    endpoint = f"https://api.ecosuite.io/projects/{project_id}/pro-forma"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(endpoint, headers=headers)
    return response.json() if response.status_code == 200 else None

def fetch_ecosuite_energy_datums(project_id: str, start_date: str, end_date: str, token: str) -> Optional[Dict]:
    """Fetch energy datums from Ecosuite API"""
    endpoint = f"https://api.ecosuite.io/energy/datums/projects/{project_id}"
    headers = {"Authorization": f"Bearer {token}"}
    
    adjusted_end_date = adjust_end_date(end_date)
    params = {
        "start": start_date,
        "end": adjusted_end_date,
        "aggregation": "day"
    }
    
    response = requests.get(endpoint, headers=headers, params=params)
    return response.json() if response.status_code == 200 else None

def fetch_expected_generation(project_id: str, start_date: str, end_date: str, token: str) -> Optional[Dict]:
    """Fetch expected generation data from the Ecosuite API"""
    base_url = "https://api.ecosuite.io/energy/datums/generation/expected"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Adjust end date to include the specified end date in results
    adjusted_end_date = adjust_end_date(end_date)
    
    params = {
        "start": start_date,
        "end": adjusted_end_date,
        "projectIds": project_id
    }
    
    try:
        response = requests.get(base_url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching expected generation: {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception in fetch_expected_generation: {str(e)}")
        return None

def fetch_forecast_generation(project_id: str, start_date: str, end_date: str, token: str) -> Optional[Dict]:
    """Fetch forecast generation from Ecosuite API"""
    adjusted_end = adjust_end_date(end_date)
    url = f"https://api.ecosuite.io/energy/datums/generation/predicted/projects/{project_id}"
    
    params = {
        "start": start_date,
        "end": adjusted_end,
        "aggregation": "day"
    }
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers, params=params)
    return response.json() if response.status_code == 200 else None

def process_project_data(project_id: str, start_date: str, end_date: str, token: str) -> Dict:
    """Process all data for a single project and return as dictionary"""
    project_details = fetch_project_details(project_id, token)
    price_data = fetch_price_data(project_id, token)
    energy_data = fetch_ecosuite_energy_datums(project_id, start_date, end_date, token)
    expected_gen_data = fetch_expected_generation(project_id, start_date, end_date, token)
    forecast_data = fetch_forecast_generation(project_id, start_date, end_date, token)

    # Parse date for month name
    report_date = datetime.strptime(start_date, '%Y-%m-%d')
    month_name = report_date.strftime('%B').lower()
    last_day = calendar.monthrange(report_date.year, report_date.month)[1]

    # Extract metadata
    project_name = project_details.get('project', {}).get('name', 'Unknown')
    project_state = project_details.get('project', {}).get('state', '')
    cod_date = project_details.get('project', {}).get('milestones', {}).get('operational', '') or \
               project_details.get('project', {}).get('productionStartDate', '')
    system_size = project_details.get('project', {}).get('dcSize', 0)

    # Calculate actual generation - updated to match main.py logic
    actual_gen = 0
    try:
        sites = energy_data.get('project', {}).get('sites', {})
        for site_id, site_data in sites.items():
            systems = site_data.get('systems', {})
            for system_id, system_data in systems.items():
                aggregated_totals = system_data.get('aggregatedTotals', {})
                for timestamp, values in aggregated_totals.items():
                    generation = values.get('generation', 0)
                    actual_gen += generation / 1000  # Convert Wh to kWh
        print(f"✅ Total actual generation: {actual_gen:,.2f} kWh")
    except Exception as e:
        print(f"❌ Error calculating actual generation: {str(e)}")

    # Calculate expected generation - fixed to match main.py logic exactly
    expected_gen = 0
    try:
        for date_key, data in expected_gen_data.get('projects', {}).get(project_id, {}).get('aggregatedTotals', {}).items():
            expected_gen += data.get('expectedGeneration', 0)
        
        # Convert from Wh to kWh
        expected_gen /= 1000
        print(f"✅ Total expected generation: {expected_gen:,.2f} kWh")
    except Exception as e:
        print(f"❌ Error calculating expected generation: {str(e)}")

    # Calculate forecast generation
    forecast_gen = 0
    try:
        sites = forecast_data.get('projectDatums', {}).get('sites', {})
        for site_data in sites.values():
            systems = site_data.get('systems', {})
            for system_data in systems.values():
                agg_totals = system_data.get('aggregatedTotals', {})
                for day_data in agg_totals.values():
                    forecast_gen += day_data.get('predictedGeneration', 0) / 1000
    except Exception as e:
        print(f"Error calculating forecast generation: {e}")

    # Calculate insolation values
    actual_insolation = 0
    forecast_insolation = 0
    try:
        sites = project_details.get('project', {}).get('sites', {})
        first_site = next(iter(sites.values()), {})
        systems = first_site.get('systems', {})
        first_system = next(iter(systems.values()), {})
        
        for values in expected_gen_data.get('values', []):
            actual_insolation += values.get('irradianceHours', 0) / 1000
            
        monthly_irradiance = first_system.get('forecast', {}).get('irradiance', {}).get('monthlyIrradiance', {})
        forecast_insolation = monthly_irradiance.get(month_name, 0) * last_day
    except Exception as e:
        print(f"Error calculating insolation: {e}")

    # Calculate variances
    gen_variance_pct = ((actual_gen - expected_gen) / expected_gen * 100) if expected_gen else 0
    
    # Calculate availability loss and total generation
    availability_loss = 0  # This would need actual availability data
    total_gen = actual_gen + availability_loss
    combined_variance_pct = ((total_gen - expected_gen) / expected_gen * 100) if expected_gen else 0
    
    # Calculate weather adjustments
    insolation_variance_pct = ((actual_insolation - forecast_insolation) / forecast_insolation * 100) if forecast_insolation else 0
    weather_normalized_expected = forecast_gen * (actual_insolation / forecast_insolation) if forecast_insolation and actual_insolation else expected_gen
    weather_normalized_variance = actual_gen - weather_normalized_expected

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
                            start_dt = datetime.strptime(rec.get('startDate', '2018-01-01'), '%Y-%m-%d')
                            years_diff = report_date.year - start_dt.year
                            if years_diff < len(rec['rates']):
                                rec_rate = rec['rates'][years_diff].get(month_name, 0)
                        except Exception:
                            pass

    # Calculate revenues
    actual_revenue = actual_gen * ppa_rate
    expected_revenue = expected_gen * ppa_rate
    revenue_variance = actual_revenue - expected_revenue

    actual_recs = actual_gen / 1000  # Convert to MWh
    expected_recs = expected_gen / 1000
    rec_rate_display = rec_rate * 1000  # Convert to $/MWh
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
        'Start Date': start_date,  # Added start date
        'End Date': end_date,      # Added end date
        'Size (kW)': system_size,
        'Actual Generation (kWh)': actual_gen,
        'Expected Generation (kWh)': expected_gen,
        'Forecast Generation (kWh)': forecast_gen,
        'Variance with Expected Generation (%)': gen_variance_pct,
        'Availability Loss (kWh)': availability_loss,
        'Actual + Availability Loss (kWh)': total_gen,
        'Total Variance with Expected Generation (%)': combined_variance_pct,
        'Actual Insolation (kWh/m2)': actual_insolation,
        'Forecast Insolation (kWh/m2)': forecast_insolation,
        'Variance Insolation (%)': insolation_variance_pct,
        'Weather Adjusted Forecast Generation (kWh)': weather_normalized_expected,
        'Weather Adjusted Generation Variance (kWh)': weather_normalized_variance,
        'Av PPA Price ($/kWh)': ppa_rate,
        'Anticipated PPA Revenue ($)': actual_revenue,
        'Expected PPA Revenue ($)': expected_revenue,
        'PPA Revenue Variance ($)': revenue_variance,
        'Av REC Sale Price ($/MWh)': rec_rate_display,
        'Actual RECs Generated': actual_recs,
        'Expected RECs': expected_recs,
        'Anticipated RECs Revenue ($)': actual_rec_revenue,
        'Expected RECs Revenue ($)': expected_rec_revenue,
        'REC Revenue Variance ($)': rec_revenue_variance,
        'Total Anticipated Revenue ($)': total_actual_revenue,
        'Total Expected Revenue ($)': total_expected_revenue,
        'Total Revenue Variance ($)': total_revenue_variance
    }

def main() -> pd.DataFrame:
    """Main function to process projects and return DataFrame"""
    all_data = []
    
    for project_id in PROJECT_CODES:
        try:
            project_data = process_project_data(project_id, START_DATE, END_DATE, TOKEN)
            all_data.append(project_data)
            
            # Add print statements to show generation values with dates
            print(f"\nProject {project_id} Generation Summary:")
            print(f"Period: {project_data['Start Date']} to {project_data['End Date']}")
            print(f"Actual Generation: {project_data['Actual Generation (kWh)']:,.2f} kWh")
            print(f"Expected Generation: {project_data['Expected Generation (kWh)']:,.2f} kWh")
            print(f"Forecast Generation: {project_data['Forecast Generation (kWh)']:,.2f} kWh")
            print(f"Variance with Expected: {project_data['Variance with Expected Generation (%)']}%")
            
        except Exception as e:
            print(f"Error processing project {project_id}: {str(e)}")
            continue
    
    df = pd.DataFrame(all_data)
    
    # Print DataFrame preview with date columns included
    print("\nDataFrame Preview:")
    print("\nShape:", df.shape)
    print("\nColumns:", list(df.columns))
    print("\nFirst few rows:")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df[['Project Code', 'Project Name', 'Start Date', 'End Date', 
              'Actual Generation (kWh)', 'Expected Generation (kWh)', 
              'Forecast Generation (kWh)', 'Variance with Expected Generation (%)']])
    
    return df

# Execute and return DataFrame for Power BI
df = main()
df  # Return DataFrame

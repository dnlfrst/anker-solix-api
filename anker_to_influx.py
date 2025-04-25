import asyncio
import os
from datetime import datetime, timedelta
import time

from aiohttp import ClientSession
from api import api
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

# Load and validate environment variables
ANKER_COUNTRY = os.getenv("ANKER_COUNTRY")
ANKER_EMAIL = os.getenv("ANKER_EMAIL")
ANKER_PASSWORD = os.getenv("ANKER_PASSWORD")

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")


def calculate_daily_stats(client, site_name, current_timestamp):
    """Calculate daily statistics from power measurements."""
    query_api = client.query_api()
    
    # Calculate start time as beginning of the current day in Unix timestamp
    start_time = datetime.fromtimestamp(current_timestamp).replace(hour=0, minute=0, second=0)
    start_timestamp = int(start_time.timestamp())
    
    # Query for total consumption (home_load_power)
    consumption_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
        |> range(start: {start_timestamp})
        |> filter(fn: (r) => r["_measurement"] == "anker_power")
        |> filter(fn: (r) => r["site_name"] == "{site_name}")
        |> filter(fn: (r) => r["_field"] == "home_load_power")
        |> integral(unit: 1h)
        |> yield(name: "daily_consumption")
    '''
    
    # Query for total generation (total_photovoltaic_power)
    generation_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
        |> range(start: {start_timestamp})
        |> filter(fn: (r) => r["_measurement"] == "anker_power")
        |> filter(fn: (r) => r["site_name"] == "{site_name}")
        |> filter(fn: (r) => r["_field"] == "total_photovoltaic_power")
        |> integral(unit: 1h)
        |> yield(name: "daily_generation")
    '''
    
    # Query for grid import (grid_to_home_power)
    grid_import_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
        |> range(start: {start_timestamp})
        |> filter(fn: (r) => r["_measurement"] == "anker_power")
        |> filter(fn: (r) => r["site_name"] == "{site_name}")
        |> filter(fn: (r) => r["_field"] == "grid_to_home_power")
        |> integral(unit: 1h)
        |> yield(name: "grid_import")
    '''
    
    try:
        consumption_result = query_api.query(org=INFLUX_ORG, query=consumption_query)
        generation_result = query_api.query(org=INFLUX_ORG, query=generation_query)
        grid_import_result = query_api.query(org=INFLUX_ORG, query=grid_import_query)
        
        # Extract values from results (default to 0 if no data)
        daily_consumption = float(consumption_result[0].records[0].get_value() if consumption_result else 0.0)
        daily_generation = float(generation_result[0].records[0].get_value() if generation_result else 0.0)
        grid_import = float(grid_import_result[0].records[0].get_value() if grid_import_result else 0.0)
        
        # Calculate self-consumed energy
        self_consumed = daily_consumption - grid_import
        
        # Calculate metrics
        autarky = float(self_consumed / daily_consumption * 100) if daily_consumption > 0 else 0.0
        self_consumption = float(self_consumed / daily_generation * 100) if daily_generation > 0 else 0.0

        return {
            'daily_consumption': daily_consumption,
            'daily_generation': daily_generation,
            'autarky': autarky,
            'self_consumption': self_consumption
        }
    except Exception as exception:
        print(f"Error calculating daily stats: {exception}")
        
        return None


async def fetch_anker_data():
    async with ClientSession() as web_session:
        anker_api = api.AnkerSolixApi(
            ANKER_EMAIL, ANKER_PASSWORD, ANKER_COUNTRY, web_session
        )

        await anker_api.update_sites()
        power_data = []
        
        for site in anker_api.sites.values():
            if 'solarbank_info' in site:
                info = site['solarbank_info']
                grid_info = site.get('grid_info', {})
                
                # Get battery info from first solarbank if available
                battery_charge_power = 0.0
                battery_percentage = 0.0
                if info['solarbank_list']:
                    first_bank = info['solarbank_list'][0]
                    battery_charge_power = float(first_bank['charging_power'])
                    battery_percentage = float(first_bank['battery_power'])

                timestamp = parse_timestamp(info['updated_time'])

                data = {
                    'site_name': site['site_info']['site_name'],
                    # Solar metrics
                    'total_photovoltaic_power': float(info['total_photovoltaic_power']),
                    'solar_power_1': float(info.get('solar_power_1', 0.0)),
                    'solar_power_2': float(info.get('solar_power_2', 0.0)),
                    'solar_power_3': float(info.get('solar_power_3', 0.0)),
                    'solar_power_4': float(info.get('solar_power_4', 0.0)),
                    # Battery metrics
                    'battery_percentage': battery_percentage,
                    'battery_charge_power': battery_charge_power,
                    # Home and grid metrics
                    'home_load_power': float(site.get('home_load_power', 0.0)),
                    'grid_to_home_power': float(grid_info.get('grid_to_home_power', 0.0)),
                    'photovoltaic_to_grid_power': float(grid_info.get('photovoltaic_to_grid_power', 0.0)),
                    'to_home_load': float(info.get('to_home_load', 0.0)),
                    'ac_power': float(info.get('ac_power', 0.0)),
                    'timestamp': timestamp
                }
                power_data.append(data)

        return power_data


def parse_timestamp(timestamp_str):
    """Parse timestamp and convert to Unix timestamp in seconds"""
    try:
        dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        return int(dt.timestamp())
    except ValueError:
        return int(datetime.now().timestamp())


def write_to_influx(data):
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    try:
        for site_data in data:
            # Write power metrics
            point = (
                Point("anker_power")
                .tag("site_name", site_data["site_name"])
                # Solar fields
                .field("total_photovoltaic_power", site_data["total_photovoltaic_power"])
                .field("solar_power_1", site_data["solar_power_1"])
                .field("solar_power_2", site_data["solar_power_2"])
                .field("solar_power_3", site_data["solar_power_3"])
                .field("solar_power_4", site_data["solar_power_4"])
                # Battery fields
                .field("battery_percentage", site_data["battery_percentage"])
                .field("battery_charge_power", site_data["battery_charge_power"])
                # Home and grid fields
                .field("home_load_power", site_data["home_load_power"])
                .field("grid_to_home_power", site_data["grid_to_home_power"])
                .field("photovoltaic_to_grid_power", site_data["photovoltaic_to_grid_power"])
                .field("to_home_load", site_data["to_home_load"])
                .field("ac_power", site_data["ac_power"])
                .time(site_data["timestamp"], WritePrecision.S)
            )
            
            write_api.write(
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
                record=point
            )

            # Calculate and write daily statistics
            daily_stats = calculate_daily_stats(client, site_data["site_name"], site_data["timestamp"])
            if daily_stats:
                stats_point = (
                    Point("anker_daily_stats")
                    .tag("site_name", site_data["site_name"])
                    .field("daily_consumption_kwh", daily_stats["daily_consumption"] / 1000)  # Convert to kWh
                    .field("daily_generation_kwh", daily_stats["daily_generation"] / 1000)    # Convert to kWh
                    .field("autarky_percent", daily_stats["autarky"])
                    .field("self_consumption_percent", daily_stats["self_consumption"])
                    .time(site_data["timestamp"], WritePrecision.S)
                )
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=stats_point)
                
    finally:
        client.close()


if __name__ == "__main__":
    start_time = time.time()
    delay = 9
    iterations = 0
    max_iterations = 6  # Run 6 times (every 9 seconds for one minute)
    
    while iterations < max_iterations:
        try:
            data = asyncio.run(fetch_anker_data())
            
            if data:
                write_to_influx(data)
                print(f"✅ Data successfully written to InfluxDB at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (iteration {iterations + 1}/{max_iterations})")
            else:
                print(f"No data received from Anker API (iteration {iterations + 1}/{max_iterations})")
        except Exception as error:
            print(f"❌ Error: {str(error)}")
        finally:
            iterations += 1
            
            if iterations < max_iterations:
                time.sleep(delay)
    
    elapsed = time.time() - start_time
    print(f"Completed {iterations} iterations in {elapsed:.1f} seconds")
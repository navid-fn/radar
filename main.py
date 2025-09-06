import argparse
import json
from confluent_kafka import Producer
import socket
import importlib
import os
import subprocess # Import the subprocess module

def run_scrape_and_produce_result(driver_name, url):
    """
    Dynamically loads and runs a Python or Go driver, executes the scrape, 
    and produces the result to Kafka.
    """
    
    scraped_data = None
    python_driver_path = f"drivers/{driver_name}.py"
    # Assumes the compiled Go binary has the same name as the driver
    go_driver_path = f"drivers/{driver_name}" 

    # --- 1. Find and Execute the Driver ---
    if os.path.exists(python_driver_path):
        # --- Handle Python Driver ---
        print(f"Found Python driver: {python_driver_path}")
        try:
            module_name = f"drivers.{driver_name}"
            driver_module = importlib.import_module(module_name)
            
            if hasattr(driver_module, 'scrape'):
                print(f"Executing Python scrape for URL: {url}")
                scraped_data = driver_module.scrape(url)
            else:
                print(f"ERROR: Driver '{driver_name}' does not have a 'scrape' function.")
                return
        except Exception as e:
            print(f"ERROR: An error occurred during the Python scraping process: {e}")
            return

    elif os.path.exists(go_driver_path):
        # --- Handle Go Driver ---
        print(f"Found Go driver executable: {go_driver_path}")
        try:
            command = [go_driver_path, f"--url={url}"]
            print(f"Executing Go driver with command: {' '.join(command)}")
            
            # Execute the compiled Go program as a subprocess
            result = subprocess.run(
                command, 
                capture_output=True, 
                text=True, 
                check=True  # This will raise an exception for non-zero exit codes
            )
            
            # The Go program's output (stdout) should be a JSON string
            scraped_data = json.loads(result.stdout)

        except subprocess.CalledProcessError as e:
            print(f"ERROR: Go driver '{driver_name}' exited with an error.")
            print(f"Stderr: {e.stderr}")
            return
        except json.JSONDecodeError:
            print(f"ERROR: Could not decode JSON from Go driver output.")
            return
        except Exception as e:
            print(f"ERROR: An unexpected error occurred while running the Go driver: {e}")
            return
    else:
        print(f"ERROR: No Python or Go driver found for '{driver_name}' in 'drivers/' directory.")
        return

    # --- 2. Process and Produce the Result ---
    if not scraped_data:
        print("Scrape completed but returned no data.")
        return
        
    print(f"Scrape successful. Data received: {scraped_data}")

    conf = {'bootstrap.servers': 'kafka:29092', 'client.id': socket.gethostname()}
    producer = Producer(conf)
    result_topic = 'scraped_data'

    try:
        message_bytes = json.dumps(scraped_data).encode('utf-8')
        producer.produce(result_topic, value=message_bytes)
        producer.flush()
        print(f"Successfully produced result to Kafka topic '{result_topic}'.")
    except Exception as e:
        print(f"ERROR: Failed to produce scrape result to Kafka: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scraping Runner and Worker")
    parser.add_argument('--driver', required=True, help="The name of the driver file (without .py or extension).")
    parser.add_argument('--url', required=True, help="The target URL to scrape.")
    
    args = parser.parse_args()
    print("hhiii")
    
    run_scrape_and_produce_result(args.driver, args.url)


"""
Test script for API models/generate
Uses HTTP requests directly to test the API
"""
import sys
import os
import requests
import json

def test_models_generate(table_name="STTM_CUSTOMER"):
    """Test the API models/generate endpoint"""
    url = "http://localhost:8000/api/models/generate"
    
    # Test payload
    payload = {
        "table_name": table_name,
        "use_ai_enhancement": True
    }
    
    print(f"Testing API: {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    # Call API
    response = requests.post(url, json=payload)
    
    # Print results
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        # Success case
        result = response.json()
        
        print("\nResponse headers:")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")
            
        print("\nResponse summary:")
        for key, value in result.items():
            if key != "model_yaml":  # Skip the large YAML
                print(f"  {key}: {value}")
                
        print("\nModel YAML preview (first 10 lines):")
        yaml_lines = result.get("model_yaml", "").split("\n")
        for i, line in enumerate(yaml_lines[:10]):
            print(f"  {i+1}: {line}")
            
        if len(yaml_lines) > 10:
            print(f"  ... and {len(yaml_lines) - 10} more lines")
            
        # Save full YAML to file
        output_file = f"{table_name}_generated_model.yaml"
        with open(output_file, "w") as f:
            f.write(result.get("model_yaml", ""))
        print(f"\nFull YAML saved to: {output_file}")
        
        return True
    else:
        # Error case
        print("\nError response:")
        try:
            print(json.dumps(response.json(), indent=2))
        except:
            print(response.text)
        
        return False

if __name__ == "__main__":
    # Get table name from command line
    table_name = sys.argv[1] if len(sys.argv) > 1 else "STTM_CUSTOMER"
    test_models_generate(table_name)

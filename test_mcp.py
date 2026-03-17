import json
import subprocess
import sys

def send_request(method, params=None):
    request = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params or [],
        "id": 1
    }
    
    proc = subprocess.Popen(
        ['./target/release/verus-mcp'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Send request and close stdin to signal end
    proc.stdin.write(json.dumps(request) + '\n')
    proc.stdin.close()
    
    # Read response
    stdout, stderr = proc.communicate()
    
    if proc.returncode != 0:
        print(f"Error: {stderr}", file=sys.stderr)
        return None
        
    try:
        return json.loads(stdout)
    except:
        print(f"Raw output: {stdout}", file=sys.stderr)
        return None

# Test search
print("Testing search for 'poly_inverse':")
result = send_request("search", ["poly_inverse"])
print(json.dumps(result, indent=2) if result else "No result")

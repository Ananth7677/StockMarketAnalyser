import json


def read_json(file_path):
    # Opening JSON file
    f = open(file_path)

    # returns JSON object as a dictionary
    return json.load(f)

def serialize_data(data, file_path):
    """
    Serializes data to a JSON file.
    
    Args:
        data: The data to serialize (e.g., dict, list)
        file_path: Path to the output JSON file
    Returns:
        bool: True if successful, False if an error occurs
    """
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        print(f"Serialization error: {e}")
        return False

def deserialize_data(file_path):
    """
    Deserializes data from a JSON file.
    
    Args:
        file_path: Path to the input JSON file
    Returns:
        The deserialized data (e.g., dict, list) or None if an error occurs
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Deserialization error: {e}")
        return None
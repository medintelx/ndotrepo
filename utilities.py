# Function to flatten the nested dictionary (JSON)
def flatten_dict(d, parent_key=''):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)



def chunk_list(data, chunk_size=100):
    """Divide a list into chunks of the given size."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

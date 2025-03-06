import notebookutils
def print_hello_world():
    # Get raw lakehouse data
    lakehouses = notebookutils.lakehouse.list()

    # Extract paths
    paths = {lh['displayName'].split('_')[-1].lower(): lh['properties']['abfsPath'] for lh in lakehouses}

    # Assign variables
    local_bronze_path = paths.get('bronze', '')
    local_silver_path = paths.get('silver', '')
    local_gold_path = paths.get('gold', '')

    print(local_bronze_path)
    print(local_silver_path)
    print(local_gold_path)
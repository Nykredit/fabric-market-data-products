import notebookutils

def mount_lakehouses():
    # Unmount all existing mounts
    existing_mounts = notebookutils.mssparkutils.fs.ls('/mnt/')
    for mount in existing_mounts:
        notebookutils.mssparkutils.fs.unmount(mount.path)
    
    # Get raw lakehouse data
    lakehouses = notebookutils.lakehouse.list()

    # Extract paths
    paths = {lh['displayName'].split('_')[-1].lower(): lh['properties']['abfsPath'] for lh in lakehouses}

    # Assign and mount lakehouses
    if 'bronze' in paths:
        notebookutils.mssparkutils.fs.mount(paths['bronze'], '/mnt/bronze')
    if 'silver' in paths:
        notebookutils.mssparkutils.fs.mount(paths['silver'], '/mnt/silver')
    if 'gold' in paths:
        notebookutils.mssparkutils.fs.mount(paths['gold'], '/mnt/gold')

    print("Lakehouses mounted successfully:")
    print("Bronze:", paths.get('bronze', 'Not found'))
    print("Silver:", paths.get('silver', 'Not found'))
    print("Gold:", paths.get('gold', 'Not found'))
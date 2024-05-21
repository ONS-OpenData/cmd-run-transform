import os


def ClearRepo():
    files_to_keep = ("clients.py", "main.py", "landing_pages.json", "upload_details.json", "README.md", "ashe_clients.py", "ashe_main.py")

    for item in os.listdir():
        if os.path.isdir(item):
            continue

        if item in files_to_keep:
            continue

        if item.startswith("."):
            continue

        if item.endswith(".py"):
            # catch in case further files are added in future
            print(f"Not deleting .py files - {item}")
            continue

        if item.endswith(".json"):
            # catch in case further files are added in future
            print(f"Not deleting .json files - {item}")
            continue

        os.remove(item)


    

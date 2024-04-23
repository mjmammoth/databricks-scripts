import sys
import os

def print_header(title):
    print(f"\n{'='*len(title)}\n{title}\n{'='*len(title)}")


def check_errors(response):
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        sys.exit()

def get_env_var(var_name: str) -> str:
    var_value = os.getenv(var_name)
    if var_value is None or var_value == "":
        raise ValueError(f"The environment variable {var_name} is not set.")
    return var_value

def choose_restore_point(rtl_env: str) -> str:
    base_dir = "restore_states"
    try:
        dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d)) and d.startswith(rtl_env)]
        # Sort directories by their modification time, most recent first
        dirs.sort(key=lambda x: os.path.getmtime(os.path.join(base_dir, x)), reverse=True)
    except FileNotFoundError:
        print("No restore states directory found.")
        sys.exit()

    if not dirs:
        print("No restore states found.")
        sys.exit()

    if len(dirs) > 1:
        print("Available restore points:")
        for index, dir in enumerate(dirs):
            print(f"{index + 1}: {dir}")

        choice = input(f"Enter the number of the restore point to revert to (default is 1, the latest): ")
        if choice.isdigit() and 1 <= int(choice) <= len(dirs):
            backup_folder = dirs[int(choice) - 1]
        else:
            backup_folder = dirs[0]
        print(f"Restoring to the selected restore point: {backup_folder}")
    else:
        backup_folder = dirs[0]
        print(f"Restoring to the only available restore point: {backup_folder}")

    return f"{base_dir}/{backup_folder}"

#!/usr/bin/env python3
import subprocess
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import argparse
from dotenv import load_dotenv
import sys
import datetime
import re

from workspace_features import WorkspacePermissions, Workflows, JobRuns, AllPurposeCompute, SQLWarehouses
from workspace_utils import choose_restore_point, get_env_var

def get_access_token() -> str:
    # The resource ID for Azure Databricks is static and will not change for any Azure Databricks instance
    command = [
        'az', 'account', 'get-access-token',
        '--resource', '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d',
        '--query', 'accessToken',
        '-o', 'tsv'
    ]
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        access_token = result.stdout.strip()
        return access_token
    except subprocess.CalledProcessError as e:
        print(f"Error fetching access token: {e.stderr}")
        sys.exit(1)

class DatabricksWorkspaceManager:
    def __init__(self, account_id: str, workspace_id: str, token: str, rtl_env: str, workspace_url: str, restore_path: str|None = None):
        self.account_id = account_id
        self.workspace_id = workspace_id
        self.token = token
        self.rtl_env = rtl_env
        self.account_url = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id}"
        self.workspace_url = workspace_url
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self.session = self.create_session()
        self.start_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        self.restore_path = restore_path

        self.workspace_permissions = WorkspacePermissions(self)
        self.workflows = Workflows(self)
        self.job_runs = JobRuns(self)
        self.all_purpose_compute = AllPurposeCompute(self)
        self.sql_warehouses = SQLWarehouses(self)

    def create_session(self):
        session = requests.Session()
        retries = Retry(total=5,
                        backoff_factor=1,
                        status_forcelist=[403, 429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        session.headers.update(self.headers)
        return session


    def show_environment(self, **kwargs):
        self.workspace_permissions.show()
        self.workflows.show(**kwargs)
        self.job_runs.show(**kwargs)
        self.all_purpose_compute.show(**kwargs)
        self.sql_warehouses.show(**kwargs)

    def halt_environment(self, ignored_principals=None):
        self.workspace_permissions.delete(ignored_principals)
        self.workflows.pause()
        self.all_purpose_compute.stop()
        self.job_runs.stop()
        self.sql_warehouses.stop()

    def restore_environment(self, restore_path):
        self.workspace_permissions.restore(restore_path)
        self.workflows.restore(restore_path)
        self.job_runs.restore(restore_path)
        self.all_purpose_compute.restore(restore_path)
        self.sql_warehouses.restore(restore_path)


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help="Environment to use", type=str, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")
    stop_parser = subparsers.add_parser("stop", help="Stop the workspace")
    stop_parser.add_argument("-i", "--ignored-principals", help="Principal IDs to ignore when permissions are deleted", type=int, nargs="+", required=True)
    restore_parser = subparsers.add_parser("restore", help="Restore the workspace")
    show_parser = subparsers.add_parser("show", help="Show all workspace objects")
    show_parser.add_argument("-a", "--active-only", help="Show only active workflows and job runs", action="store_true")

    args = parser.parse_args()

    with open(".env.json") as f:
        try:
            workspace_url = json.load(f)[args.env]
            print(f"Using workspace URL: {workspace_url}")
            re_match = re.search(r"adb-(\d+)\.", workspace_url)
            if re_match is None:
                raise ValueError("Workspace URL is not in the correct format")
            workspace_id = re_match.group(1)
        except FileNotFoundError:
            print(".env.json Environment file not found")
            sys.exit(1)
        except KeyError:
            print(f"Environment {args.env} not found in .env.json file")
            sys.exit(1)
        except ValueError as e:
            print(f"Error parsing workspace URL: {e}")
            print("Please ensure the workspace URL is in the format 'https://adb-<workspace_id>.<integer>.azuredatabricks.net'")
            sys.exit(1)
        except TypeError:
            print("Value for environment in .env.json file is not a URL string")
            print("Please ensure a URL in the format of 'https://adb-<workspace_id>.<integer>.azuredatabricks.net' is provided")
            sys.exit(1)

    workspace_manager = DatabricksWorkspaceManager(
        account_id=get_env_var("AZ_DATABRICKS_ACCOUNT_ID"),
        workspace_id=workspace_id,
        workspace_url=workspace_url,
        token=get_access_token(),
        rtl_env=args.env
    )

    if args.command == "show":
        workspace_manager.show_environment(unpaused_only=args.active_only,running_only=args.active_only,unterminated_only=args.active_only)
    elif args.command == "stop":
        workspace_manager.halt_environment(ignored_principals=args.ignored_principals)
    elif args.command == "restore":
        restore_path = choose_restore_point(rtl_env=args.env)
        workspace_manager.restore_environment(restore_path=restore_path)

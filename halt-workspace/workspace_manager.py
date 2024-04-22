import subprocess
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import argparse
import os
from dotenv import load_dotenv
from workspace_features import WorkspacePermissions, Workflows, JobRuns, AllPurposeCompute, SQLWarehouses

# MSAL
def get_access_token():
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
        return None

class DatabricksWorkspaceManager:
    def __init__(self, account_id, workspace_id, token, rtl_env, workspace_url):
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
        self.workspace_permissions = WorkspacePermissions(self)
        self.workflows = Workflows(self)
        self.job_runs = JobRuns(self)
        self.all_purpose_compute = AllPurposeCompute(self)
        self.sql_warehouses = SQLWarehouses(self)
        self.session = self.create_session()

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

    def restore_environment(self):
        self.workspace_permissions.restore()
        self.workflows.restore()
        self.job_runs.restore()
        self.all_purpose_compute.restore()
        self.sql_warehouses.restore()


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    runtime = parser.add_mutually_exclusive_group(required=True)
    runtime.add_argument("--stop", help="Stop the workspace", action="store_true")
    runtime.add_argument("--restore", help="Restore the workspace", action="store_true")
    runtime.add_argument("--show", help="Show all workspace objects", action="store_true")
    parser.add_argument("--env", help="Environment to use", type=str, required=True)
    parser.add_argument("--active", help="Show only active workflows and job runs", action="store_true")
    parser.add_argument("--ignored-principals", help="Principal IDs to ignore", type=int, nargs="+")
    args = parser.parse_args()

    with open(".env.json") as f:
        env = json.load(f)[args.env]
    workspace_manager = DatabricksWorkspaceManager(
        account_id=os.getenv("AZ_DATABRICKS_ACCOUNT_ID"),
        workspace_id=env["WORKSPACE_ID"],
        workspace_url=env["WORKSPACE_URL"],
        token=get_access_token(),
        rtl_env=args.env
    )

    if args.show:
        workspace_manager.show_environment(unpaused_only=args.active,running_only=args.active,unterminated_only=args.active)
    elif args.stop:
        workspace_manager.halt_environment(ignored_principals=args.ignored_principals)
    elif args.restore:
        workspace_manager.restore_environment()

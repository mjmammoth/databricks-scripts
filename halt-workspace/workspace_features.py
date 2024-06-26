import os
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor
from workspace_utils import check_errors, print_header
from abc import ABC, abstractmethod
import sys

# Display all rows and columns, no truncation
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

class DatabricksFeature(ABC):
    def __init__(self, manager):
        self.manager = manager
        self.account_url = manager.account_url
        self.workspace_url = manager.workspace_url
        self.workspace_id = manager.workspace_id
        self.headers = manager.headers
        self.rtl_env = manager.rtl_env
        self.session = manager.session
        self.start_time = manager.start_time
        self.restore_path = manager.restore_path if manager.restore_path else None

    def store(self, state_content, name):
        base_path = f"restore_states/{self.rtl_env}_{self.start_time}"
        os.makedirs(base_path, exist_ok=True)
        with open(f"{base_path}/{name}.json", "w") as f:
            json.dump(state_content, f, indent=4)

    def combine_paginated_results(self, url, aggregate_on, **kwargs):
        params = kwargs.get("params", {})
        response = self.session.get(url, headers=self.headers, params=params)
        check_errors(response)

        results = response.json()
        params["page_token"] = results.get("next_page_token")
        has_more = bool(results.get("has_more", False))

        params = params if params else {}
        while has_more:
            if aggregate_on not in results:
                raise ValueError(f"Expected key '{aggregate_on}' not found in response. Available keys: {results.keys()}")
            response = self.session.get(url, headers=self.headers, params=params)
            params["page_token"] = response.json().get("next_page_token")
            check_errors(response)
            results[aggregate_on] += response.json()[aggregate_on]
            has_more = bool(response.json()["has_more"])
            print('.', end='', flush=True)
        print()
        return results


    @abstractmethod
    def show(self, **kwargs):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def restore(self, restore_path: str):
        pass


class WorkspacePermissions(DatabricksFeature):
    def __init__(self, manager):
        super().__init__(manager)

    def _get_permissions(self):
        response = self.session.get(f"{self.account_url}/workspaces/{self.workspace_id}/permissionassignments", headers=self.headers)
        check_errors(response)
        return response.json()

    def _get_principal_ids(self):
        response = self._get_permissions()
        principal_ids = [assignment["principal"]["principal_id"] for assignment in response["permission_assignments"]]
        return principal_ids

    def _remove_permission(self, principal_id):
        response = self.session.delete(f"{self.account_url}/workspaces/{self.workspace_id}/permissionassignments/principals/{principal_id}", headers=self.headers)
        check_errors(response)

    def _restore_permission(self, assignment):
        response = self.session.put(f"{self.account_url}/workspaces/{self.workspace_id}/permissionassignments/principals/{assignment['principal']['principal_id']}", headers=self.headers, json=assignment)
        check_errors(response)


    def show(self, **kwargs):
        print_header("Workspace Permissions")
        permissions = self._get_permissions()
        if permissions.get("permission_assignments"):
            rows = [{
                "Principal ID": assignment["principal"]["principal_id"],
                "Display Name": assignment["principal"]["display_name"],
                "Permissions": ", ".join(assignment["permissions"])
            } for assignment in permissions["permission_assignments"]]
        
            df = pd.DataFrame(rows)
            print(df.to_string(index=False))
        else:
            print("No permissions found")

    def stop(self, **kwargs):
        print_header("Deleting Workspace Permissions")
        permissions = self._get_permissions()
        self.store(permissions, "permission_assignments")
        principal_ids = self._get_principal_ids()
        if kwargs.get("ignored_principals"):
            print(f"Ignoring principals: {kwargs['ignored_principals']}")
            principal_ids = [principal for principal in principal_ids if principal not in kwargs["ignored_principals"]]
        with ThreadPoolExecutor(max_workers=150) as executor:
            futures = [executor.submit(self._remove_permission, principal_id) for principal_id in principal_ids]
            for future in futures:
                try:
                    result = future.result() # Just to check for exceptions
                except Exception as e:
                    print(f"Error removing permission: {e}")
        print(f"Deleted {len(principal_ids)} permissions")

    def delete(self, ignored_principals):
        self.stop(ignored_principals=ignored_principals)

    def restore(self, restore_path: str):
        print_header("Restoring Workspace Permissions")
        with open(f"{restore_path}/permission_assignments.json") as f:
            permission_assignments = json.load(f)

        with ThreadPoolExecutor(max_workers=150) as executor:
            futures = [executor.submit(self._restore_permission, assignment) for assignment in permission_assignments["permission_assignments"]]
            for future in futures:
                try:
                    result = future.result() # Just to check for exceptions
                except Exception as e:
                    print(f"Error restoring permission: {e}")

        print(f"Restored {len(permission_assignments["permission_assignments"])} permissions")


class Workflows(DatabricksFeature):
    def __init__(self, manager):
        super().__init__(manager)

    def _get(self):
        result = self.combine_paginated_results(f"{self.workspace_url}/api/2.1/jobs/list", "jobs")
        return result

    def show(self, **kwargs):
        if kwargs.get("unpaused_only"):
            print_header("Unpaused Workflows")
        else:
            print_header("All Workflows")

        workflow = self._get()

        if not workflow.get("jobs"):
            print("No workflows exist")
            return

        print(f"{len(workflow['jobs'])} workflows exist")

        rows = [{
            "Job ID": workflow["job_id"],
            "Job Name": workflow["settings"]["name"],
            "Creator": workflow["creator_user_name"] if "creator_user_name" in workflow else "",
            "ContinuousStatus": workflow["settings"].get("continuous", {}).get("pause_status", ""),
            "Schedule": workflow["settings"]["schedule"]["quartz_cron_expression"] if "schedule" in workflow["settings"] else "",
            "ScheduleEnabled": workflow["settings"]["schedule"]["pause_status"] if "schedule" in workflow["settings"] else False,
            "Trigger": workflow["settings"]["trigger"]["pause_status"] if "trigger" in workflow["settings"] else "",
        } for workflow in workflow["jobs"]]
    
        df = pd.DataFrame(rows)
        if kwargs.get("unpaused_only"):
            # Cater for all of the possible unpaused/scheduling statuses
            unpaused_jobs = df.loc[(df['ScheduleEnabled'] == "UNPAUSED") | 
                   (df['Trigger'] == "UNPAUSED") | 
                   (df['ContinuousStatus'] == "UNPAUSED")]
            if unpaused_jobs.empty:
                print("All workflows are paused")
                return
            print(unpaused_jobs.to_string(index=False))
        else:
            print(df.to_string(index=False))

    def stop(self):
        print_header("Pausing Workflows")
        workflows = self._get()

        if not workflows.get("jobs"):
            print("..no workflows to pause")
            return

        print(f"Pausing {len(workflows['jobs'])} workflows")

        self.store(workflows, "workflows")
        for workflow in workflows["jobs"]:
            if workflow["settings"].get("schedule", {}).get("pause_status") == "UNPAUSED":
                print(f"Pausing scheduled job {workflow['job_id']}")
                payload = {"job_id": workflow["job_id"], "new_settings": {"schedule": {"pause_status": "PAUSED", "quartz_cron_expression": workflow["settings"]["schedule"]["quartz_cron_expression"], "timezone_id": workflow["settings"]["schedule"]["timezone_id"]}}}
                response = self.session.post(f"{self.workspace_url}/api/2.1/jobs/update", headers=self.headers, json=payload)
                check_errors(response)

            if workflow["settings"].get("continuous", {}).get("pause_status") == "UNPAUSED":
                print(f"Pausing continuous job {workflow['job_id']}")
                response = self.session.post(f"{self.workspace_url}/api/2.1/jobs/update", headers=self.headers, json={"job_id": workflow["job_id"], "new_settings": {"continuous": {"pause_status": "PAUSED"}}})
                check_errors(response)

            if workflow["settings"].get("trigger", {}).get("pause_status") == "UNPAUSED":
                print(f"Pausing file trigger job {workflow['job_id']}")
                response = self.session.post(f"{self.workspace_url}/api/2.1/jobs/update", headers=self.headers, json={"job_id": workflow["job_id"], "new_settings": {"trigger": {"pause_status": "PAUSED"}}})
                check_errors(response)

    def pause(self):
        self.stop()

    def restore(self, restore_path: str):
        print_header("Restoring Workflows")
        try:
            with open(f"{restore_path}/workflows.json") as f:
                workflows = json.load(f)
        except FileNotFoundError:
            print("No workflows to restore")
            return

        if not workflows.get("jobs"):
            print("No workflows to restore")
            return

        for workflow in workflows["jobs"]:
            if workflow["settings"].get("schedule", {}).get("pause_status") == "UNPAUSED":
                print(f"Resuming scheduled job {workflow['job_id']}")
                payload = {"job_id": workflow["job_id"], "new_settings": {"schedule": {"pause_status": "UNPAUSED", "quartz_cron_expression": workflow["settings"]["schedule"]["quartz_cron_expression"], "timezone_id": workflow["settings"]["schedule"]["timezone_id"]}}}
                response = self.session.post(f"{self.workspace_url}/api/2.1/jobs/update", headers=self.headers, json=payload)
                check_errors(response)

            if workflow["settings"].get("continuous", {}).get("pause_status") == "UNPAUSED":
                print(f"Resuming continuous job {workflow['job_id']}")
                response = self.session.post(f"{self.workspace_url}/api/2.1/jobs/update", headers=self.headers, json={"job_id": workflow["job_id"], "new_settings": {"continuous": {"pause_status": "UNPAUSED"}}})
                check_errors(response)

            if workflow["settings"].get("trigger", {}).get("pause_status") == "UNPAUSED":
                print(f"Resuming file trigger job {workflow['job_id']}")
                response = self.session.post(f"{self.workspace_url}/api/2.1/jobs/update", headers=self.headers, json={"job_id": workflow["job_id"], "new_settings": {"trigger": {"pause_status": "UNPAUSED"}}})
                check_errors(response)
        pass


class JobRuns(DatabricksFeature):
    def __init__(self, manager):
        super().__init__(manager)

    def _get(self, **kwargs):
        params = {"limit": 25}
        if kwargs.get('running_only'):
            params["active_only"] = "true"
        result = self.combine_paginated_results(f"{self.workspace_url}/api/2.1/jobs/runs/list", "runs", params=params)
        return result

    def show(self, **kwargs):
        if kwargs.get("running_only"):
            print_header("Running Jobs")
        else:
            print_header("All Job Runs")
        jobs = self._get(**kwargs)

        if not jobs.get("runs"):
            print("No job runs found")
            return

        if jobs.get("runs"):
            rows = [{
                "Job ID": job["job_id"],
                "Creator": job["creator_user_name"],
                "Start Time": job["start_time"],
                "State": job["state"]["life_cycle_state"],
            } for job in jobs["runs"]]
        
            df = pd.DataFrame(rows)
            if df.empty:
                print("No active job runs")
                return
            print(df.to_string(index=False))

    def stop(self):
        print_header("Stopping Active Job Runs")
        jobs = self._get(running_only=True)
        self.store(jobs, "job_runs")

        if not jobs.get("runs"):
            print("No active job runs to stop")
            return

        for job in jobs["runs"]:
            response = self.session.post(f"{self.workspace_url}/api/2.1/jobs/runs/cancel", headers=self.headers, json={"run_id": job["run_id"]})
            check_errors(response)

        print(f"Stopped {len(jobs['runs'])} active job runs")

    def restore(self, restore_path: str):
        print('.. not restoring job runs')


class AllPurposeCompute(DatabricksFeature):
    def __init__(self, manager):
        super().__init__(manager)

    def _get(self):
        result = self.combine_paginated_results(f"{self.workspace_url}/api/2.0/clusters/list", "clusters")
        return result

    def show(self, **kwargs):
        print_header("All Purpose Compute")
        compute = self._get()
        if compute.get("clusters"):
            rows = [{
                "Cluster ID": cluster["cluster_id"],
                "Cluster Name": cluster["cluster_name"],
                "State": cluster["state"],
                "Creator": cluster["creator_user_name"],
            } for cluster in compute["clusters"]]
        
            df = pd.DataFrame(rows)
            if kwargs.get("unterminated_only"):
                unterminated_clusters = df.loc[df['State'] != "TERMINATED"]
                if unterminated_clusters.empty:
                    print("All clusters are terminated")
                    return
                print(unterminated_clusters.to_string(index=False))
                return
            print(df.to_string(index=False))

    def stop(self):
        print_header("Stopping All Purpose Compute")
        clusters = self._get()
        self.store(clusters, "all_purpose_compute_clusters")

        clusters = [cluster for cluster in clusters["clusters"] if cluster["state"] != "TERMINATED"]
        if not clusters:
            print("All clusters are already terminated")
            return

        print(f"Terminating {len(clusters)} clusters")
        for cluster in clusters:
            response = self.session.post(f"{self.workspace_url}/api/2.0/clusters/delete", headers=self.headers, json={"cluster_id": cluster["cluster_id"]})
            if response.status_code != 200:
                print(f"Error: {response.status_code}")
                sys.exit()
        print(f"Terminated {len(clusters)} clusters")

    def restore(self, restore_path: str):
        print('.. not restoring all purpose compute')


class SQLWarehouses(DatabricksFeature):
    def __init__(self, manager):
        super().__init__(manager)

    def _get(self):
        result = self.combine_paginated_results(f"{self.workspace_url}/api/2.0/sql/warehouses", "warehouses")
        return result

    def show(self, **kwargs):
        print_header("SQL Warehouses")
        warehouses = self._get()
        if warehouses.get("warehouses"):
            rows = [{
                "Warehouse ID": warehouse["id"],
                "Warehouse Name": warehouse["name"],
                "State": warehouse["state"],
            } for warehouse in warehouses["warehouses"]]
        
            df = pd.DataFrame(rows)
            if kwargs.get("unterminated_only"):
                unterminated_warehouses = df.loc[df['State'] != "STOPPED"]
                if unterminated_warehouses.empty:
                    print("All warehouses are terminated")
                    return
                print(unterminated_warehouses.to_string(index=False))
                return
            print(df.to_string(index=False))

    def stop(self):
        print_header("Stopping SQL Warehouses")
        warehouses = self._get()
        self.store(warehouses, "sql_warehouses")

        if not warehouses.get("warehouses"):
            print("No warehouses to stop")
            return

        for warehouse in warehouses["warehouses"]:
            if warehouse["state"] != "STOPPED":
                response = self.session.post(f"{self.workspace_url}/api/2.0/sql/warehouses/{warehouse['id']}/stop", headers=self.headers)
                check_errors(response)

        print(f"Stopped {len(warehouses['warehouses'])} warehouses")

    def restore(self, restore_path: str):
        print('.. not restoring sql warehouses')

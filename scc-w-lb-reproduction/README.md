Reproduction steps to aid Azure support ticket #2503260050001478

### Pre-requisites

- [terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
- To be logged in via [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/)
  - Set the target subscription

  ```bash
  az account set --subscriptionz set 
- Terraform variables to be set by editing [terraform.tfvars](./terraform.tfvars) accordingly

> [!NOTE]
> The `prefix` variable set in the `terraform.tfvars` file is what will dictate the name of the resource group where these resources will be created
> e.g. prefix = dentsuscc would create a resource group called `dentsuscc-databrick-connectivity-with-lb`

```bash
# git clone this repo
git clone git@github.com:mjmammoth/databricks-scripts.git
# change direcory into scc-w-lb-reproduction
cd scc-w-lb-reproduction
# initialize terraform
terraform init
# terraform plan the resources to see what would be created
terraform plan
# If the plan looks good, apply the changes
terraform apply
```

### The bug/problem

With the `use_nat_gateway` variable set to `false` (the default), the workspace will be created with a loadbalancer being linked to it.

The JSON view of the workspace will not report that a loadbalancer is linked, nor will the loadbalancer ID be visible in the properties of the workspace within the Azure portal.

Running `terraform plan` again after creation will indicate that the workspace needs to be recreated as from its perspective, the loadbalancer ID has changed (to nothing, because the API no longer reports it) - and therefore wants to recreate the workspace.

### Migration to NAT Gateway

To attempt to migrate to using a NAT gateway for cluster egress, set the `use_nat_gateway` variable to `true` in the `terraform.tfvars` file and run `terraform plan`, and if all looks good, `terraform apply` again.

In this state, when trying to start compute inside databricks, an error will be raised.
Something to the effect of:

> [!Error]
> Cluster 'cluster_id' was terminated. Reason: NETWORK_CONFIGURATION_FAILURE (CLIENT_ERROR). Parameters: azure_error_code:AzureLoadBalancerConfigurationFailure, azure_error_message:Encountered error while trying to get Load Balancer Backend Pool: Load Balancer does not exist - /subscriptions/subscription_id/resourceGroups/resource_group/providers/Microsoft.Network/loadBalancers/loadbalancer, databricks_error_message:Azure error: Error code: AzureLoadBalancerConfigurationFailure, error message: Encountered error while trying to get Load Balancer Backend Pool: Load Balancer does not exist ...

Which indicates that

1. Even though the Azure API has stopped reporting the loadbalancer ID, the databricks workspace is still trying to use it and
2. That if the workspace was created with a loadbalancer, it is not currently possible to migrate away from the loadbalancer and towards a NAT gateway for egress as is [recommended for VNET-injected Secure Clusters](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/secure-cluster-connectivity#egress-with-vnet-injection)

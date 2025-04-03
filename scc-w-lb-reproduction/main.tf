provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

resource "azurerm_resource_group" "example" {
  name     = "${var.prefix}-databrick-connectivity-with-lb"
  location = "eastus2"
}

resource "azurerm_virtual_network" "example" {
  name                = "${var.prefix}-databricks-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name
}

resource "azurerm_subnet" "public" {
  name                 = "${var.prefix}-public-subnet"
  resource_group_name  = azurerm_resource_group.example.name
  virtual_network_name = azurerm_virtual_network.example.name
  address_prefixes     = ["10.0.1.0/24"]

  delegation {
    name = "${var.prefix}-databricks-del"

    service_delegation {
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
      name = "Microsoft.Databricks/workspaces"
    }
  }
}

resource "azurerm_subnet" "private" {
  name                 = "${var.prefix}-private-subnet"
  resource_group_name  = azurerm_resource_group.example.name
  virtual_network_name = azurerm_virtual_network.example.name
  address_prefixes     = ["10.0.2.0/24"]

  delegation {
    name = "${var.prefix}-databricks-del"

    service_delegation {
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
      name = "Microsoft.Databricks/workspaces"
    }
  }
}

resource "azurerm_subnet_network_security_group_association" "private" {
  subnet_id                 = azurerm_subnet.private.id
  network_security_group_id = azurerm_network_security_group.example.id
}

resource "azurerm_subnet_network_security_group_association" "public" {
  subnet_id                 = azurerm_subnet.public.id
  network_security_group_id = azurerm_network_security_group.example.id
}

resource "azurerm_network_security_group" "example" {
  name                = "${var.prefix}-databricks-nsg"
  location            = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name
}

resource "azurerm_databricks_workspace" "example" {
  name                        = "DBW-${var.prefix}"
  resource_group_name         = azurerm_resource_group.example.name
  location                    = azurerm_resource_group.example.location
  sku                         = "premium"
  managed_resource_group_name = "${var.prefix}-DBW-managed-with-lb"

  public_network_access_enabled         = true
  load_balancer_backend_address_pool_id = var.use_nat_gateway ? null : azurerm_lb_backend_address_pool.example[0].id

  custom_parameters {
    no_public_ip        = true
    public_subnet_name  = azurerm_subnet.public.name
    private_subnet_name = azurerm_subnet.private.name
    virtual_network_id  = azurerm_virtual_network.example.id

    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
  }

  tags = {
    Environment = "Production"
    Pricing     = "Standard"
  }
}

resource "azurerm_public_ip" "example" {
  name                    = "Databricks-LB-PublicIP"
  location                = azurerm_resource_group.example.location
  resource_group_name     = azurerm_resource_group.example.name
  idle_timeout_in_minutes = 4
  allocation_method       = "Static"

  sku = "Standard"
}

resource "azurerm_lb" "example" {
  count               = var.use_nat_gateway ? 0 : 1
  name                = "Databricks-LB"
  location            = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name

  sku = "Standard"

  frontend_ip_configuration {
    name                 = "Databricks-PIP"
    public_ip_address_id = azurerm_public_ip.example.id
  }
}

resource "azurerm_lb_outbound_rule" "example" {
  count               = var.use_nat_gateway ? 0 : 1
  name                = "Databricks-LB-Outbound-Rules"

  loadbalancer_id          = azurerm_lb.example[0].id
  protocol                 = "All"
  enable_tcp_reset         = true
  allocated_outbound_ports = 1024
  idle_timeout_in_minutes  = 4

  backend_address_pool_id = azurerm_lb_backend_address_pool.example[0].id

  frontend_ip_configuration {
    name = azurerm_lb.example[0].frontend_ip_configuration[0].name
  }
}

resource "azurerm_lb_backend_address_pool" "example" {
  count           = var.use_nat_gateway ? 0 : 1
  loadbalancer_id = azurerm_lb.example[0].id
  name            = "Databricks-BE"
}

resource "azurerm_nat_gateway" "nat_gw" {
  count = var.use_nat_gateway ? 1 : 0
  name = "databricks-nat-gw"
  location = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name
  sku_name = "Standard"
}

resource "azurerm_nat_gateway_public_ip_association" "nat_gw_ip" {
  count = var.use_nat_gateway ? 1 : 0
  nat_gateway_id = azurerm_nat_gateway.nat_gw[0].id
  public_ip_address_id = azurerm_public_ip.example.id
}

resource "azurerm_subnet_nat_gateway_association" "public_subnet" {
  count          = var.use_nat_gateway ? 1 : 0
  subnet_id      = azurerm_subnet.public.id
  nat_gateway_id = azurerm_nat_gateway.nat_gw[0].id
}

resource "azurerm_subnet_nat_gateway_association" "private_subnet" {
  count          = var.use_nat_gateway ? 1 : 0
  subnet_id      = azurerm_subnet.private.id
  nat_gateway_id = azurerm_nat_gateway.nat_gw[0].id
}

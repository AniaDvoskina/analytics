terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version =  "4.19.0"  
    }
    databricks = {
      source = "databricks/databricks"
      version = "1.64.1"
    }
    azuread = {
      source = "hashicorp/azuread"
      version = "2.24.0"  
    }
  }
}

provider "azuread" {
  tenant_id = "8a114ab3-b0d6-4824-9440-c3ee1b307a1c"
}

provider "azurerm" {
  features {}
  subscription_id = "e6485776-febf-427b-b102-094fcdd33b45"
  tenant_id       = "8a114ab3-b0d6-4824-9440-c3ee1b307a1c"
}

# Define Azure Resource Group
resource "azurerm_resource_group" "analytics" {
  name     = "AnalyticsResourceGroup"
  location = "West Europe"
}

# Create an Event Hub Namespace
resource "azurerm_eventhub_namespace" "analytics_namespace" {
  name                = "kaggleevents"
  location            = azurerm_resource_group.analytics.location
  resource_group_name = azurerm_resource_group.analytics.name
  sku                 = "Basic"  
  capacity            = 1
}

# Create an Event Hub inside the Namespace
resource "azurerm_eventhub" "analytics_hub" {
  name                = "kaggleeventhub"
  namespace_name      = azurerm_eventhub_namespace.analytics_namespace.name
  resource_group_name = azurerm_resource_group.analytics.name
  partition_count     = 2
  message_retention   = 1 
}
#Create a storage account
resource "azurerm_storage_account" "kaggle_project" {
  name                     = "kaggleglobal" 
  resource_group_name      = azurerm_resource_group.analytics.name
  location                =  azurerm_resource_group.analytics.location
  account_tier             = "Standard"
  account_replication_type = "LRS"  

  tags = {
    environment = "dev"
  }
}
#Create a container
resource "azurerm_storage_container" "analytics_container" {
  name                  = "kaggle-pipeline"  
  storage_account_name  = azurerm_storage_account.kaggle_project.name
  container_access_type = "private"  
}
#Create a databrciks workspace
resource "azurerm_databricks_workspace" "analytics_project" {
  name                = "global-databricks-workspace" 
  location            = azurerm_resource_group.analytics.location
  resource_group_name = azurerm_resource_group.analytics.name
  sku                  = "trial"
}
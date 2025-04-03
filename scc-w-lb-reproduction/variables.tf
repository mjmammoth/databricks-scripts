variable "prefix" {
  description = "The Prefix used for all resources in this example"
}

variable "subscription_id" {
  description = "The subscription ID to use for the resources"
  type        = string
}

variable "use_nat_gateway" {
  description = "Whether to use a NAT Gateway for the private subnet"
  type        = bool
  default     = false
}

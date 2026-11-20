# One Vultr VM for archive backfill runs (#124). The API key comes from VULTR_API_KEY only.
terraform {
  required_providers {
    vultr = {
      source  = "vultr/vultr"
      version = "~> 2.33"
    }
  }
}

provider "vultr" {}

variable "plan" {
  description = "Vultr plan id; vc2-8c-32gb is 8 vCPU, 32 GB, 640 GB SSD"
  default     = "vc2-8c-32gb"
}

variable "region" {
  default = "ord"
}

variable "ssh_public_key" {
  description = "Contents of the operator's SSH public key"
  type        = string
}

variable "admin_cidr" {
  description = "Only this IPv4 CIDR may reach SSH, e.g. 203.0.113.7/32"
  type        = string
}

variable "git_ref" {
  description = "Commit or branch of alexnham/helius-indexer to build"
  default     = "main"
}

resource "vultr_ssh_key" "operator" {
  name    = "helius-indexer-backfill"
  ssh_key = var.ssh_public_key
}

resource "vultr_firewall_group" "backfill" {
  description = "helius-indexer backfill: SSH from the operator only"
}

resource "vultr_firewall_rule" "ssh" {
  firewall_group_id = vultr_firewall_group.backfill.id
  protocol          = "tcp"
  ip_type           = "v4"
  subnet            = split("/", var.admin_cidr)[0]
  subnet_size       = tonumber(split("/", var.admin_cidr)[1])
  port              = "22"
}

resource "vultr_instance" "backfill" {
  label             = "helius-indexer-backfill"
  plan              = var.plan
  region            = var.region
  os_id             = 2284 # Ubuntu 24.04 LTS x64
  ssh_key_ids       = [vultr_ssh_key.operator.id]
  firewall_group_id = vultr_firewall_group.backfill.id
  enable_ipv6       = false
  backups           = "disabled"
  user_data         = templatefile("${path.module}/cloud-init.yaml", { git_ref = var.git_ref })
}

output "ip" {
  value = vultr_instance.backfill.main_ip
}

output "ssh" {
  description = "Login, with the backfill dashboard tunnelled to localhost:8768"
  value       = "ssh -L 8768:127.0.0.1:8768 root@${vultr_instance.backfill.main_ip}"
}

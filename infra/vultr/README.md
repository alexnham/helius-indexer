# Vultr backfill VM

One VM for the archive backfill (#124): Terraform creates the instance, an SSH key and a
firewall that admits SSH from one address only. Cloud-init clones the repository, starts
PostgreSQL from `compose.yaml` (with larger limits from `compose.override.yaml`) and builds
`--features archive`. Nothing else listens publicly; the dashboard is reached through SSH.

The default `vc2-8c-32gb` costs $0.219/h (Vultr plans API, 2026-11-19): ~$0.25 for the
1-hour test, ~$37 for a week. Inbound traffic is not billed. Its 640 GB disk holds the test,
not a 30-day run; size the full run from the test (#124, #70).

## Create

```
export VULTR_API_KEY=...   # in your shell profile, never in a file here
cat > terraform.tfvars <<EOT
ssh_public_key = "$(cat ~/.ssh/id_ed25519.pub)"
admin_cidr     = "$(curl -s https://api.ipify.org)/32"
EOT
terraform init && terraform apply
```

Cloud-init takes ~10 minutes; `/opt/indexer/.build-done` appears when the build finishes
(`tail -f /var/log/cloud-init-output.log` shows progress).

## 1-hour test

```
ssh root@IP
cd /opt/indexer
export DATABASE_URL=postgres://indexer:indexer@127.0.0.1:55432/indexer
B=target/release/helius-indexer; A="--binary $B --directory /opt/runs --session vm_test"
# --days 30 probes public RPC, which failed from a VM; start from a known slot instead.
python3 scripts/archive-backfill.py plan $A --allow-provider --start-slot 470447000
python3 scripts/archive-backfill.py run $A --allow-provider --config config/backfill.example.json \
  --slots-per-batch 200 --max-batches 400 --max-reserved-bytes 600000000000
```

Run it in `tmux` so a dropped SSH session does not stop it; `touch /opt/runs/CANCEL`
stops it after the current batch. `python3 scripts/archive-backfill.py serve $A` shows progress on
http://127.0.0.1:8768 through the tunnel in `terraform output ssh`. Record the results in `docs/validation/`.

## Tear down

Copy off anything to keep first (`pg_dump`, the run directory), then `terraform destroy`.
Billing stops only when the instance is destroyed, not when it is stopped.

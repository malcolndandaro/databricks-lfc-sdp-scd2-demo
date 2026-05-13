.DEFAULT_GOAL := help
.PHONY: help reset reset-sql-server reset-volume reset-databricks

# ----------------------------------------------------------------------------
# Demo reset targets — Slice 12.
#
# `make reset` is reset-databricks alone. Why not chain reset-sql-server first?
# Chicken-and-egg: reset-sql-server triggers the deployed sqlserver_setup Job,
# which uses the workspace's CURRENT notebook code. bundle deploy (which updates
# the notebook code) only happens at step 2 of reset-databricks. So if the
# notebook has any new behaviour committed since the last deploy, reset-sql-server
# runs against stale code and fails (we hit this 2026-05-11 when phase_0_truncate
# was added). Since the new sqlserver_setup Job has phase_0_truncate built in,
# the standalone reset-sql-server is redundant for full resets; it remains as a
# surgical command for when SQL Server alone needs re-seeding.
#
# reset-volume is also NOT chained — it's called INSIDE reset-databricks at the
# right point (after bundle deploy recreates the bronze.lz volume).
#
# Every script asserts target == 'dev' internally; this Makefile only ever
# passes --yes to skip the interactive 5-second prompt. To opt back into
# the prompt, run the underlying script directly:
#     python scripts/reset_databricks.py
#
# Per PRD: reset is admin-only and never a live demo scene. Do NOT wire it
# into GitHub Actions or run it against directsales_prod.
# ----------------------------------------------------------------------------

help:
	@echo "Demo reset targets (Slice 12). All target=dev only:"
	@echo ""
	@echo "  make reset              Full reset = reset-databricks (deploys latest"
	@echo "                          bundle code first, then runs the 9-step flow"
	@echo "                          which itself includes TRUNCATE + sqlserver_setup"
	@echo "                          + volume repopulate + SDP + governance + verify)"
	@echo "                          ~5-8 min total."
	@echo "  make reset-sql-server   Surgical: TRUNCATE locally + run deployed"
	@echo "                          sqlserver_setup Job. Uses workspace's current"
	@echo "                          notebook code — only safe when you know the"
	@echo "                          deployed code matches what you want."
	@echo "  make reset-volume       Surgical: wipe + regenerate parquet landing"
	@echo "                          zone. Use between rehearsals when only the"
	@echo "                          parquet landing zone is dirty."
	@echo "  make reset-databricks   Same as 'make reset'."
	@echo ""
	@echo "All scripts route through Databricks SDK / CLI; no SQL Server"
	@echo "credentials are read locally except by reset-sql-server (TRUNCATE)."
	@echo ""
	@echo "Run the underlying script directly to see the interactive prompt"
	@echo "(this Makefile passes --yes for unattended use):"
	@echo "    python scripts/reset_databricks.py"

# `make reset` = reset-databricks ONLY. It does bundle deploy first, so the
# Job runs against fresh code every time. See the long comment above for why
# reset-sql-server isn't chained anymore.
reset: reset-databricks
	@echo ""
	@echo "Full reset complete."

reset-sql-server:
	python scripts/reset_sql_server.py --yes

reset-volume:
	python scripts/reset_volume.py --yes

reset-databricks:
	python scripts/reset_databricks.py --yes

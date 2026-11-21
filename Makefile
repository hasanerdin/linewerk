.PHONY: smoke
smoke: ## Write and read a tiny Delta table to verify Spark + Delta
	rm -rf data/lake/_smoke
	uv run python -m linewerk.jobs.smoke
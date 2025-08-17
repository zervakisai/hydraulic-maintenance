# ————————————————————————————————————————————
# Strict Validation Makefile
# ————————————————————————————————————————————
.PHONY: help validate strict validate-high validate-mid validate-low profile clean

# Μπορείς να αλλάξεις thresholds από το command line:
#   make validate MAX_NAN_RATIO=0.0002 MAX_NEG_VIOL=0
MAX_NAN_RATIO ?= 0.001
MAX_NEG_VIOL  ?= 0

PYTHON := python

help:
	@echo "make validate           - Validate high/mid/low with default thresholds"
	@echo "make strict             - Validate with stricter thresholds"
	@echo "make validate-high      - Validate μόνο το high group"
	@echo "make validate-mid       - Validate μόνο το mid group"
	@echo "make validate-low       - Validate μόνο το low group"
	@echo "make profile            - Validate + HTML profiling (sampled)"
	@echo "make clean              - Καθάρισε intermediate & reports"
	@echo ""
	@echo "Override thresholds: make validate MAX_NAN_RATIO=0.0005 MAX_NEG_VIOL=0"

validate:
	$(PYTHON) -m src.validation.validate_data \
	  --groups high mid low \
	  --max-nan-ratio $(MAX_NAN_RATIO) \
	  --max-neg-violations $(MAX_NEG_VIOL)

strict:
	$(PYTHON) -m src.validation.validate_data \
	  --groups high mid low \
	  --max-nan-ratio 0.0005 \
	  --max-neg-violations 0

validate-high:
	$(PYTHON) -m src.validation.validate_data \
	  --groups high \
	  --max-nan-ratio $(MAX_NAN_RATIO) \
	  --max-neg-violations $(MAX_NEG_VIOL)

validate-mid:
	$(PYTHON) -m src.validation.validate_data \
	  --groups mid \
	  --max-nan-ratio $(MAX_NAN_RATIO) \
	  --max-neg-violations $(MAX_NEG_VIOL)

validate-low:
	$(PYTHON) -m src.validation.validate_data \
	  --groups low \
	  --max-nan-ratio $(MAX_NAN_RATIO) \
	  --max-neg-violations $(MAX_NEG_VIOL)

profile:
	$(PYTHON) -m src.validation.validate_data \
	  --groups high mid low \
	  --profile --profile-sample 50000 \
	  --max-nan-ratio $(MAX_NAN_RATIO) \
	  --max-neg-violations $(MAX_NEG_VIOL)

clean:
	rm -f data/interim/*.parquet
	rm -f reports/*.json reports/*.html


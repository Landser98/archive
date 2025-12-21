# =========================
# Environment
# =========================
VENV_ACTIVATE = source .venv/bin/activate

# =========================
# Alatau City Bank
# =========================
alatau:
	$(VENV_ACTIVATE) && \
	python -m src.alatau_city_bank.batch_parse data/alatau_city_bank --months-back 12

alatau-file:
	$(VENV_ACTIVATE) && \
	python -m src.alatau_city_bank.batch_parse \
	data/alatau_city_bank/statements_1758618878177.pdf \
	--out-dir data/alatau_city_bank/out \
	--json-dir data/alatau_city_bank/json

# =========================
# BCC
# =========================
bcc:
	$(VENV_ACTIVATE) && \
	python -m src.bcc.batch_parse data/bcc \
	--out-dir data/bcc/out \
	--jsonl-dir data/bcc/converted_jsons \
	--pdf-meta-dir data/bcc/pdf_meta \
	--months-back 12

bcc-file:
	$(VENV_ACTIVATE) && \
	python -m src.bcc.batch_parse data/bcc/1717389395273.pdf \
	--out-dir data/bcc/out \
	--jsonl-dir data/bcc/converted_jsons \
	--pdf-meta-dir data/bcc/pdf_meta \
	--months-back 12

# =========================
# Eurasian Bank
# =========================
eurasian:
	$(VENV_ACTIVATE) && \
	python -m src.eurasian_bank.batch_parse data/eurasian_bank \
	--out-dir data/eurasian_bank/out \
	--pdf-meta-dir data/eurasian_bank/pdf_meta \
	--pattern "*.pdf" \
	--months-back 12

# =========================
# ForteBank
# =========================
forte:
	$(VENV_ACTIVATE) && \
	python -m src.forte_bank.batch_parse data/forte_bank \
	--out-dir data/forte_bank/out \
	--jsonl-dir data/forte_bank/converted_jsons \
	--pdf-meta-dir data/forte_bank/pdf_meta \
	--pattern "*.pdf" \
	--months-back 12

# =========================
# Freedom Bank
# =========================
freedom:
	$(VENV_ACTIVATE) && \
	python -m src.freedom_bank.batch_parse data/freedom \
	--out-dir data/freedom/out \
	--pdf-meta-dir data/freedom/pdf_meta \
	--pattern "*.pdf" \
	--months-back 12

# =========================
# Halyk
# =========================
halyk-business:
	$(VENV_ACTIVATE) && \
	python -m src.halyk_business.batch_parse data/halyk_bank/halyk_business \
	--out-dir data/halyk_business/out \
	--jsonl-dir data/halyk_business/converted_jsons \
	--pdf-meta-dir data/halyk_business/pdf_meta \
	--months-back 12

halyk-ind:
	$(VENV_ACTIVATE) && \
	python -m src.halyk_ind.batch_parse data/halyk_bank/halyk_individual \
	--out-dir data/halyk_individual/out \
	--jsonl-dir data/halyk_individual/converted_jsons \
	--months-back 12

# =========================
# Kaspi
# =========================
kaspi-gold:
	$(VENV_ACTIVATE) && \
	python -m src.kaspi_gold.batch_parse data/kaspi_gold \
	--out-dir data/kaspi_gold/kaspi_parsed

kaspi-pay:
	$(VENV_ACTIVATE) && \
	python -m src.kaspi_pay.batch_parse data/kaspi_pay \
	--out-dir data/kaspi_pay/out \
	--input-type pdf \
	--jsonl-dir data/kaspi_pay/converted_jsons \
	--pdf-meta-dir data/kaspi_pay/pdf_meta

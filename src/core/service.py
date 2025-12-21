from .adapters import (
    parse_kaspi_gold_statement,
    parse_kaspi_pay_statement,
    parse_halyk_business_statement,
    parse_halyk_individual_statement,
    parse_freedom_bank_statement,
    parse_forte_bank_statement,
    parse_eurasian_bank_statement,
    parse_bcc_bank_statement,
    parse_alatau_city_bank_statement,
)

BANK_DISPATCH = {
    "kaspi_gold": parse_kaspi_gold_statement,
    "kaspi_pay": parse_kaspi_pay_statement,
    "halyk_business": parse_halyk_business_statement,
    "halyk_individual": parse_halyk_individual_statement,
    "freedom_bank": parse_freedom_bank_statement,
    "forte_bank": parse_forte_bank_statement,
    "eurasian_bank": parse_eurasian_bank_statement,
    "bcc_bank": parse_bcc_bank_statement,
    "alatau_city_bank": parse_alatau_city_bank_statement,
}

def parse_statement(bank_key: str, pdf_name: str, pdf_bytes: bytes):
    if bank_key not in BANK_DISPATCH:
        raise NotImplementedError(f"Unknown bank_key={bank_key}")
    return BANK_DISPATCH[bank_key](pdf_name, pdf_bytes)

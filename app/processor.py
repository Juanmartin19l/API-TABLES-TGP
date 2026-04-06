import re
from typing import List, Optional


DATE_PATTERN = re.compile(r"\d{1,2}/\d{1,2}/\d{4}")
NUMBER_PATTERN = re.compile(r"[\d.,]+")

COLUMNS = ["Fecha", "Origen", "Monto", "Retenido", "MEP/CTA BNA", "TOTAL"]


def clean_text(text: str) -> str:
    if text is None:
        return ""
    return text.strip().replace("\n", "").replace("\r", "")


def is_null_value(text: str) -> bool:
    text = text.strip().replace("$", "").replace("-", "").strip()
    return text == "" or text.lower() == "null" or text == "$"


def is_negative_value(text: str) -> bool:
    return False


def parse_value_from_token(token: str) -> tuple[Optional[float], bool]:
    token = token.strip()

    if is_null_value(token):
        return None, False

    is_negative = is_negative_value(token)

    token = token.replace("-$", "").replace("$", "").strip()

    token = token.replace("-", "").strip()

    if not token:
        return None, False

    number_match = NUMBER_PATTERN.search(token)
    if not number_match:
        return None, False

    number_str = number_match.group().replace(".", "").replace(",", ".")
    try:
        value = float(number_str)
        return value, is_negative
    except ValueError:
        return None, False


def find_dates_and_split(text: str) -> List[dict]:
    text = text.strip()

    date_positions = []
    for match in DATE_PATTERN.finditer(text):
        date_positions.append(
            {"date": match.group(), "start": match.start(), "end": match.end()}
        )

    if not date_positions:
        return []

    rows = []

    for idx, date_info in enumerate(date_positions):
        date_start = date_info["start"]
        date_str = date_info["date"]

        if idx + 1 < len(date_positions):
            next_date_start = date_positions[idx + 1]["start"]
            row_text = text[date_start:next_date_start]
        else:
            row_text = text[date_start:]

        rows.append({"Fecha": date_str, "raw_text": row_text.strip()})

    return rows


def process_row_text(row_text: str) -> dict:
    row_text = row_text.strip()

    date_match = DATE_PATTERN.search(row_text)
    if date_match:
        row_text = row_text[date_match.end() :].strip()

    tokens = row_text.split("$")

    origin = None
    monto = None
    retenido = None
    mep_cta_bna = None
    total = None

    if tokens:
        first_part = tokens[0].strip()

        if first_part:
            parts = first_part.split()
            filtered_parts = []
            for part in parts:
                if not DATE_PATTERN.match(part):
                    filtered_parts.append(part)
            if filtered_parts:
                origin = " ".join(filtered_parts)

    values = []
    for i in range(1, len(tokens)):
        token = tokens[i]

        value, is_negative = parse_value_from_token(token)

        values.append(
            {"value": value, "is_negative": is_negative, "token": token.strip()}
        )

    if len(values) >= 1:
        monto = values[0]["value"]

    if len(values) >= 2:
        retenido = values[1]["value"]
        if values[1]["is_negative"] and retenido is not None:
            retenido = -abs(retenido)

    if len(values) >= 3:
        mep_cta_bna = values[2]["value"]

    if len(values) >= 4:
        total = values[3]["value"]

    return {
        "Fecha": None,
        "Origen": origin,
        "Monto": monto,
        "Retenido": retenido,
        "MEP/CTA BNA": mep_cta_bna,
        "TOTAL": total,
    }


def process_ocr_response(ocr_response: dict) -> List[dict]:
    all_items = []

    for archivo in ocr_response.get("resultados_por_archivo", []):
        resultado = archivo.get("resultado")
        if resultado and resultado.get("resultados"):
            for item in resultado["resultados"]:
                all_items.append(item.get("texto", ""))

    full_text = " ".join([clean_text(item) for item in all_items if item])

    full_text = re.sub(r"\s+", " ", full_text)

    rows_data = find_dates_and_split(full_text)

    results = []
    for row in rows_data:
        row_result = process_row_text(row["raw_text"])
        row_result["Fecha"] = row["Fecha"]
        results.append(row_result)

    return results

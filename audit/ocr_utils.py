import easyocr
import re

reader = easyocr.Reader(['en'])

def extract_text_from_image(image_path):
    result = reader.readtext(image_path)
    text = " ".join([item[1] for item in result])
    return text

def _parse_number(value):
    if value is None:
        return None
    normalized = str(value).strip().replace(' ', '').replace(',', '')
    if normalized.endswith('.') or normalized.endswith(','):
        normalized = normalized[:-1]
    try:
        return float(normalized)
    except ValueError:
        return None


def extract_amount(text):
    # Match numbers like 1,234.56 or 1234,56 and drop trailing punctuation
    matches = re.findall(r'\d+(?:[.,]\d+)?', text)
    amounts = []
    for m in matches:
        n = _parse_number(m)
        if n is not None and n > 100:
            amounts.append(n)

    return max(amounts) if amounts else 0


def detect_price_mismatch(text):
    nums = re.findall(r'\d+(?:[.,]\d+)?', text)
    numbers = [_parse_number(x) for x in nums]
    numbers = [x for x in numbers if x is not None]

    if len(numbers) >= 3:
        unit = numbers[0]
        qty = numbers[1]
        total = numbers[2]

        # Allow small floating point tolerance
        if abs((unit * qty) - total) > 1e-6:
            return True

    return False
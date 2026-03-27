import easyocr
import re

reader = easyocr.Reader(['en'])

def extract_text_from_image(image_path):
    result = reader.readtext(image_path)
    text = " ".join([item[1] for item in result])
    return text

def extract_amount(text):
    matches = re.findall(r'\d+[.,]?\d*', text)
    amounts = [float(m.replace(',', '')) for m in matches if float(m) > 100]
    return max(amounts) if amounts else 0

def detect_price_mismatch(text):
    import re

    numbers = [float(x) for x in re.findall(r'\d+', text)]

    if len(numbers) >= 3:
        unit = numbers[0]
        qty = numbers[1]
        total = numbers[2]

        if unit * qty != total:
            return True

    return False
import sys, time, pathlib, re
import pandas as pd
import requests
from bs4 import BeautifulSoup


_num_re = re.compile(r"[^0-9]")
def _to_int(text: str | None) -> int | None:
    if not text:
        return None
    digits = _num_re.sub("", text)
    return int(digits) if digits else None
def scrape_one(url: str) -> dict[str, str]:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
    except Exception as exc:
        return {"Title": "", "Description": "", "Error": str(exc)}

    soup = BeautifulSoup(res.text, "html.parser")
    mileage = ""

    title = soup.title.string.strip() if soup.title else ""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    description = meta_desc["content"].strip() if meta_desc and meta_desc.get("content") else ""

    for div in soup.find_all("div", attrs={"data-testid": "detail"}):
        labels = div.find_all("p")
        if len(labels) >= 2:
            label = labels[0].get_text(strip=True)
            value = labels[1].get_text(strip=True)

            if "Przebieg" in label:
                mileage = value
                break
    price_tag = soup.find("span", class_="offer-price__number")
    price_val: int | None = _to_int(price_tag.get_text()) if price_tag else None
    description = ""
    desc_div = soup.find("div", class_="ooa-unlmzs e11t9j224")
    if desc_div:
        paragraphs = desc_div.find_all("p")
        description = "\n".join(p.get_text(strip=True) for p in paragraphs)

    return {"Title": title, "Przebieg": mileage, "Price": price_val, "Description": description, "Error": ""}

def main(input_path: str, output_path: str | None = None, throttle: float = 1.0):
    inp = pathlib.Path(input_path)
    if not output_path:
        output_path = inp.with_stem(inp.stem + "_scraped")

    print(f"Reading {inp} …")
    df = pd.read_excel(inp)

    sample_cols = scrape_one("https://example.org").keys()
    for col in sample_cols:
        if col not in df.columns:
            df[col] = pd.NA

    for idx, url in df.iloc[:, 0].items():
        if pd.isna(url) or not str(url).strip():
            continue

        print(f"[{idx}] scraping {url}")
        scraped = scrape_one(str(url))
        for col, val in scraped.items():
            df.at[idx, col] = val

        time.sleep(throttle)


    df["Przebieg"] = pd.to_numeric(df["Przebieg"], errors="coerce").astype("Int64")
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce").astype("Int64")

    print(f"Writing {output_path} …")
    df.to_excel(output_path, index=False)
    print("Done!  ➜", output_path)


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        print("Usage: python bulk_scraper.py input.xlsx [output.xlsx]")
        sys.exit(1)
    main(*sys.argv[1:])


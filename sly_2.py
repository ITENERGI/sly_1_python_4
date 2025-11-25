import requests
import pandas as pd
from datetime import datetime
import time


def get_tenders(edrpou, year):
    """Витягує тендери за ЄДРПОУ та роком через актуальне API ProZorro (2025)"""
    base_url = "https://public-api.prozorro.gov.ua/api/2.5/tenders"

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31T23:59:59"

    params = {
        "descending": "true",
        "limit": 10000000,
        # Фільтр за покупцем (ЄДРПОУ)
        "buyer_identifier": edrpou,
        # Фільтр за датою модифікації
        "date_modified_from": start_date,
        "date_modified_to": end_date
    }

    tenders = []
    offset = None
    attempt = 0
    max_attempts = 5

    print(f"Починаємо завантаження тендерів за {year} рік для ЄДРПОУ {edrpou}...")

    while True:
        if offset:
            params["offset"] = offset

        for attempt in range(max_attempts):
            response = requests.get(base_url, params=params, timeout=30)
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                print("Обмеження швидкості, чекаємо 10 сек...")
                time.sleep(10)
            else:
                print(f"Помилка {response.status_code}: {response.text}")
                time.sleep(5)
        else:
            print("Не вдалося отримати відповідь після кількох спроб")
            break

        data = response.json()
        batch = data.get("data", [])
        if not batch:
            break

        tenders.extend(batch)
        print(f"Отримано {len(batch)} тендерів, всього: {len(tenders)}")

        # Нова пагінація
        if "next_page" in data and "offset" in data["next_page"]:
            offset = data["next_page"]["offset"]
        else:
            break

        time.sleep(0.5)  # ввічлива затримка

    print(f"Завершено! Всього отримано {len(tenders)} тендерів.")
    return tenders


def analyze_tenders(tenders, edrpou, year):
    if not tenders:
        print("Немає тендерів для аналізу.")
        return

    records = []
    total_amount = 0

    for t in tenders:
        value = t.get("value", {}).get("amount", 0) or 0
        total_amount += value

        items = t.get("items", [])
        category_code = "N/A"
        category_desc = "N/A"
        if items:
            cls = items[0].get("classification", {})
            category_code = cls.get("id", "N/A")
            category_desc = cls.get("description", "N/A")

        records.append({
            "tenderID": t.get("tenderID", "N/A"),
            "id": t.get("id"),
            "title": t.get("title", "Без назви"),
            "value_amount": value,
            "currency": t.get("value", {}).get("currency", "UAH"),
            "category_code": category_code,
            "category_desc": category_desc,
            "dateModified": t["dateModified"],
            "status": t.get("status")
        })

    df = pd.DataFrame(records)

    # Групування за категоріями
    summary = df.groupby(["category_code", "category_desc"]).agg(
        Кількість_тендерів=("tenderID", "count"),
        Загальна_сума=("value_amount", "sum"),
        Середня_сума=("value_amount", "mean")
    ).round(0).sort_values("Загальна_сума", ascending=False)

    print(f"\n{'=' * 50}")
    print(f"АНАЛІЗ ЗАКУПІВЕЛЬ ЗА {year} РІК | ЄДРПОУ {edrpou}")
    print(f"{'=' * 50}")
    print(f"Всього тендерів: {len(tenders)}")
    print(f"Загальна сума: {total_amount:,.0f} грн")
    print(f"Топ категорій:")
    print(summary.head(10).to_string())

    # Збереження
    df.to_csv(f"tenders_{edrpou}_{year}.csv", index=False, encoding="utf-8-sig")
    summary.to_csv(f"analytics_summary_{edrpou}_{year}.csv", encoding="utf-8-sig")

    print(f"\nФайли збережено:")
    print(f"   • tenders_{edrpou}_{year}.csv")
    print(f"   • analytics_summary_{edrpou}_{year}.csv")

    return df, summary


# ЗАПУСК
if __name__ == "__main__":
    EDRPOU = "00034074"  # ← заміни на потрібний
    YEAR = 2025

    tenders = get_tenders(EDRPOU, YEAR)
    if tenders:
        analyze_tenders(tenders, EDRPOU, YEAR)
    else:
        print("Тендери не знайдено :(")
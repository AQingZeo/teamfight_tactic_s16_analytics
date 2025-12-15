import asyncio
import os
import pandas as pd
from playwright.async_api import async_playwright

# Adjust these if needed
INPUT_CSV = "champions_s16.csv"
OUTPUT_CSV = "champions_s16.csv"

# Path to your Edge profile ROOT (not the Default subfolder)
EDGE_USER_DATA_DIR = "/Users/zeoxzhang/Library/Application Support/Microsoft Edge"


def url_name(name: str) -> str:
    """Convert champion name into tactics.tools URL slug."""
    slug = name.lower()
    slug = slug.replace(" ", "-")
    slug = slug.replace("'", "")
    slug = slug.replace("&", "")
    slug = slug.replace(".", "")
    return slug


async def scrape_items(champion_names):
    """
    Scrape top item trio per champion from tactics.tools.
    Returns dict: champion_name -> "item1,item2,item3"
    """
    results = {}

    async with async_playwright() as p:
        # Use persistent context with your real Edge profile
        browser = await p.chromium.launch_persistent_context(
            user_data_dir=EDGE_USER_DATA_DIR,
            channel="msedge",          # use installed Edge
            headless=False,            # MUST be visible, to avoid blocking
            slow_mo=150,               # slows actions slightly; more human
        )

        # Use existing page or create one
        pages = browser.pages
        if pages:
            page = pages[0]
        else:
            page = await browser.new_page()

        for champ in champion_names:
            slug = url_name(champ)
            url = f"https://tactics.tools/vie/units/{slug}"
            print(f"\nScraping {champ} → {url}")

            try:
                await page.goto(url, timeout=60000)
                await page.wait_for_load_state("networkidle")

                content = await page.content()
                has_trios_tab = "ITEM TRIOS" in content or "Item trios" in content
                print("Has 'ITEM TRIOS' text?", has_trios_tab)

                if not has_trios_tab:
                    # Take a screenshot for debugging
                    screenshot_path = f"debug_{slug}.png"
                    await page.screenshot(path=screenshot_path, full_page=True)
                    print(f"ITEM TRIOS not found for {champ}, screenshot saved to {screenshot_path}")
                    results[champ] = ""
                    continue

                # Click the "Item trios" tab.
                # Try several variants to be safe.
                clicked = False
                for label in ["ITEM TRIOS", "Item trios", "Item Trios", "TRIOS"]:
                    try:
                        await page.get_by_text(label, exact=False).click(timeout=5000)
                        clicked = True
                        break
                    except Exception:
                        continue

                if not clicked:
                    print(f"Could not click ITEM TRIOS tab for {champ}")
                    results[champ] = ""
                    continue

                # Give time for the table to render
                await page.wait_for_timeout(2000)

                # Try to find the first row in the item trios table.
                # This is intentionally generic so you can tweak with DevTools:
                # - First try a <table> structure.
                # - If not, fall back to a generic 'row' div.
                item_names = []

                # Attempt 1: real table
                try:
                    row = page.locator("table tbody tr").first
                    await row.wait_for(timeout=5000)
                    icons = row.locator("img[alt]")
                    count = await icons.count()
                    for i in range(count):
                        alt = await icons.nth(i).get_attribute("alt")
                        if alt:
                            item_names.append(alt)
                except Exception:
                    pass

                # Attempt 2: generic flex row
                if not item_names:
                    try:
                        row = page.locator("div").filter(has_text="Top 4").locator("..")
                        # This is a heuristic; if it fails we continue below.
                    except Exception:
                        pass

                # Attempt 3: any first row with multiple item icons
                if not item_names:
                    rows = page.locator("img[alt]").locator("xpath=../..")
                    # This is rough — we will just take the first grouping of >=3 icons
                    groups_checked = 0
                    icons_all = await page.query_selector_all("img[alt]")
                    if len(icons_all) >= 3:
                        # naive grouping: take first 3 icons
                        for img in icons_all[:3]:
                            alt = await img.get_attribute("alt")
                            if alt:
                                item_names.append(alt)

                # Deduplicate & clean
                cleaned = []
                for item in item_names:
                    if item not in cleaned:
                        cleaned.append(item)

                cleaned = cleaned[:3]
                while len(cleaned) < 3:
                    cleaned.append("")

                items_str = ",".join(cleaned)
                print(f"{champ} → {items_str}")
                results[champ] = items_str

            except Exception as e:
                print(f"FAILED for {champ}: {e}")
                # Optional: screenshot on failure
                try:
                    screenshot_path = f"error_{slug}.png"
                    await page.screenshot(path=screenshot_path, full_page=True)
                    print(f"Saved error screenshot to {screenshot_path}")
                except Exception:
                    pass
                results[champ] = ""

        await browser.close()
    return results


async def main():
    # Load CSV
    df = pd.read_csv(INPUT_CSV)
    print("Detected columns:", df.columns.tolist())

    # normalize columns (strip whitespace / BOM)
    df.columns = df.columns.str.strip().str.replace("\ufeff", "", regex=False)
    print("Cleaned columns:", df.columns.tolist())

    if "name" not in df.columns:
        raise RuntimeError("No 'name' column found in CSV after cleaning.")

    champion_names = df["name"].tolist()

    # Scrape item trios
    item_dict = await scrape_items(champion_names)

    # Insert into 'items' column
    if "items" not in df.columns:
        df["items"] = ""
    df["items"] = df["name"].map(item_dict)

    # Save back to CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nUpdated CSV saved → {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from typing import List, Dict

# ============================================================================
# METRO URLS - Add your Wikipedia URLs here
# ============================================================================
METROS = [
    {
        'name': 'Atlanta',
        'url': 'https://en.wikipedia.org/wiki/Atlanta_metropolitan_area'
    },
    {
        'name': 'Houston',
        'url': 'https://en.wikipedia.org/wiki/Greater_Houston'
    },
    {
        'name': 'Dallas',
        'url': 'https://en.wikipedia.org/wiki/Dallas%E2%80%93Fort_Worth_metroplex'
    },
    {
        'name': 'Miami',
        'url': 'https://en.wikipedia.org/wiki/Miami_metropolitan_area'
    },
    {
        'name': 'Charlotte',
        'url': 'https://en.wikipedia.org/wiki/Charlotte_metropolitan_area'
    },
    {
        'name': 'Washington DC',
        'url': 'https://en.wikipedia.org/wiki/Washington_metropolitan_area'
    },
    {
        'name': 'Phoenix',
        'url': 'https://en.wikipedia.org/wiki/Phoenix_metropolitan_area'
    },
    {
        'name': 'Philadelphia',
        'url': 'https://en.wikipedia.org/wiki/Philadelphia_metropolitan_area'
    },
    # Add more metros here...
]

# ============================================================================
# Configuration
# ============================================================================
INTERPOLATE = True  # Set to True to interpolate annual values between census years
OUTPUT_FILE = 'metro_population_data.csv'


def clean_number(text: str) -> int:
    """Remove commas and convert to integer."""
    if not text or text == '—' or text == '':
        return None
    # Remove commas and any other non-digit characters except digits
    cleaned = re.sub(r'[^\d]', '', text)
    try:
        return int(cleaned)
    except ValueError:
        return None


def scrape_metro_data(metro_name: str, url: str) -> pd.DataFrame:
    """Scrape census population data for a single metro from Wikipedia."""
    print(f"Scraping {metro_name}...")

    try:
        # Fetch the page with proper headers to avoid 403 errors
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the census table
        table = soup.find('table', class_='us-census-pop')
        if not table:
            print(f"  ⚠️  Warning: Could not find census table for {metro_name}")
            return pd.DataFrame()

        # Parse table rows
        rows = []
        for tr in table.find_all('tr'):
            cells = tr.find_all(['td', 'th'])
            if len(cells) >= 2:
                row_data = [cell.get_text(strip=True) for cell in cells]
                rows.append(row_data)

        if not rows:
            print(f"  ⚠️  Warning: No data rows found for {metro_name}")
            return pd.DataFrame()

        # Create DataFrame
        df = pd.DataFrame(rows[1:], columns=rows[0] if rows else None)

        # Clean up the dataframe
        if 'Census' in df.columns and 'Pop.' in df.columns:
            # Filter to only Census and Pop columns
            df = df[['Census', 'Pop.']].copy()

            # Clean the Census column (extract year)
            df['Census'] = df['Census'].str.extract(r'(\d{4})', expand=False)

            # Clean the Pop column (remove commas, convert to int)
            df['Pop.'] = df['Pop.'].apply(clean_number)

            # Remove rows with missing data
            df = df.dropna()

            # Convert Census to int
            df['Census'] = df['Census'].astype(int)

            # Add metro name column
            df.insert(0, 'Metro', metro_name)

            # Rename columns for clarity
            df.columns = ['Metro', 'Year', 'Population']

            print(f"  ✓ Scraped {len(df)} census data points")
            return df
        else:
            print(f"  ⚠️  Warning: Expected columns not found for {metro_name}")
            return pd.DataFrame()

    except Exception as e:
        print(f"  ✗ Error scraping {metro_name}: {str(e)}")
        return pd.DataFrame()


def interpolate_annual_data(df: pd.DataFrame) -> pd.DataFrame:
    """Interpolate annual population values between census years."""
    print("\nInterpolating annual values...")

    interpolated_dfs = []

    for metro in df['Metro'].unique():
        metro_data = df[df['Metro'] == metro].sort_values('Year').reset_index(drop=True)

        interpolated_rows = []

        for i in range(len(metro_data) - 1):
            start_year = metro_data.loc[i, 'Year']
            end_year = metro_data.loc[i + 1, 'Year']
            start_pop = metro_data.loc[i, 'Population']
            end_pop = metro_data.loc[i + 1, 'Population']

            # Calculate annual increment (linear interpolation)
            years_diff = end_year - start_year
            pop_diff = end_pop - start_pop
            annual_increment = pop_diff / years_diff

            # Create rows for each year
            for year_offset in range(years_diff):
                year = start_year + year_offset
                population = int(start_pop + (annual_increment * year_offset))
                interpolated_rows.append({
                    'Metro': metro,
                    'Year': year,
                    'Population': population
                })

        # Add the final census year
        interpolated_rows.append({
            'Metro': metro,
            'Year': metro_data.loc[len(metro_data) - 1, 'Year'],
            'Population': metro_data.loc[len(metro_data) - 1, 'Population']
        })

        interpolated_dfs.append(pd.DataFrame(interpolated_rows))

    result_df = pd.concat(interpolated_dfs, ignore_index=True)
    print(f"  ✓ Interpolated to {len(result_df)} annual data points")
    return result_df


def main():
    """Main function to scrape all metros and combine data."""
    print("=" * 60)
    print("Wikipedia Metro Population Scraper")
    print("=" * 60)

    all_data = []

    # Scrape each metro
    for i, metro in enumerate(METROS):
        df = scrape_metro_data(metro['name'], metro['url'])
        if not df.empty:
            all_data.append(df)

        # Respectful delay between requests (except after the last one)
        if i < len(METROS) - 1:
            time.sleep(1.5)

    if not all_data:
        print("\n✗ No data was scraped. Please check your URLs.")
        return

    # Combine all metro data
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n✓ Combined data for {len(METROS)} metros")
    print(f"  Total census data points: {len(combined_df)}")

    # Interpolate if requested
    if INTERPOLATE:
        combined_df = interpolate_annual_data(combined_df)

    # Sort by year and metro
    combined_df = combined_df.sort_values(['Year', 'Population'], ascending=[True, False])

    # Filter to just get years after 1980
    combined_df = combined_df[combined_df['Year'] >= 1980]

    # Save to CSV
    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✓ Data saved to {OUTPUT_FILE}")

    # Print summary statistics
    print("\n" + "=" * 60)
    print("Summary:")
    print("=" * 60)
    print(f"Metros: {combined_df['Metro'].nunique()}")
    print(f"Year range: {combined_df['Year'].min()} - {combined_df['Year'].max()}")
    print(f"Total data points: {len(combined_df)}")
    print("\nFirst few rows:")
    print(combined_df.head(10))
    print("\nLast few rows:")
    print(combined_df.tail(10))


if __name__ == "__main__":
    main()

import pandas as pd

# Read the existing CSV
df = pd.read_csv('metro_population_data.csv')

# 2020 census values (from the data)
pop_2020 = {
    'Dallas': 7637387,
    'Houston': 7122240,
    'Washington DC': 6385162,
    'Philadelphia': 6245051,
    'Miami': 6138333,
    'Atlanta': 6089815,
    'Phoenix': 4845832,
    'Charlotte': 2660329
}

# 2024 estimates (from user's screenshot)
pop_2024 = {
    'Atlanta': 6409047,
    'Charlotte': 2883370,
    'Dallas': 8344032,
    'Houston': 7796182,
    'Miami': 6457988,
    'Philadelphia': 6330422,
    'Phoenix': 5186958,
    'Washington DC': 6437907
}

# Remove any existing 2021-2024 data to avoid duplicates
df = df[df['Year'] <= 2020]

# Calculate interpolated values and create new rows
new_rows = []

for metro in pop_2020.keys():
    start_pop = pop_2020[metro]
    end_pop = pop_2024[metro]

    # Calculate annual increment
    annual_increment = (end_pop - start_pop) / 4

    # Create rows for 2021-2024
    for year_offset in range(1, 5):  # 1, 2, 3, 4
        year = 2020 + year_offset
        if year == 2024:
            population = end_pop  # Use exact 2024 value
        else:
            population = int(start_pop + (annual_increment * year_offset))

        new_rows.append({
            'Metro': metro,
            'Year': year,
            'Population': population
        })

# Add new rows to dataframe
new_df = pd.DataFrame(new_rows)
df = pd.concat([df, new_df], ignore_index=True)

# Sort by year and population
df = df.sort_values(['Year', 'Population'], ascending=[True, False])

# Save back to CSV
df.to_csv('metro_population_data.csv', index=False)

print("✓ Added 2021-2024 data with interpolation")
print(f"Total rows: {len(df)}")
print("\nLast 20 rows:")
print(df.tail(20))

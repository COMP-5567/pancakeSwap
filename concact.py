import pandas as pd
import os

# List of file names to merge
files = [f'pancakeswap_tokens{i}' for i in range(11)]  # pancakeswap_pairs, pancakeswap_pairs1 to pancakeswap_pairs10

# Initialize an empty list to store DataFrames
dfs = []

# Read each CSV file and append to the list
for file in files:
    try:
        df = pd.read_csv(f'{file}.csv')
        dfs.append(df)
        print(f'Successfully loaded {file}.csv')
    except FileNotFoundError:
        print(f'Warning: {file}.csv not found, skipping...')
    except Exception as e:
        print(f'Error loading {file}.csv: {str(e)}')

# Check if any files were loaded
if not dfs:
    print('No files were loaded. Please check if the CSV files exist.')
else:
    # Concatenate all DataFrames
    merged_df = pd.concat(dfs, ignore_index=True)

    # Save the merged DataFrame to a new CSV file
    output_file = 'pancakeswap_tokens10000.csv'
    merged_df.to_csv(output_file, index=False)
    print(f'Merged data saved to {output_file}')
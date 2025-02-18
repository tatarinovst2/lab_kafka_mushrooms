import pandas as pd
import argparse
import sys


def stratified_sample(input_file, output_file, total_rows):
    df = pd.read_csv(input_file)

    if 'class' not in df.columns:
        print("Error: The CSV file must contain a 'class' column.")
        sys.exit(1)

    classes = df['class'].unique()
    if len(classes) != 2:
        print("Error: There must be exactly 2 classes in the 'class' column.")
        print(f"Found classes: {classes}")
        sys.exit(1)

    if total_rows % 2 != 0:
        print("Warning: Total number of rows requested is not even. It will be adjusted to the nearest even number.")
        total_rows -= 1

    samples_per_class = total_rows // 2

    samples_available = df['class'].value_counts()
    min_available = samples_available.min()
    if samples_per_class > min_available:
        print(f"Warning: Requested {samples_per_class} samples per class, but only {min_available} available for at least one class.")
        samples_per_class = min_available
        total_rows = samples_per_class * 2
        print(f"Adjusted total rows to {total_rows} ({samples_per_class} per class).")

    sampled_df = df.groupby('class').apply(lambda x: x.sample(n=samples_per_class, random_state=42)).reset_index(drop=True)

    sampled_df = sampled_df.sample(frac=1, random_state=42).reset_index(drop=True)

    sampled_df.to_csv(output_file, index=False)
    print(f"Successfully sampled {total_rows} rows with a 50/50 class distribution.")
    print(f"Sampled data saved to '{output_file}'.")


def main():
    parser = argparse.ArgumentParser(description="Stratified Sampling of CSV to achieve a 50/50 class distribution.")
    parser.add_argument('input_file', type=str, help='Path to the input CSV file.')
    parser.add_argument('output_file', type=str, help='Path to save the sampled CSV file.')
    parser.add_argument('total_rows', type=int, help='Total number of rows to sample (must be even for 50/50 split).')

    args = parser.parse_args()

    if args.total_rows <= 0:
        print("Error: The total number of rows must be a positive integer.")
        sys.exit(1)

    stratified_sample(args.input_file, args.output_file, args.total_rows)


if __name__ == "__main__":
    main()

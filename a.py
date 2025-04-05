import dask.dataframe as dd
# Load large dataset from CSV
file_path = r"C:\Users\vaish\Downloads\amazon.csv\meesho Orders Aug.csv" # Replace with your actual file path
df = dd.read_csv(r"C:\Users\vaish\Downloads\amazon.csv\meesho Orders Aug.csv")

# Ensure column names are properly formatted
df.columns = df.columns.str.strip()

# Exploratory Data Analysis (EDA)
print(df.dtypes)
print(df.head())

# Aggregation example: Group by 'Reason for Credit Entry' and compute count, mean, and sum
agg_df = df.groupby("Reason for Credit Entry").agg({
    "Quantity": "count",
    "Supplier Discounted Price (Incl GST and Commision)": ["mean", "sum"]
})

# Compute and display results
print(agg_df.compute())

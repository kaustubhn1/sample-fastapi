import polars as pl
from fastapi import HTTPException, status


def load_and_validate_data(csv_file):
    # Load CSV data into Polars DataFrame
    pl_df = pl.read_csv(csv_file)

    # Perform data validations
    if pl_df.is_empty():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "CSV Data is empty"},
        )

    # Define expected column names
    expected_columns = [
        "item_id",
        "quantity",
        "date_production_start",
        "date_received_into_inventory",
        "date_shipped_from_inventory",
    ]

    # Check if all expected columns are present in the DataFrame
    missing_columns = [col for col in expected_columns if col not in pl_df.columns]
    if missing_columns:
        missing_columns_msg = ", ".join(missing_columns)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Missing columns: {missing_columns_msg}"},
        )

    # Check for missing values in specific columns
    date_columns = [
        "date_production_start",
        "date_received_into_inventory",
        "date_shipped_from_inventory",
    ]
    for col in date_columns:
        null_count = pl_df[col].null_count()
        if null_count > 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": f"Column '{col}' contains {null_count} missing values"
                },
            )

    # Convert Polars DataFrame to Pandas DataFrame
    pd_df = pl_df.to_pandas()

    # Additional validation: Ensure quantity values make sense
    if not pd_df["quantity"].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Invalid quantity values"},
        )

    return pd_df

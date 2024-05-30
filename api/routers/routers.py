from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import select, func
from typing import Optional
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status
from api.db.models import get_db, get_engine, insert_into_postgres, InventoryData
from .helper import load_and_validate_data
from fastapi import Depends, UploadFile, File, HTTPException, status
import shutil
import pandas as pd
from fastapi import Depends, HTTPException
from fastapi import Depends, HTTPException
from typing import Optional
from datetime import datetime
from sqlalchemy import cast, TIMESTAMP

router = APIRouter()


@router.post("/upload-and-validate")
async def upload_and_validate(
    file: UploadFile = File(...),
    db=Depends(get_db),
    engine=Depends(get_engine),
):
    try:
        if (
            not file.filename
            or file.filename == ""
            or file.filename.isspace()
            or file.filename == '""'
            or file.size == 0
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "No file uploaded"},
            )
        # Save the uploaded file to a temporary location
        with open("uploaded_file.csv", "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Load and validate data
        pd_df = load_and_validate_data("uploaded_file.csv")

        # Add transaction_timestamp column and insert data into PostgreSQL
        pd_df["transaction_timestamp"] = pd.to_datetime("now")
        insert_into_postgres(pd_df, "inventory_data", engine)

        return {"message": "Data uploaded and validated successfully"}
    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail={"error": "File not found"}
        )
    except HTTPException:
        raise  # Re-raise HTTPException to maintain custom detail messages
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail={"error": str(e)}
        )
        
        
@router.get("/item-total-quantity/{item_id}")
async def get_item_total_quantity(
    start_date: datetime,
    end_date: datetime,
    item_id: int,
    db=Depends(get_db),
    engine=Depends(get_engine),
):
    try:
        async with AsyncSession(engine) as session:
            # Set timezone to None for start_date and end_date
            start_date = start_date.replace(tzinfo=None)
            end_date = end_date.replace(tzinfo=None)
            # Modify your SQL query to use start_date and end_date
            stmt = (
                select(
                    InventoryData.item_id,
                    func.sum(InventoryData.quantity).label("total_quantity"),
                )
                .where(
                    InventoryData.item_id == item_id,
                    InventoryData.date_received_into_inventory.between(
                        start_date,
                        end_date,
                    ),
                )
                .group_by(InventoryData.item_id)
            )

            print(stmt)  # Print the SQL statement for debugging purposes

            result = await session.execute(stmt)
            row = result.first()

            print(row)  # Print the row for debugging purposes

            if row:
                return {"item_id": row[0], "total_quantity": row[1]}
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No data found for item_id {item_id} between {start_date} and {end_date}",
                )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail={"error": str(e)}
        )

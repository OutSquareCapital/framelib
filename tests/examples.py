import duckdb
import narwhals as nw
import polars as pl

import framelib as fl


class Sales(fl.Schema):
    """Schema for the sales."""

    transaction_id = fl.UInt32(primary_key=True)
    customer_id = fl.UInt16()
    amount = fl.Float32()


class Analytics(fl.DataBase):
    """Embedded DuckDB database for analytics. Contain a sales table."""

    sales = fl.Table(Sales)


class MyProject(fl.Folder):
    """Root folder for the project. __source__ automatically set to Path("myproject")"""

    ## Files are defined as attributes
    raw_sales = fl.CSV(model=Sales)  # Located at 'myproject/raw_sales.csv'

    ## Instantiate the embedded database
    analytics_db = Analytics()  # Located at 'myproject/analytics_db.ddb'


def create_structure() -> None:
    MyProject.source().mkdir(parents=True, exist_ok=True)
    print(f"✅ Project structure created at: {MyProject.source().as_posix()}")


def create_mock_sales_data() -> None:
    mock_sales_data = pl.DataFrame(
        {
            "transaction_id": [101, 102, 103],
            "customer_id": [1, 2, 1],
            "amount": [120.50, 75.00, 50.25],
        }
    )
    MyProject.raw_sales.write(mock_sales_data, retries=2)
    print(f"✅ Raw sales data written to: {MyProject.raw_sales.source}")


def load_data_into_db() -> None:
    raw_df: pl.LazyFrame = MyProject.raw_sales.scan_cast()

    with MyProject.analytics_db as db:
        db.sales.create_or_replace_from(raw_df)
        print("✅ Data loaded into DuckDB.")

        ## Query the data directly from the database using the Narwhals API
        report_df: duckdb.DuckDBPyRelation = (
            db.sales.scan()
            .group_by("customer_id")
            .agg(
                total_spent=nw.col("amount").sum(),
                transaction_count=nw.len(),
            )
            .to_native()
        )
        print("\n📊 Generated Report:")
        print(report_df)


def show_inheritance_and_tree() -> None:
    class ProductionData(fl.Folder):
        sales = fl.CSV(model=Sales)

    class Reports(ProductionData):
        """
        Reports folder inheriting from ProductionData.
        No schema defined in the files, so they default to framelib.Schema.
        """

        sales = fl.CSV()
        sales_formatted = fl.Parquet()

    print("\n📁 Inheritance Example:\n")
    print(ProductionData.sales.source)
    print(Reports.sales.source)
    print(Reports.sales_formatted.source)
    print("\n📂 Project Structure:\n")
    print(Reports.show_tree())


def read_and_cast() -> None:
    print("\n📋 Raw Sales Data:")
    print(MyProject.raw_sales.read().schema)
    print("Casted to the defined schema:")
    print(MyProject.raw_sales.read_cast().schema)


def append_data() -> None:
    new_sales = pl.DataFrame(
        {
            "transaction_id": [104, 105],
            "customer_id": [3, 2],
            "amount": [200.00, 150.75],
        }
    )

    with MyProject.analytics_db as db:
        ## High-level methods simplify common database operations
        print("\n📦 Sales Data in DB before insert_into:")
        print(db.sales.scan().to_native())
        print("\n📦 Sales Data in DB after insert_into:")
        print(db.sales.insert_into(new_sales).scan().to_native())
        ## Intelligently insert rows, skipping duplicates based on the primary key
        print("\n📦 Sales Data in DB after insert_or_ignore (no duplicates):")
        print(db.sales.insert_or_ignore(new_sales).scan().to_native())
        print("\n📦 Sales Data in DB after truncate:")
        print(db.sales.truncate().scan().to_native())


def clean_project() -> None:
    MyProject.clean()
    print("\n✅ Project structure cleaned up.")
    try:
        MyProject.raw_sales.read()
    except FileNotFoundError:
        print("✅ Confirmed: Raw sales file no longer exists.")


if __name__ == "__main__":
    create_structure()
    create_mock_sales_data()
    load_data_into_db()
    show_inheritance_and_tree()
    read_and_cast()
    append_data()
    clean_project()

import dataframely as dy
import narwhals as nw
import polars as pl

import framelib as fl

## Schema for the raw input file (CSV)
# TODO: en faire un marimo notebook


class SalesFile(dy.Schema):
    transaction_id = dy.UInt32(nullable=False)
    customer_id = dy.UInt16(nullable=False)
    amount = dy.Float32(nullable=False)


## Schema for the database table


class SalesDB(fl.Schema):
    transaction_id = fl.UInt32(primary_key=True)
    customer_id = fl.UInt16()
    amount = fl.Float32()


### Declare Your Project Layout
## Declare the embedded database and its tables
class Analytics(fl.DataBase):
    sales = fl.Table(SalesDB)


## Declare the root folder for our project
# Automatically set the __source__ as Path("myproject)
class MyProject(fl.Folder):
    ## Files are defined as attributes
    raw_sales = fl.CSV(model=SalesFile)  # Located at 'myproject/raw_sales.csv'

    ## Instantiate the embedded database
    analytics_db = Analytics()  # Located at 'myproject/analytics_db.ddb'


def create_structure():
    ## Create the folder structure on disk
    MyProject.source().mkdir(parents=True, exist_ok=True)
    print(f"✅ Project structure created at: {MyProject.source().as_posix()}")


### Use the Defined Layout


## Mock some data for the example
def create_mock_sales_data():
    mock_sales_data = pl.DataFrame(
        {
            "transaction_id": [101, 102, 103],
            "customer_id": [1, 2, 1],
            "amount": [120.50, 75.00, 50.25],
        }
    )

    ## 1. Write data to the CSV, automatically casting to the `SalesFile` schema

    MyProject.raw_sales.write_cast(mock_sales_data)
    print(f"✅ Raw sales data written to: {MyProject.raw_sales.source}")


## 2. Scan the raw data and load it into the DuckDB database
def load_data_into_db():
    raw_df = MyProject.raw_sales.scan_cast()

    with MyProject.analytics_db as db:
        db.sales.create_or_replace_from(raw_df)
        print("✅ Data loaded into DuckDB.")

        ## 3. Query the data directly from the database using the Narwhals API
        report_df = (
            db.sales.read()
            .group_by("customer_id")
            .agg(
                total_spent=nw.sum("amount"),
                transaction_count=nw.len(),
            )
            .collect()
            .to_native()
        )
        print("\n📊 Generated Report:")
        print(report_df)


def show_inheritance_example():
    class ProductionData(fl.Folder):
        sales = fl.CSV(model=SalesFile)

    class Reports(ProductionData):  ## Inherits from ProductionData
        ## This file will be located at './production_data/v2/reports.parquet'
        sales = fl.CSV(dy.Schema)  # Located at './production_data/reports/sales.csv'
        # Located at './production_data/reports/sales_formatted.parquet'

        sales_formatted = fl.Parquet(dy.Schema)

    print("\n📁 Inheritance Example:")
    print(Reports.sales.source)

    print(Reports.sales_formatted.source)


def read_and_cast():
    print("\n📋 Raw Sales Data:")
    print(MyProject.raw_sales.read())
    print("casted to the defined schema:")
    print(MyProject.raw_sales.read_cast())


def append_and_show_tree():
    new_sales = pl.DataFrame(
        {
            "transaction_id": [104, 105],
            "customer_id": [3, 2],
            "amount": [200.00, 150.75],
        }
    )

    with MyProject.analytics_db as db:
        ## High-level methods simplify common database operations
        db.sales.append(new_sales)
        db.sales.truncate()
        ## Intelligently insert rows, skipping duplicates based on the primary key
        db.sales.insert_if_not_exists(new_sales)
    print(MyProject.show_tree())


if __name__ == "__main__":
    create_structure()
    create_mock_sales_data()
    read_and_cast()
    load_data_into_db()
    show_inheritance_example()
    append_and_show_tree()

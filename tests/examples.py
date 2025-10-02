from __future__ import annotations

import dataframely as dy
import narwhals as nw
import polars as pl

import framelib as fl


# --- Schema Definitions (the structure of the data) ---
class CustomersFile(dy.Schema):
    customer_id = dy.UInt16(nullable=False)
    name = dy.String(nullable=False)
    country = dy.String(nullable=False)


class SalesFile(dy.Schema):
    transaction_id = dy.UInt32(nullable=False)
    customer_id = dy.UInt16(nullable=False)
    amount = dy.Float32(nullable=False)


class CustomersDB(fl.Schema):
    customer_id = fl.UInt16(primary_key=True)
    name = fl.String()
    country = fl.String()


class SalesDB(fl.Schema):
    transaction_id = fl.UInt32(primary_key=True)
    customer_id = fl.UInt16()
    amount = fl.Float32()


class SalesReport(dy.Schema):
    country = dy.String(nullable=False)
    total_transactions = dy.Int64(nullable=False)
    total_revenue = dy.Float64(nullable=False)


# --- Layout Definitions via INHERITANCE ---
class Root(fl.Folder):
    """
    Root layout. Its path will automatically be './root'.
    It only contains the declaration of other layouts that inherit from it.
    """

    pass


class AnalyticsDB(fl.DataBase):
    """
    The DataBase has its own path logic (.ddb), so it doesn't inherit from Root.
    It's placed within a folder for logical grouping.
    """

    customers = fl.Table(CustomersDB)
    sales = fl.Table(SalesDB)


class InputData(Root):
    """Inherits from Root. Its path will automatically be './root/inputdata'."""

    customers = fl.CSV(model=CustomersFile)
    sales = fl.Parquet(model=SalesFile)
    analytics = AnalyticsDB()


class OutputReports(Root):
    """Inherits from Root. Its path will automatically be './root/outputreports'."""

    sales_report = fl.Json(model=SalesReport)


def setup_mock_data() -> None:
    """Creates the files and directories described in the layouts."""
    print("🔧 Setting up the data environment...")
    Root.source().mkdir(parents=True, exist_ok=True)
    InputData.source().mkdir(parents=True, exist_ok=True)
    OutputReports.source().mkdir(parents=True, exist_ok=True)

    InputData.customers.write(
        pl.DataFrame(
            {
                "customer_id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "David"],
                "country": ["USA", "USA", "France", "France"],
            }
        )
    )
    InputData.sales.write(
        pl.DataFrame(
            {
                "transaction_id": [101, 102, 103, 104, 105],
                "customer_id": [1, 2, 1, 3, 4],
                "amount": [1200.50, 25.00, 75.25, 300.00, 55.50],
            }
        )
    )
    print(f"✅ Test data created in '{Root.source()}'\n")


def run_analysis() -> None:
    """Orchestrates the pipeline using the declared API."""
    print("🚀 Starting the analysis pipeline...")

    # 1. Read data
    customers_df = InputData.customers.read_cast()
    sales_df = InputData.sales.read_cast()

    # 2. Load and Analyze
    with InputData.analytics as db:
        print("📥 Loading data into the database...")
        db.customers.create_or_replace_from(customers_df)
        db.sales.create_or_replace_from(sales_df)

        print("⚙️  Analyzing via the Narwhals API (without raw SQL)...")
        lazy_report = (
            db.customers.read()
            .join(db.sales.read(), on="customer_id")
            .group_by("country")
            .agg(
                nw.len().alias("total_transactions"),
                nw.sum("amount").alias("total_revenue"),
            )
            .sort("total_revenue", descending=True)
        )
        report_df = lazy_report.collect("polars").to_native()
        print(" -> Analysis complete.\n")

    # 3. Write the report
    print("📊 Final Report: Revenue by Country")
    print(report_df)

    OutputReports.sales_report.write_cast(report_df)
    print(f"\n✅ Report saved to: {OutputReports.sales_report.source}")

    # 4. Clean up the environment
    InputData.clean()
    OutputReports.clean()
    Root.clean()
    print("✅ Data environment cleaned up.")
    assert not Root.source().exists()


if __name__ == "__main__":
    setup_mock_data()
    run_analysis()

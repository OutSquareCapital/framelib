import marimo

__generated_with = "0.17.0"
app = marimo.App()

with app.setup(hide_code=True):
    import marimo as mo
    import narwhals as nw
    import polars as pl

    import framelib as fl

    class Sales(fl.Schema):
        transaction_id = fl.UInt32(primary_key=True)
        customer_id = fl.UInt16()
        amount = fl.Float32()

    class Analytics(fl.DataBase):
        sales = fl.Table(Sales)

    class MyProject(fl.Folder):
        raw_sales = fl.CSV(model=Sales)
        analytics_db = Analytics()


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Declare Your Data Architecture

    Root folder for the project automatically set to **Path("myproject")**

    ```python
    import framelib as fl

    class Sales(fl.Schema):
        #Schema for the sales.

        transaction_id = fl.UInt32(primary_key=True)
        customer_id = fl.UInt16()
        amount = fl.Float32()


    class Analytics(fl.DataBase):
        #Embedded DuckDB database for analytics. Contain a sales table.

        sales = fl.Table(Sales)


    class MyProject(fl.Folder):
        ## Files are defined as attributes
        raw_sales = fl.CSV(model=Sales)  # Located at 'myproject/raw_sales.csv'

        ## Instantiate the embedded database
        analytics_db = Analytics()  # Located at 'myproject/analytics_db.ddb'
    ```
    """,
    )


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Create the structure on disk

    Call the source() method to directly interact with the underlying path
    """,
    )


@app.cell
def _():
    MyProject.source().mkdir(parents=True, exist_ok=True)
    MyProject.source().as_posix()


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Create mock sales data

    Write data to the CSV, automatically passing the path argument.

    Since write/read/scan properties returns partials, pass any native polars argument with IDE support for documentation and argument validity.
    """,
    )


@app.cell
def _():
    mock_sales_data = pl.DataFrame(
        {
            "transaction_id": [101, 102, 103],
            "customer_id": [1, 2, 1],
            "amount": [120.50, 75.00, 50.25],
        },
    )
    MyProject.raw_sales.write(mock_sales_data, retries=2)
    MyProject.raw_sales.read()


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Load data into the DuckDB database and generate a report

    Query the data directly from the database using the Narwhals API.

    You can then easily convert it to polars for example.

    📊 Generated Report:
    """,
    )


@app.cell
def _():
    def get_report(db: Analytics) -> pl.DataFrame:
        return (
            db.sales.create_or_replace_from(MyProject.raw_sales.scan_cast())
            .scan()
            .group_by("customer_id")
            .agg(
                total_spent=nw.col("amount").sum(),
                transaction_count=nw.len(),
            )
            .to_native()
            .pl()
        )

    MyProject.analytics_db.apply(get_report)


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Read and cast data

    Reading the data directly from the database will give you this schema:
    """,
    )


@app.cell
def _():
    MyProject.raw_sales.read().schema


@app.cell(hide_code=True)
def _():
    mo.md(r"""Once casted to the defined schema:""")


@app.cell
def _():
    MyProject.raw_sales.read_cast().schema


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Show the project structure


    the Reports folder here inerhit from ProductionData.

    No schema defined in the files, so they default to framelib.Schema.
    """,
    )


@app.cell
def _():
    class ProductionData(fl.Folder):
        sales = fl.CSV(model=Sales)

    class Reports(ProductionData):
        sales = fl.CSV()
        sales_formatted = fl.Parquet()

    print("\n📁 Inheritance Example:\n")
    print(ProductionData.sales.source)
    print(Reports.sales.source)
    print(Reports.sales_formatted.source)
    print("\n📂 Project Structure:\n")
    print(Reports.show_tree())


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Append data and perform various database operations

    High-level methods simplify common database operations
    """,
    )


@app.cell
def _():
    new_sales = pl.DataFrame(
        {
            "transaction_id": [104, 105],
            "customer_id": [3, 2],
            "amount": [200.00, 150.75],
        },
    )

    def database_op(dba: Analytics):
        print("\n📦 Sales Data in DB before insert_into:")
        print(dba.sales.scan().to_native())
        print("\n📦 Sales Data in DB after insert_into:")
        print(dba.sales.insert_into(new_sales).scan().to_native())
        ## Intelligently insert rows, skipping duplicates based on the primary key
        print("\n📦 Sales Data in DB after insert_or_ignore (no duplicates):")
        print(dba.sales.insert_or_ignore(new_sales).scan().to_native())
        print("\n📦 Sales Data in DB after truncate:")
        print(dba.sales.truncate().scan().to_native())

    MyProject.analytics_db.apply(database_op).close()


@app.cell(hide_code=True)
def _():
    mo.md(r"""### Clean up the project structure""")


@app.cell
def _():
    MyProject.clean()
    print("\n✅ Project structure cleaned up.")
    try:
        MyProject.raw_sales.read()
    except FileNotFoundError:
        print("✅ Confirmed: Raw sales file no longer exists.")


if __name__ == "__main__":
    app.run()

"""
Example Marimo notebook: sales_report.py

This demonstrates how to write a Marimo notebook that works well with
marimo-jupyter-scheduler:

  1. Read schedule parameters from environment variables (MARIMO_PARAM_*)
  2. Perform the actual work (here: synthesise some data)
  3. Build a rich output — the scheduler captures this as HTML

Run manually:
    marimo export html examples/sales_report.py -o /tmp/report.html

Run via scheduler:
    Add to a *.marimo-schedule.yml file (see sample.marimo-schedule.yml)
"""

import marimo

__generated_with = "0.22.0"
app = marimo.App(width="medium", app_title="Daily Sales Report")


@app.cell
def _imports():
    import os
    import datetime
    import marimo as mo
    return datetime, mo, os


@app.cell
def _parameters(mo, os, datetime):
    """Read parameters injected by the scheduler as environment variables."""
    # When run via marimo-jupyter-scheduler, parameters are passed as:
    #   MARIMO_PARAM_DATE=2024-01-15
    #   MARIMO_PARAM_REGION=EMEA
    report_date_str = os.environ.get(
        "MARIMO_PARAM_DATE",
        datetime.date.today().isoformat(),
    )
    region = os.environ.get("MARIMO_PARAM_REGION", "ALL")

    try:
        report_date = datetime.date.fromisoformat(report_date_str)
    except ValueError:
        report_date = datetime.date.today()

    mo.md(f"""
    ## Parameters
    | Parameter | Value |
    |-----------|-------|
    | Date      | `{report_date}` |
    | Region    | `{region}` |
    """)
    return region, report_date, report_date_str


@app.cell
def _generate_data(report_date, region):
    """Generate synthetic sales data (replace with real DB query)."""
    import random
    random.seed(hash(str(report_date) + region))

    products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Service Z"]
    data = {
        "product": products,
        "units_sold": [random.randint(50, 500) for _ in products],
        "revenue_eur": [random.randint(1000, 20000) for _ in products],
    }
    return data, products


@app.cell
def _summary(mo, data, report_date, region):
    """Display a summary table."""
    total_revenue = sum(data["revenue_eur"])
    total_units = sum(data["units_sold"])

    mo.md(f"""
    ## Daily Sales Report — {report_date} ({region})

    **Total Revenue:** €{total_revenue:,}
    **Total Units Sold:** {total_units:,}
    """)


@app.cell
def _table(mo, data):
    """Render the data as a table."""
    rows = list(zip(data["product"], data["units_sold"], data["revenue_eur"]))
    table_md = "| Product | Units Sold | Revenue (€) |\n|---------|------------|-------------|\n"
    for product, units, rev in rows:
        table_md += f"| {product} | {units:,} | €{rev:,} |\n"
    mo.md(table_md)


@app.cell
def _footer(mo, report_date):
    """Add a timestamp footer."""
    import datetime
    generated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    mo.md(f"*Report generated at {generated_at} for date {report_date}.*")


if __name__ == "__main__":
    app.run()

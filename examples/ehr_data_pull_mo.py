import marimo

__generated_with = "0.22.0"
app = marimo.App(
    width="medium",
    layout_file="layouts/ehr_data_pull_mo.slides.json",
    sql_output="pandas",
)


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import os
    import sqlalchemy

    _password = os.environ.get("POSTGRES_PASSWORD", "example")
    DATABASE_URL = f"postgresql://postgres:{_password}@localhost:5432/ehrexample"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return engine, os


@app.cell
def _(engine, mo):
    df = mo.sql(
        f"""
        SELECT * FROM ehr.patients
        """,
        engine=engine
    )
    return (df,)


@app.cell
def _(df):
    df.to_csv("examples/output.csv")
    return


@app.cell
def _(os):
    last_run = os.environ.get("MARIMO_PARAM_LAST_RUN_AT", "")
    with open("examples/output.txt", "w") as f:
        f.write(last_run)
    return


if __name__ == "__main__":
    app.run()

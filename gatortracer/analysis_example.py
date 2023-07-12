"""An example of data analysis."""
from collections import Counter

import plotly.express as px
import plotly.graph_objects as go
import polars as pl


def scatter_fibo_percentage():
    df = pl.read_csv("wow.csv")
    df_with_time = df.filter(pl.col("report_time").is_not_null())

    df_with_time = df_with_time.with_columns(
        pl.col(["report_time"]).str.to_datetime(
            format="%Y-%m-%d %H:%M:%S", strict=False
        )
    )

    wanted_df = df_with_time.filter(
        pl.col(["repo-name"]) == "fibonacci-algorithms-Yanqiao4396"
    )

    trace = go.Scatter(
        x=wanted_df["report_time"], y=wanted_df["percentage_score"], mode="markers"
    )
    layout = go.Layout(
        title="Scatter Plot",
        xaxis=dict(title="datetime"),
        yaxis=dict(title="passing_rate"),
    )
    fig = go.Figure(data=[trace], layout=layout)
    fig.show()


def pie_objective():
    df = pl.read_csv("wow.csv")
    df = df.with_columns(pl.col(["status", "repo-name"]).fill_null("None"))
    objectives = df["status"].to_list()
    counter = Counter(objectives)
    print(counter)
    keys = counter.keys()
    values = counter.values()
    pie = px.pie(names=keys, values=values, title="check_passing_pie")

    pie.show()


if __name__ == "__main__":
    pie_objective()

import streamlit as st
import re
import pandas as pd
from datetime import datetime, date

# Front matter
st.set_page_config(
    page_title="dbt Model Finder",
    page_icon=":shark:",
    layout="centered",
    initial_sidebar_state="auto",
    menu_items=None,
)
st.title("dbt Model Finder :shark:")
st.subheader("(aka: find the model that's still running)")
st.write(
    "I love [dbt](https://www.getdbt.com)! But often when a dbt run hangs, I'm searching through the output line-by-line trying to figure out which models are _still_ running. 😵"
)
st.write(
    "Well, never again. 🎉 Now there's an app for that! Paste your in-progress dbt output in the textbox below. The output should look something like this:"
)
st.code(
    """
10:03:09  1 of 10 START sql table model hyrule.source_orders  [RUN]
10:03:09  2 of 10 START sql table model hyrule.source_users  [RUN]
10:03:09  3 of 10 START sql table model hyrule.source_payments  [RUN]
10:03:09  4 of 10 START sql table model hyrule.source_events  [RUN]
10:03:09  5 of 10 START sql table model hyrule.dim_users  [RUN]
10:03:09  6 of 10 START sql table model hyrule.fct_orders  [RUN]
10:03:19  1 of 10 OK created sql table model hyrule.source_orders  [SELECT in 10.78s]
10:03:19  7 of 10 START sql table model hyrule.fct_payments  [RUN]
10:03:23  2 of 10 OK created sql table model hyrule.source_users  [SELECT in 14.44s]
10:03:23  8 of 10 START sql table model hyrule.mart_weekly_orders  [RUN]
10:03:29  3 of 10 OK created sql table model hyrule.source_payments  [SELECT in 20.14s]
10:03:29  9 of 10 START sql incremental model hyrule.mart_weekly_sessions  [RUN]
10:05:55  7 of 10 OK created sql table model hyrule.fct_payments  [SELECT in 155.22s]
10:05:55  10 of 10 START sql incremental model hyrule.mart_mrr  [RUN]
10:06:33  8 of 10 OK created sql table model hyrule.mart_weekly_orders  [SELECT in 189.95s]
10:06:33  11 of 12 START sql table model hyrule.growth_matrix  [RUN]
10:07:32  9 of 10 OK created sql incremental model hyrule.mart_weekly_sessions  [SELECT in 243.76s]
10:08:00  12 of 12 START sql incremental model hyrule.triforce_purchases  [RUN]
10:08:35  12 of 12 ERROR creating sql incremental model hyrule.triforce_purchases  [ERROR in 17.03s]
"""
)

# Ask for dbt output
st.header("")
raw_input = st.text_area(
    "Paste your in-progress dbt output here (or try the example above)",
    height=300,
    help="The text should've come from your command line interface. Logs downloaded from dbt Cloud are not supported yet.",
)
st.header("")

# Check if dbt output is entered
if not raw_input:
    st.stop()

# Process dbt output
input = raw_input.splitlines()

# Let's see the input
# st.table(input)

# Create lists to store parsed string data
timecodes = []
xofys = []
build_results = []
model_names = []
xs = []
ys = []

# Parse input
for line in input:
    # Get timecode position
    timecode_string = re.search("([0-9]+(:[0-9]+)+)", line).group()
    timecode_string = datetime.strptime(timecode_string, "%H:%M:%S").time()
    timecodes.append(timecode_string)

    # Get 'x of y' position
    xofy_string = re.search("(\d+) of (\d+)", line).group()
    xofys.append(xofy_string)

    # Get result position
    result_string = re.search("(?<= )(START)|(OK)|(ERROR)(?= )", line).group()
    build_results.append(result_string)

    # Get model name position
    modelname_string = re.search("(?<=model ).+\.[A-z]+[^ \.]*(?=.*\[)", line).group()
    model_names.append(modelname_string)

# Parse 'x of y' into x and y
for xofy in xofys:
    x = re.search("^(\d+) ", xofy).group().strip()
    y = re.search(" (\d+)$", xofy).group().strip()

    xs.append(int(x))
    ys.append(int(y))

# Assemble data frame
df = pd.DataFrame(
    data=zip(xs, ys, timecodes, model_names, build_results, input),
    columns=[
        "model_num",
        "total_models",
        "timecode",
        "model_name",
        "build_result",
        "raw_line",
    ],
)

# Sort by model_num and timecode
df = df.sort_values(by=["model_num", "timecode"]).reset_index(drop=True)

# Now split dataframe into START vs. OK (or whatever else)
df_start = df[df["build_result"] == "START"]
df_ok = df[df["build_result"] != "START"]

# Drop and rename columns before joining
df_start = df_start.rename(columns={"timecode": "start_time"}).drop(
    columns=["build_result", "raw_line", "total_models"]
)
df_ok = df_ok.rename(columns={"timecode": "end_time"}).drop(
    columns=["model_name", "build_result"]
)

# And then join based on model_num
df_joined = df_start.merge(df_ok, on="model_num", how="outer")

# Extract models that are still running and count them
df_running = (
    df_joined.query("end_time.isnull()")
    .sort_values(by=["start_time"])
    .reset_index(drop=True)
)

# How many models are still running?
running_models = df_running.shape[0]

if running_models == 0:
    st.subheader("Hmm, I couldn't find any models still running! 🤔")
elif running_models == 1:
    st.subheader(f"Looks like there's 1 model still running. 🤏")
    st.dataframe(df_running[["model_num", "start_time", "model_name"]])
else:
    st.subheader(f"Looks like there's {running_models} models still running. 🚀")
    st.dataframe(df_running[["model_num", "start_time", "model_name"]])

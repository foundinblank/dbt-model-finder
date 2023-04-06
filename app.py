import streamlit as st
import re
import pandas as pd
from datetime import datetime, date


REGEX = r"\s*(?:\[0m)?(\d{2}:\d{2}:\d{2})\s+(\d+)\s+of\s+\d+\s+.*?(?:model\s|of\s)\b(\w+\.\w+)\b"
EXAMPLE_LOGS = """10:03:09  1 of 10 START sql table model hyrule.source_quests  [RUN]
10:03:09  2 of 10 START sql table model hyrule.source_fairies  [RUN]
10:03:09  3 of 10 START sql table model hyrule.source_rupees  [RUN]
10:03:09  4 of 10 START sql table model hyrule.source_rewards  [RUN]
10:03:09  5 of 10 START sql table model hyrule.dim_fairies  [RUN]
10:03:09  6 of 10 START sql table model hyrule.fct_quests  [RUN]
10:03:19  1 of 10 OK created sql table model hyrule.source_quests  [SELECT in 10.78s]
10:03:19  7 of 10 START sql table model hyrule.fct_rupees  [RUN]
10:03:23  2 of 10 OK created sql table model hyrule.source_fairies  [SELECT in 14.44s]
10:03:23  8 of 10 START sql table model hyrule.mart_weekly_quests  [RUN]
10:03:29  3 of 10 OK created sql table model hyrule.source_rupees  [SELECT in 20.14s]
10:03:29  9 of 10 START sql incremental model hyrule.mart_weekly_rewards  [RUN]
10:05:55  7 of 10 OK created sql table model hyrule.fct_rupees  [SELECT in 155.22s]
10:05:55  10 of 10 START sql incremental model hyrule.mart_worlds  [RUN]
10:06:33  8 of 10 OK created sql table model hyrule.mart_weekly_quests  [SELECT in 189.95s]
10:06:33  11 of 12 START sql table model hyrule.heart_matrix  [RUN]
10:07:32  9 of 10 OK created sql incremental model hyrule.mart_weekly_rewards  [SELECT in 243.76s]
10:08:00  12 of 12 START sql incremental model hyrule.triforce_purchases  [RUN]
10:08:35  12 of 12 ERROR creating sql incremental model hyrule.triforce_purchases  [ERROR in 17.03s]"""


def title_and_description():
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
        """I love [dbt](https://www.getdbt.com)!
        But often when a dbt run hangs, I'm searching through the output line-by-line trying to figure out which models are _still_ running. 😵"""
    )
    st.write(
        """Well, never again. 🎉 Now there's an app for that! Paste your in-progress dbt output in the textbox below.
        The text can be copied from your command line interface or the console logs in dbt Cloud.
        It works with dbt run and dbt source freshness!"""
    )

    with st.expander("dbt output example"):
        st.code(EXAMPLE_LOGS)


def get_logs():
    # Ask for dbt output
    st.header("")

    return st.text_area(
        "Paste your in-progress dbt output here (or copy the example above).",
        height=300,
        help="The text should've come from your command line interface, the dbt Cloud UI or the dbt Cloud logs.",
        placeholder=EXAMPLE_LOGS,
    )


def clean_input(raw_input):
    # Check if dbt output is entered
    if not raw_input:
        st.stop()

    # Process dbt output
    input = raw_input.splitlines()

    # Remove empty lines and whitespace
    input = [line for line in input if not line.isspace()]

    # Turn modified input into dataframe
    df = pd.DataFrame(input, columns=["raw_line"])

    # TO DO: Sort out the status & runtime columns
    # TO DO: Add functionality for dbt tests and dbt build
    # Parse out the start time, model number, model name, and status using regex
    # Throw an error if the input doesn't look like dbt output
    try:
        df["post_regex"] = df["raw_line"].apply(
            lambda col: re.search(REGEX, col).groups()
        )
    except AttributeError:
        st.error(
            "The input you provided doesn't look like dbt output. Please check your input and try again."
        )
        st.stop()

    # Convert the post_regex column from a tuple to multiple columns
    df[["start_time", "model_num", "model_name"]] = pd.DataFrame(
        df["post_regex"].to_list(), index=df.index
    )

    # Convert columns to their correct data types
    df["start_time"] = df.apply(
        lambda row: datetime.strptime(row["start_time"], "%H:%M:%S").time(), axis=1
    )
    df["model_num"] = df["model_num"].astype(int)

    # Get list of columns that are still running
    # We are just looking for models that have a single line of output
    still_running = (
        df.groupby("model_num")["raw_line"]
        .count()
        .reset_index()
        .rename(columns={"raw_line": "count_records"})
        .query("count_records == 1")["model_num"]
        .to_list()
    )

    # Filter out the rows that are still running, clean up the columns and return
    return (
        df[df["model_num"].isin(still_running)]
        .sort_values(by=["model_num"])
        .rename(
            columns={
                "model_num": "Model Number",
                "start_time": "Start Time",
                "model_name": "Model Name",
            }
        )
    )


def output(df):
    feedback_str = "What do you think? Drop some feedback in [the repo](https://github.com/foundinblank/dbt-model-finder/) or email me at adamstone@gmail.com."
    running_models = df.shape[0]

    if running_models == 0:
        st.subheader("Hmm, I couldn't find any models still running! 🤔")
    elif running_models == 1:
        st.subheader("Looks like there's 1 model still running. 🤏")
        st.dataframe(df[["Model Number", "Start Time", "Model Name"]])
    else:
        st.subheader(f"Looks like there's {running_models} models still running. 🚀")
        st.dataframe(df[["Model Number", "Start Time", "Model Name"]])
    st.caption(feedback_str)


def main():
    title_and_description()
    raw_input = get_logs()
    df = clean_input(raw_input)
    output(df)


if __name__ == "__main__":
    main()

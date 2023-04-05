import streamlit as st
import re
import pandas as pd
from datetime import datetime, date


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
        "I love [dbt](https://www.getdbt.com)! But often when a dbt run hangs, I'm searching through the output line-by-line trying to figure out which models are _still_ running. 😵"
    )
    st.write(
        "Well, never again. 🎉 Now there's an app for that! Paste your in-progress dbt output in the textbox below. The text can be copied from your command line interface or from the dbt Cloud UI. Log files _downloaded_ from dbt Cloud are not supported yet."
    )

    with st.expander("dbt output example"):
        st.code(
            """
        10:03:09  1 of 10 START sql table model hyrule.source_quests  [RUN]
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
    10:08:35  12 of 12 ERROR creating sql incremental model hyrule.triforce_purchases  [ERROR in 17.03s]
        """
        )

def get_logs():
    # Ask for dbt output
    st.header("")

    return st.text_area(
        "Paste your in-progress dbt output here (or copy the example above).",
        height=300,
        help="The text should've come from your command line interface or the dbt Cloud UI.",
        placeholder="""10:03:09  1 of 10 START sql table model hyrule.source_quests  [RUN]
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
    10:08:35  12 of 12 ERROR creating sql incremental model hyrule.triforce_purchases  [ERROR in 17.03s]
    """,
    )

def clean_input(raw_input):
    # Check if dbt output is entered
    if not raw_input:
        st.stop()

    # Process dbt output
    input = raw_input.splitlines()

    # Modify input by removing extraneous words -- this helps with string parsing later
    extraneous_words = [
        "sql ",
        "created ",
        "creating ",
        "model ",
        "loaded ",
        "file ",
        "in ",
        "VIEW ",
        "\.. ",
    ]

    for word in extraneous_words:
        input = [re.sub(word, "", line) for line in input]

    # For input lines containing passing tests, insert "test" into input
    input = [re.sub("PASS ", "PASS test ", line) for line in input]

    # Turn modified input into dataframe
    df = pd.DataFrame(input, columns=["raw_line"])

    # Split raw_line into columns
    df = df["raw_line"].str.split(expand=True)
    df = df.rename(
        columns={
            0: "timecode",
            1: "model_num",
            2: "of",
            3: "total_models",
            4: "action",
            5: "materialization",
            6: "model_name",
            7: "status",
            8: "runtime_1",
            9: "runtime_2",
        },
        inplace=False,
    )

    # Drop unnecessary columns
    df = df.drop(columns=["of"])

    #  Whenever the action column = FAIL, replace the value in the materialization column with "test"
    df["materialization"] = df.apply(
        lambda row: "test" if row["action"] == "FAIL" else row["materialization"], axis=1
    )

    # Now add the original input back in as a column, convert empty strings to NaN, then drop all-NaN rows
    df["raw_line"] = raw_input.splitlines()
    df["raw_line"] = df.apply(
        lambda row: None if row["raw_line"] == "" else row["raw_line"], axis=1
    )
    df = df.dropna(axis=0, how="all")

    # TO DO: Sort out the status & runtime columns

    # Cast appropriate columns as timestamps & integers
    df["timecode"] = df.apply(
        lambda row: datetime.strptime(row["timecode"], "%H:%M:%S").time(), axis=1
    )
    df["model_num"] = df["model_num"].astype(int)
    df["total_models"] = df["total_models"].astype(int)

    # Sort by model_num and timecode
    df = df.sort_values(by=["model_num", "timecode"]).reset_index(drop=True)

    # Now split dataframe into START vs. OK (or whatever else)
    df_start = df[df["action"] == "START"]
    df_ok = df[df["action"] != "START"]

    # Drop and rename columns before joining
    df_start = df_start.rename(columns={"timecode": "start_time"}).drop(
        columns=["action", "raw_line", "total_models"]
    )
    df_ok = df_ok.rename(columns={"timecode": "end_time"}).drop(
        columns=["model_name", "action"]
    )

    # And then join based on model_num
    df_joined = df_start.merge(df_ok, on="model_num", how="outer")

    # Extract models that are still running and count them
    df_running = (
        df_joined.query("end_time.isnull()")
        .sort_values(by=["start_time"])
        .reset_index(drop=True)
    )
    return df_running

def output(df):
    # How many models are still running?
    running_models = df.shape[0]
    if running_models == 0:
        st.subheader("Hmm, I couldn't find any models still running! 🤔")
        st.caption(
            "What do you think? Drop some feedback in [the repo](https://github.com/foundinblank/dbt-model-finder/) or email me at adamstone@gmail.com."
        )
    elif running_models == 1:
        st.subheader(f"Looks like there's 1 model still running. 🤏")
        st.dataframe(df[["model_num", "start_time", "model_name"]])
        st.caption(
            "What do you think? Drop some feedback in [the repo](https://github.com/foundinblank/dbt-model-finder/) or email me at adamstone@gmail.com."
        )
    else:
        st.subheader(f"Looks like there's {running_models} models still running. 🚀")
        st.dataframe(df[["model_num", "start_time", "model_name"]])
        st.caption(
            "What do you think? Drop some feedback in [the repo](https://github.com/foundinblank/dbt-model-finder/) or email me at adamstone@gmail.com."
        )

def main():
    title_and_description()
    raw_input = get_logs()
    df = clean_input(raw_input)
    output(df)

if __name__ == "__main__":
    main()
#%%

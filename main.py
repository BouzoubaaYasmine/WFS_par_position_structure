import pandas as pd

# Sample data (replace with the actual data)
data = {
    "Column_Name": [
        "NR_DOSSIER",
        "IDENT_CLIENT_ENTI",
        "CODE_PROCESS_DEC_SO",
        "DATE_DEMANDE_SO",
        "IDENT_CLIENT_GROU",
        "IDENT_CLIENT_GROU9",
        "SEGMENTATION_SNI",
        "DT_SEGMENTATION",
        "CODE_CLIENT_SO",
        "VERS_PROC",
        "RAISON_SOCIALE",
        "CODE_VILLE",
        # ... Add all other column names here ...
        "SEG_QQ",
        "STATUT_QQ",
        "FILLER1"
    ],
    "Type": [
        "X",
        "X",
        "X",
        "X",
        "X",
        "U Z",
        "X",
        "X",
        "X",
        "U Z",
        "X",
        "X",
        # ... Add the corresponding types for each column ...
        "X",
        "X",
        "X"
    ],
    "Length": [
        13,
        10,
        10,
        8,
        8,
        8,
        4,
        8,
        5,
        5,
        40,
        4,
        # ... Add the corresponding lengths for each column ...
        1,
        1,
        898
    ]
}


df = pd.DataFrame(data)

def calculate_start_pos(lengths):
    start_pos = [1]
    for length in lengths[:-1]:
        start_pos.append(start_pos[-1] + length)
    return start_pos

# Calculate x and y values based on the Start_Pos and Length columns
df["Start_Pos"] = calculate_start_pos(df["Length"])
df["x"] = df["Start_Pos"]
df["y"] = df["Start_Pos"] + df["Length"] - 1

# Generate the SQL SELECT query dynamically based on the column names, x, and y values
select_columns = ",\n".join(
    f"TRIM(CAST(SUBSTR(LINE, {x}, {y - x + 1}) AS {column_name}))"
    if data_type == "X" else
    f"SUBSTR(LINE, {x}, {y - x + 1}) AS {column_name}"
    for column_name, x, y, data_type in zip(df["Column_Name"], df["x"], df["y"], df["Type"])
)

# Create the script for inserting data into the table
hive_insert_script = f"""
SET tez.queue.name=${{queueName}};
USE awb_${{env}};

INSERT INTO TABLE orc_eden_notation_input
SELECT
    {select_columns}
 CASE
       WHEN REGEXP_EXTRACT(INPUT__FILE__NAME, '.*/(.*)/(.*)_([0-9]{8})_(.*)_([0-9]+)(.txt)', 5) = 1 THEN CURRENT_TIMESTAMP
       ELSE FROM_UNIXTIME(UNIX_TIMESTAMP(REGEXP_EXTRACT(INPUT__FILE__NAME, '.*/(.*)/(.*)_([0-9]{8})_(.*)_([0-9]+)(.txt)', 4), 'yyyyMMdd')) END  AS `TIME`,
   FROM_UNIXTIME(UNIX_TIMESTAMP(REGEXP_EXTRACT(INPUT__FILE__NAME, '.*/(.*)/(.*)_([0-9]{8})_(.*)_([0-9]+)(.txt)', 4), 'yyyyMMdd')) AS DATE_TECHNIQUE
FROM  external_eden_notation_input;
"""

# Print the Hive insert script
print(hive_insert_script)

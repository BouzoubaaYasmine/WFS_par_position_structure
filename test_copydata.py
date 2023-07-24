
def calculate_start_pos(lengths):
    start_pos = [1]
    for length in lengths[:-1]:
        start_pos.append(start_pos[-1] + length)
    return start_pos

with open("output.txt", "r") as file:
    lines = file.readlines()

column_data = [line.strip().split() for line in lines]
print(column_data[0])
for i in range(len(column_data)):
    if len(column_data[i]) == 3:
        column_data[i].insert(2,"")
print(column_data[0])
column_names, data_types, data_types2,lengths = zip(*column_data)

def getLengths(l):
    if "." in l:
        return int(l.split(".")[0]) + int(l.split(".")[1])
    return int(l)

lengths = [getLengths(l) for l in lengths]

start_pos = calculate_start_pos(lengths)
x_values = start_pos
y_values = [start_pos[i] + lengths[i] - 1 for i in range(len(start_pos))]


select_columns = ",\n".join(
    f"TRIM(CAST(SUBSTR(LINE, {x}, {y - x + 1}) AS {column_name}))"
    if data_type == "X" else
    f"SUBSTR(LINE, {x}, {y - x + 1 }) AS {column_name}"

    for column_name, x, y, data_type in zip(column_names, x_values, y_values, data_types)
)

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

print(hive_insert_script)

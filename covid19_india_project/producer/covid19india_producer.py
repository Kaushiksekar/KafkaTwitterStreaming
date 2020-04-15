import csv
import os
from topic_csv_mapper import TOPIC_CSV_MAPPER
from kafka import KafkaProducer
import json

def parse_csv(file_name, topic_name):
    with open(file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row_index, row in enumerate(csv_reader):
            if row_index == 0:
                headings = row
                continue
            temp_row_dict = form_json(row, headings, topic_name)
        print("Row count=", row_index + 1)

def my_decorator(func):
    def inner(row, headings, topic_name):
        row_json = func(row, headings, topic_name)
        produce_json_to_kafka(row_json, topic_name)
        return row_json
    return inner

def produce_json_to_kafka(row_json, topic_name):
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
    producer.send(topic_name, value=json.dumps(row_json).encode('utf-8'))

@my_decorator
def form_json(row, headings, topic_name):
    temp_row_dict = dict()
    for col_index, col_val in enumerate(row):
        temp_row_dict[headings[col_index]] = col_val
    return temp_row_dict



def main():
    for csv_file in TOPIC_CSV_MAPPER:
        print(csv_file, ".....started")
        topic_name = TOPIC_CSV_MAPPER[csv_file]
        csv_file = "../resources/" + csv_file
        csv_data = parse_csv(csv_file, topic_name)
        print(csv_file, ".....done")


if __name__ == '__main__':
    main()

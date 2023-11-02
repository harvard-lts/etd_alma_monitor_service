import argparse
import pymongo
import datetime
import os

def connect_to_mongodb():
    client = pymongo.MongoClient(os.getenv("MONGO_URL"))
    db = client[os.getenv("MONGO_DB")]
    collection = db[os.getenv("MONGO_TEST_COLLECTION")]
    return collection

def add_records(collection):
        records = [
            {
                "proquest_id": 1234567,
                "school_alma_dropbox": "gsd",
                "alma_submission_status": "ALMA_DROPBOX",
                "insertion_date": datetime.datetime.now().isoformat(),
                "last_modified_date": datetime.datetime.now().isoformat(),
                "alma_dropbox_submission_date":
                datetime.datetime.now().isoformat()
            },
            {
                "proquest_id": 2345678,
                "school_alma_dropbox": "dce",
                "alma_submission_status": "ALMA_DROPBOX",
                "insertion_date": datetime.datetime.now().isoformat(),
                "last_modified_date": datetime.datetime.now().isoformat(),
                "alma_dropbox_submission_date":
                datetime.datetime.now().isoformat()
            },
            {
                "proquest_id": 3456789,
                "school_alma_dropbox": "college",
                "alma_submission_status": "ALMA",
                "insertion_date": datetime.datetime.now().isoformat(),
                "last_modified_date": datetime.datetime.now().isoformat(),
                "alma_dropbox_submission_date":
                datetime.datetime.now().isoformat()
            }             
        ]
        collection.insert_many(records)
        print("Added records to MongoDB")

def query_by_alma_status(collection):
        query = {"alma_submission_status": "ALMA_DROPBOX"}
        fields = {"proquest_id": 1, "school_alma_dropbox": 1, "alma_submission_status": 1}
        matching_records = collection.find(query, fields)
        for rec in matching_records:
           print(rec)

def delete_records(collection):
    result = collection.delete_many({})
    print(f"Deleted {result.deleted_count} records from the collection.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="mongo test collection operations")
    parser.add_argument("operation", choices=["add", "query", "delete"], help="options are either add, query, or delete")

    args = parser.parse_args()
    collection = connect_to_mongodb()

    if args.operation == "add":
        add_records(collection)
    elif args.operation == "query":
        query_by_alma_status(collection)
    elif args.operation == "delete":
        delete_records(collection)


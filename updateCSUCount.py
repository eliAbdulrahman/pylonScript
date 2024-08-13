# Imports
import requests
from google.cloud import bigquery
import json
import variables

# Function to patch a CSU 
def addTagToCSU(id): 
    url = "https://api.usepylon.com/issues/" + id

    payload = {
        "custom_fields": [
            {
                "slug": "product_issue_type",
                "values": ["csu"]
            }
        ],
    }
    headers = {"Authorization": "Bearer " + variables.token}

    response = requests.request("PATCH", url, json=payload, headers=headers)
    print(response.text)

# Function to patch a CFR
def addTagToCFR(id): 
    url = "https://api.usepylon.com/issues/" + id

    payload = {
        "custom_fields": [
            {
                "slug": "product_issue_type",
                "values": ["cfr"]
            }
        ],
    }
    headers = {"Authorization": "Bearer" + variables.token}

    response = requests.request("PATCH", url, json=payload, headers=headers)
    print(response.text)



# Loading the big query project 
client = bigquery.Client(project=variables.project)

# Take user input on date to look through 
date = input("Enter date - month(01 - 12):")
if len(date) < 1: 
    var = "SELECT id, custom_fields, external_issue FROM `demos-321800.pylon_bigquery_sync.pylon_issues` WHERE external_issue is not null" 
else:
    num = int(date)
    toAdd = "2024-"+ date + "-01 00:00:00 UTC"
    if 1 <= num and num <= 8: 
        var = "SELECT id, custom_fields, external_issue FROM `demos-321800.pylon_bigquery_sync.pylon_issues` WHERE external_issue is not null and created_at > '" + toAdd + "'"
    else: 
        var = "SELECT id, custom_fields, external_issue FROM `demos-321800.pylon_bigquery_sync.pylon_issues` WHERE external_issue is not null"    



# Query sent to big query 
query = var

# Getting the results and waiting for the query to finish 
results = client.query(query)
rows = results.result()

# Var to count the number of CSU and store the identified tickets
countOfMissingCSU = 0 
CSUcount = []
CFRcount = []
countOfMissingCFR = 0 

# Iterating through each ticket
for row in rows: 
    fields = row.custom_fields
    link = row.external_issue
    for x in link: 
        issue = x["link"]
    identifier = row.id

    # Parse the external issue to check it's connected to a Jira CSU (looks at the link)

    partsOfIssue = issue.split("/")
    endElement = partsOfIssue[len(partsOfIssue) -1]

    # Store the type of issue it gets from the Jira link
    typeIssue = endElement.split("-")

    # Checks for tickets that are a CSU, but do not have tags 
    if typeIssue[0] == "CSU" and (fields is None or "Product Issue Type" not in fields):
        CSUcount.append("Logging issue id " + str(identifier) + " which has Jira csu " + str(issue) + " to get tagged as CSU.")
        countOfMissingCSU += 1

    # Checks for tickets that are a CFR, but do not have tags
    if typeIssue[0] == "CFR" and (fields is None or "Product Issue Type" not in fields):
        CFRcount.append("Logging issue id " + str(identifier) + " which has Jira CFR " + str(issue) + " to get tagged as CFR.")
        countOfMissingCFR += 1


# Statments to display the information
print("\n".join(CSUcount))
print()
print(countOfMissingCSU)
print("---------------------------------------------------------------------")
print("---------------------------------------------------------------------")
print("---------------------------------------------------------------------")
print("---------------------------------------------------------------------")
print("\n".join(CFRcount))
print()
print(countOfMissingCFR)
    

# Showcase the patch to pylon 
change_id = input("If you would like to change an issue enter the id: ")
if len(change_id) > 1: 
    addTagToCSU(change_id)




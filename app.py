import os
import json
import requests
import concurrent.futures

REDUCTO_POD_SCHEMA = {
    "type": "object",
    "properties": {
        "referenceIds": {
            "type": "object",
            "properties": {
                "value": {
                    "type": "array",
                    "description": "List of all reference IDs associated with the shipment.",
                    "items": {
                        "type": "string",
                        "description": "A unique identifier for a document or entity related to the shipping, billing, or consignment process.",
                    },
                },
                "confidenceScore": {
                    "type": "number",
                },
            },
        },
        "shipperName": {
            "type": "object",
            "properties": {
                "value": {
                    "type": "string",
                    "description": "Name of the shipper.",
                },
                "confidenceScore": {
                    "type": "number",
                },
            },
        },
        "shipperAddress": {
            "type": "object",
            "properties": {
                "value": {
                    "type": "string",
                    "description": "Address of the shipper.",
                },
                "confidenceScore": {
                    "type": "number",
                },
            },
        },
        "consigneeName": {
            "type": "object",
            "properties": {
                "value": {
                    "type": "string",
                    "description": "Name of the consignee.",
                },
                "confidenceScore": {
                    "type": "number",
                },
            },
        },
        "consigneeAddress": {
            "type": "object",
            "properties": {
                "value": {
                    "type": "string",
                    "description": "Address of the consignee.",
                },
                "confidenceScore": {
                    "type": "number",
                },
            },
        },
        "carrierName": {
            "type": "object",
            "properties": {
                "value": {
                    "type": "string",
                    "description": "Name of the carrier responsible for shipping the goods.",
                },
                "confidenceScore": {
                    "type": "number",
                },
            },
        },
    },
    "required": ["referenceIds"],
    "description": "Object containing key information about a shipment, including various identification and party details.",
}


def run_extraction(path: str):
    upload_form = requests.post(
        "https://v1.api.reducto.ai/upload",
        headers={"Authorization": f"Bearer {os.environ['REDUCTO_API_KEY']}"},
    ).json()

    requests.put(upload_form["presigned_url"], data=open(path, "rb"))

    response = requests.post(
        "https://v1.api.reducto.ai/extract",
        json={"document_url": upload_form["file_id"], "schema": REDUCTO_POD_SCHEMA},
        headers={"Authorization": f"Bearer {os.environ['REDUCTO_API_KEY']}"},
    )

    output = response.json()

    with open(path.replace(".pdf", ".json"), "w") as f:
        json.dump(output, f)

    return output


def run_extractions_parallel():
    paths = ["./pg1.pdf", "./pg2.pdf", "./pg3.pdf"]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(run_extraction, path) for path in paths]
        outputs = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]
    return outputs


outputs = run_extractions_parallel()

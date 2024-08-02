import { readFileSync, writeFileSync } from "fs";
import fetch from "node-fetch";

const REDUCTO_POD_SCHEMA = {
  type: "object",
  properties: {
    referenceIds: {
      type: "object",
      properties: {
        value: {
          type: "array",
          description:
            "List of all reference IDs associated with the shipment.",
          items: {
            type: "string",
            description:
              "A unique identifier for a document or entity related to the shipping, billing, or consignment process.",
          },
        },
        confidenceScore: {
          type: "number",
        },
      },
    },
    shipperName: {
      type: "object",
      properties: {
        value: {
          type: "string",
          description: "Name of the shipper.",
        },
        confidenceScore: {
          type: "number",
        },
      },
    },
    shipperAddress: {
      type: "object",
      properties: {
        value: {
          type: "string",
          description: "Address of the shipper.",
        },
        confidenceScore: {
          type: "number",
        },
      },
    },
    consigneeName: {
      type: "object",
      properties: {
        value: {
          type: "string",
          description: "Name of the consignee.",
        },
        confidenceScore: {
          type: "number",
        },
      },
    },
    consigneeAddress: {
      type: "object",
      properties: {
        value: {
          type: "string",
          description: "Address of the consignee.",
        },
        confidenceScore: {
          type: "number",
        },
      },
    },
    carrierName: {
      type: "object",
      properties: {
        value: {
          type: "string",
          description:
            "Name of the carrier responsible for shipping the goods.",
        },
        confidenceScore: {
          type: "number",
        },
      },
    },
  },
  required: ["referenceIds"],
  description:
    "Object containing key information about a shipment, including various identification and party details.",
};

interface UploadFormResponse {
  presigned_url: string;
  file_id: string;
}

async function runExtraction(path: string): Promise<any> {
  const uploadFormResponse = await fetch("https://v1.api.reducto.ai/upload", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.REDUCTO_API_KEY}`,
    },
  });
  const uploadFormData: UploadFormResponse =
    (await uploadFormResponse.json()) as any;

  await fetch(uploadFormData.presigned_url, {
    method: "PUT",
    body: readFileSync(path),
  });

  const extractionResponse = await fetch("https://v1.api.reducto.ai/extract", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.REDUCTO_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      document_url: uploadFormData.file_id,
      schema: REDUCTO_POD_SCHEMA,
    }),
  });

  const output: any = await extractionResponse.json();

  writeFileSync(path.replace(".pdf", ".json"), JSON.stringify(output, null, 2));

  return output;
}

async function runExtractionsParallel(): Promise<any[]> {
  const paths = ["./pg1.pdf", "./pg2.pdf", "./pg3.pdf"];
  const extractionPromises = paths.map((path) => runExtraction(path));
  return Promise.all(extractionPromises);
}

async function main() {
  try {
    const outputs = await runExtractionsParallel();
    console.log("Extraction completed:", outputs);
  } catch (error) {
    console.error("Error during extraction:", error);
  }
}

main();

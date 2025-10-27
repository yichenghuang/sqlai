import { type NextRequest, NextResponse } from "next/server";
import { callMCPServerWithRetry } from "@/app/utils/mcpUtils";

interface ScanParams {
  data_src_id: string;
}

interface scanDataSrcResponse {
  job_id: string;
}



// Enhanced MCP server integration with retry logic and better error handling
export async function POST(request: Request) {
  try {
    const body = await request.json()

    console.log("body: ", body);

    if (!body) {
      return NextResponse.json({ error: "Data source ID is required" }, { status: 400 })
    }
    const { dataSrcId: data_src_id } = body;
    console.log("data_src_id: ", data_src_id);

    // Call MCP connect_datasource tool with retry logic
    const result = await callMCPServerWithRetry<ScanParams>(
      "scan_datasource", {data_src_id});

    console.log("result", result);

    let scanResult = null;

    const {job_id} = result?.structuredContent as scanDataSrcResponse;

    if (result?.structuredContent) {
        scanResult = { jobId: job_id }
    }
    console.log(scanResult)

    return NextResponse.json(scanResult)
  } catch (error) {
    console.error("API Error:", error)
    return NextResponse.json({ error: error }, { status: 500 })
  }
}

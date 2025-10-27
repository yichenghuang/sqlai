import { NextResponse } from "next/server";
import { callMCPServerWithRetry } from "@/app/utils/mcpUtils";

// Interface for conn_params as defined by the MCP connect_datasource tool
interface ConnParams {
  host?: string;
  user?: string;
  password?: string;
  database?: string;
}

// Interface for the connect_datasource tool arguments
interface ConnectDatasourceArgs {
  type: string;
  conn_params: ConnParams;
}

// Define a type that represents both your specific arguments and the required generic object shape
type ConnectDatasourceArgsConstraint = ConnectDatasourceArgs & Record<string, unknown>;

interface ConnectDataSrcResponse {
  data_src_id: string;
  scan_time: string;
}

export async function POST(request: Request) {
  try {
    const body = await request.json()
    console.log("body", body);
    const { type, ...conn_params } = body;

    console.log("Connecting to datasource:", { type, conn_params });

    // Call MCP connect_datasource tool with retry logic
    const result = await callMCPServerWithRetry<ConnectDatasourceArgs>(
      "connect_datasource", {
        type,
        conn_params,
      });

    console.log(result);

    const { data_src_id, scan_time } = result?.structuredContent as ConnectDataSrcResponse;

    const connectionResult = {
      success: true,
      message: `Successfully connected to ${type} database at ${conn_params.host}`,
      connectionId: `conn_${Date.now()}`,
      dataSrcId: data_src_id, // MCP server returns resource ID
      lastScanTime: scan_time, // MCP server returns the last scan time
    }

    console.log("[v0] Connection successful, data source ID:", data_src_id)

    return NextResponse.json(connectionResult)
  } catch (error) {
    console.error("[v0] Connection error:", error)
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to connect to data source" },
      { status: 500 },
    )
  }
}

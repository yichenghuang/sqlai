import { NextResponse } from "next/server"

export async function GET() {
  try {
    const mcpServerUrl = process.env.MCP_SERVER_URL
    const mcpApiKey = process.env.MCP_API_KEY

    if (!mcpServerUrl) {
      return NextResponse.json(
        {
          status: "error",
          message: "MCP server not configured",
          mcp_configured: false,
          timestamp: new Date().toISOString(),
        },
        { status: 500 },
      )
    }

    // Test MCP server connectivity
    const healthResponse = await fetch(`${mcpServerUrl}/health`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        ...(mcpApiKey && { Authorization: `Bearer ${mcpApiKey}` }),
      },
      signal: AbortSignal.timeout(5000),
    })

    const mcpHealthy = healthResponse.ok
    const mcpStatus = mcpHealthy ? "connected" : "disconnected"

    return NextResponse.json({
      status: "ok",
      message: "API is running",
      mcp_configured: true,
      mcp_status: mcpStatus,
      mcp_response_time: mcpHealthy ? `${Date.now()}ms` : "timeout",
      timestamp: new Date().toISOString(),
    })
  } catch (error) {
    return NextResponse.json(
      {
        status: "error",
        message: "Health check failed",
        mcp_configured: !!process.env.MCP_SERVER_URL,
        mcp_status: "error",
        error: error instanceof Error ? error.message : "Unknown error",
        timestamp: new Date().toISOString(),
      },
      { status: 500 },
    )
  }
}

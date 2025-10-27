import { type NextRequest, NextResponse } from "next/server";
import { callMCPServerWithRetry } from "@/app/utils/mcpUtils";

interface QueryParams {
  data_src_id: string;
  qry: string;
}

interface QueryResponse {
  sql: string;
  data: Record<string, any>[];
}

interface ResultWithStructuredContent {
  structuredContent: QueryResponse;
  // ... ignore the rest of the complex properties
}


// Enhanced MCP server integration with retry logic and better error handling
export async function POST(request: Request) {
  try {
    const body = await request.json()

    if (!body) {
      return NextResponse.json({ error: "Query is required" }, { status: 400 })
    }
    const { dataSrcId: data_src_id, query: qry } = body;

    // Call MCP connect_datasource tool with retry logic
    const result = await callMCPServerWithRetry<QueryParams>(
      "query", {data_src_id, qry});

    console.log("result", result);

    const typedResult = (result as unknown) as ResultWithStructuredContent;

    if (result && 'structuredContent' in result) {
        return NextResponse.json({
          // message: result.message || generateResponseMessage(result.structuredContent, qry),
          data: typedResult.structuredContent.data,
          sql: typedResult.structuredContent.sql,
          query: qry,
        })
    } else {

    }
  } catch (error) {
    console.error("API Error:", error)
    return NextResponse.json({ error: error }, { status: 500 })
  }
}

// Enhanced MCP server communication with retry logic
// async function callMCPServerWithRetry(query: string, maxRetries = 3) {
//   // const mcpServerUrl = process.env.MCP_SERVER_URL
//   // const mcpApiKey = process.env.MCP_API_KEY

//   // if (!mcpServerUrl) {
//   //   throw new Error("MCP_SERVER_URL not configured")
//   // }

//   for (let attempt = 1; attempt <= maxRetries; attempt++) {
//     try {
//       // Step 1: Submit query to MCP server
//       const client = new Client({
//         name: 'streamable-http-client',
//         version: '1.0.0'
//       });
      
//       const transport = new StreamableHTTPClientTransport(
//         new URL("http://localhost:8000/mcp")
//       );
//       await client.connect(transport);
     
//       const toolsResponse = await client.listTools();
//       const qryTool = toolsResponse.tools?.find((t) => t.name === "query");
//       if (!qryTool) {
//         throw new Error("Tool 'test' not found on MCP server");
//       }
 
//       const result = await client.callTool({
//         name: "query",
//         arguments: { tbl: query }
//       });
//       return result
//     } catch (error) {
//       console.warn(`MCP server attempt ${attempt} failed:`, error)

//       if (attempt === maxRetries) {
//         throw error
//       }

//       // Exponential backoff
//       await new Promise((resolve) => setTimeout(resolve, Math.pow(2, attempt) * 1000))
//     }
//   }
// }

// Poll MCP server for async query results
async function pollForResults(baseUrl: string, queryId: string, apiKey?: string, maxPolls = 10) {
  for (let poll = 1; poll <= maxPolls; poll++) {
    try {
      const resultResponse = await fetch(`${baseUrl}/result/${queryId}`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          ...(apiKey && { Authorization: `Bearer ${apiKey}` }),
        },
        signal: AbortSignal.timeout(5000),
      })

      if (!resultResponse.ok) {
        throw new Error(`MCP result fetch failed: ${resultResponse.status}`)
      }

      const result = await resultResponse.json()

      // Check if results are ready
      if (result.status === "completed" || result.data) {
        return {
          data: result.data || result.results,
          message: result.message,
          source: "mcp_server",
        }
      }

      // Wait before next poll
      if (poll < maxPolls) {
        await new Promise((resolve) => setTimeout(resolve, 1000))
      }
    } catch (error) {
      console.warn(`MCP polling attempt ${poll} failed:`, error)
      if (poll === maxPolls) {
        throw error
      }
    }
  }

  throw new Error("MCP server polling timeout")
}

// Generate appropriate response message based on data type
function generateResponseMessage(data: any, query: string): string {
  if (!data) return "I couldn't find any results for your query."

  if (Array.isArray(data)) {
    const count = data.length
    if (count === 0) return "No results found for your query."
    if (count === 1) return "I found 1 result for your query."
    return `I found ${count} results for your query.`
  }

  if (typeof data === "object") {
    const keys = Object.keys(data)
    if (keys.length === 1) return `Here's the ${keys[0]} information you requested.`
    return "Here's the information you requested."
  }

  return "I've processed your query successfully."
}

// Enhanced mock data generation with more realistic scenarios
function generateEnhancedMockData(query: string) {
  const lowerQuery = query.toLowerCase()

  // Sales and revenue queries
  if (lowerQuery.includes("sales") && lowerQuery.includes("region")) {
    return [
      { region: "North America", total_sales: 2450000, orders: 1250, avg_order: 1960, growth: 12.5 },
      { region: "Europe", total_sales: 1890000, orders: 980, avg_order: 1929, growth: 8.3 },
      { region: "Asia Pacific", total_sales: 3200000, orders: 1680, avg_order: 1905, growth: 18.7 },
      { region: "Latin America", total_sales: 890000, orders: 450, avg_order: 1978, growth: 15.2 },
      { region: "Middle East & Africa", total_sales: 650000, orders: 320, avg_order: 2031, growth: 22.1 },
    ]
  }

  if (lowerQuery.includes("sales") && (lowerQuery.includes("month") || lowerQuery.includes("time"))) {
    return [
      { month: "January", sales: 450000, target: 400000, achievement: 112.5 },
      { month: "February", sales: 520000, target: 450000, achievement: 115.6 },
      { month: "March", sales: 680000, target: 500000, achievement: 136.0 },
      { month: "April", sales: 590000, target: 520000, achievement: 113.5 },
      { month: "May", sales: 720000, target: 600000, achievement: 120.0 },
      { month: "June", sales: 850000, target: 650000, achievement: 130.8 },
    ]
  }

  // Product performance queries
  if (lowerQuery.includes("product") || lowerQuery.includes("top")) {
    return [
      { product: "Premium Widget Pro", revenue: 1250000, units_sold: 3200, margin: 45.2, rating: 4.8 },
      { product: "Standard Widget Plus", revenue: 980000, units_sold: 4100, margin: 38.7, rating: 4.6 },
      { product: "Basic Widget Lite", revenue: 750000, units_sold: 5800, margin: 28.3, rating: 4.3 },
      { product: "Enterprise Widget Suite", revenue: 2100000, units_sold: 1200, margin: 52.1, rating: 4.9 },
      { product: "Mobile Widget App", revenue: 650000, units_sold: 8900, margin: 35.6, rating: 4.4 },
    ]
  }

  // Customer analytics queries
  if (lowerQuery.includes("customer") || lowerQuery.includes("user")) {
    return [
      { segment: "Enterprise", count: 450, avg_value: 12500, retention: 94.2, satisfaction: 4.7 },
      { segment: "SMB", count: 2800, avg_value: 3200, retention: 87.5, satisfaction: 4.4 },
      { segment: "Startup", count: 1200, avg_value: 890, retention: 78.3, satisfaction: 4.2 },
      { segment: "Individual", count: 8900, avg_value: 120, retention: 65.8, satisfaction: 4.0 },
    ]
  }

  // Financial metrics queries
  if (lowerQuery.includes("revenue") || lowerQuery.includes("profit") || lowerQuery.includes("financial")) {
    return [
      { metric: "Total Revenue", current: 12500000, previous: 10800000, change: 15.7, target: 13000000 },
      { metric: "Gross Profit", current: 5200000, previous: 4600000, change: 13.0, target: 5500000 },
      { metric: "Operating Expenses", current: 3800000, previous: 3500000, change: 8.6, target: 3900000 },
      { metric: "Net Income", current: 1400000, previous: 1100000, change: 27.3, target: 1600000 },
    ]
  }

  // Performance metrics queries
  if (lowerQuery.includes("performance") || lowerQuery.includes("kpi") || lowerQuery.includes("metric")) {
    return [
      { kpi: "Customer Acquisition Cost", value: 245, target: 200, status: "Above Target", trend: "Improving" },
      { kpi: "Customer Lifetime Value", value: 3200, target: 2800, status: "Exceeds Target", trend: "Stable" },
      { kpi: "Monthly Recurring Revenue", value: 890000, target: 850000, status: "On Target", trend: "Growing" },
      { kpi: "Churn Rate", value: 2.3, target: 3.0, status: "Below Target", trend: "Improving" },
    ]
  }

  // Default comprehensive summary
  return {
    total_records: 15847,
    last_updated: new Date().toISOString(),
    query_processed: true,
    execution_time: "1.2s",
    data_sources: ["primary_db", "analytics_warehouse", "customer_data"],
    status: "success",
  }
}

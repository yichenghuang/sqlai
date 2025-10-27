import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js"

// Generalized MCP server communication with retry logic
export async function callMCPServerWithRetry<T extends object | undefined>(
  toolName: string, args: T, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      // Step 1: Initialize MCP client and transport
      const client = new Client({
        name: 'streamable-http-client',
        version: '1.0.0'
      });
      
      console.log(process.env.MCP_SERVER_URL)
      const transport = new StreamableHTTPClientTransport(
        new URL(process.env.MCP_SERVER_URL || "http://127.0.0.1:8000/mcp")
      );
      await client.connect(transport);
     
      // Step 2: Find the specified tool
      const toolsResponse = await client.listTools();
      const tool = toolsResponse.tools?.find((t) => t.name === toolName);
      if (!tool) {
        throw new Error(`Tool '${toolName}' not found on MCP server`);
      }
 
      // FIX: Assert 'args' to the required type using the double assertion
      const assertedArgs = args as ({ [key: string]: unknown } | undefined);

      // Step 3: Call the specified tool with provided arguments  
      const result = await client.callTool({
        name: toolName,
        arguments: assertedArgs,
      });
      return result;
    } catch (error) {
      console.warn(`MCP server attempt ${attempt} failed for tool '${toolName}':`, error);

      if (attempt === maxRetries) {
        throw error;
      }

      // Exponential backoff
      await new Promise((resolve) => setTimeout(resolve, Math.pow(2, attempt) * 1000));
    }
  }
}
import { type NextRequest, NextResponse } from "next/server";
import { callMCPServerWithRetry } from "@/app/utils/mcpUtils";


interface ScanProgressParams {
  job_id: string;
}

interface ScanProgressResponse {
  progress: number;
  timestamp: string; 
}

export async function POST(request: Request) {
  try {
    const body = await request.json()

    console.log("body: ", body);

    if (!body) {
      return NextResponse.json({ error: "Job ID is required" }, { status: 400 })
    }

    const { jobId: job_id } = body;
    console.log("Checking scan status for job: ", job_id);

    // Call MCP connect_datasource tool with retry logic
    const result = await callMCPServerWithRetry<ScanProgressParams>(
      "scan_progress", {job_id});
  
    console.log("result", result);
    

    return NextResponse.json(result?.structuredContent)
  

    // Simulate getting status from MCP server
    // let job = scanJobs.get(jobId)

    // if (!job) {
    //   // Initialize new job
    //   job = {
    //     progress: 0,
    //     status: "in_progress",
    //     startTime: Date.now(),
    //   }
    //   scanJobs.set(jobId, job)
    // }

    // Simulate progress (in real implementation, this comes from MCP server)
    // if (job.status === "in_progress") {
    //   const elapsed = Date.now() - job.startTime
    //   // Simulate scan taking about 15-20 seconds
    //   const simulatedProgress = Math.min(100, Math.floor((elapsed / 18000) * 100))

    //   job.progress = simulatedProgress

    //   if (simulatedProgress >= 100) {
    //     job.status = "completed"
    //     job.lastScanTime = new Date().toISOString()
    //   }

    //   scanJobs.set(jobId, job)
    // }

    // return NextResponse.json({
    //   jobId,
    //   progress: job.progress,
    //   status: job.status,
    //   lastScanTime: job.lastScanTime,
    // })
  } catch (error) {
    console.error("Scan status error:", error)
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to get scan status" },
      { status: 500 },
    )
  }
}

"use client"

import type React from "react"

import { useState, useRef, useMemo, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Loader2,
  Send,
  User,
  Bot,
  Menu,
  X,
  Database,
  Search,
  Check,
  ChevronUp,
  ChevronDown,
  ChevronsUpDown,
} from "lucide-react"

import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getPaginationRowModel,
  getFilteredRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table"

interface Message {
  id: string
  type: "user" | "assistant"
  content: string
  data?: any
  sql?: string
  timestamp: Date
}

interface DataVisualizationProps {
  data: any
}

const DataTable = ({ data }: { data: any[] }) => {
  const [sorting, setSorting] = useState<SortingState>([])
  const [globalFilter, setGlobalFilter] = useState("")

  const columns = useMemo<ColumnDef<any>[]>(() => {
    if (!data || data.length === 0) return []

    const firstItem = data[0]
    const keys = Object.keys(firstItem)



    return keys.map((key) => ({
      // Use the raw key as the column ID to avoid misinterpretation
      id: key,
      // Use accessorFn to safely access the key, even if it contains dots
      accessorFn: (row) => row[key],
      // Format the header by replacing camelCase and handling dots
      header: key
      .replace(/([A-Z])/g, " $1") // Add space before capital letters
        .replace(/^./, (str) => str.toUpperCase()) // Capitalize first letter
        .replace(/\./g, " "), // Replace dots with spaces for readability
      cell: ({ getValue }) => {
    //   accessorKey: key,
    //   header: key.replace(/([A-Z])/g, " $1").replace(/^./, (str) => str.toUpperCase()),
    //   cell: ({ getValue }) => {
         const value = getValue()

        if (typeof value === "number") {
          return <span className="font-mono text-blue-600">{value.toLocaleString()}</span>
        } else if (typeof value === "boolean") {
          return (
            <span
              className={`px-2 py-1 rounded-full text-xs font-medium ${
                value ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"
              }`}
            >
              {value ? "Yes" : "No"}
            </span>
          )
        } else if (typeof value === "object" && value !== null) {
          return (
            <details className="cursor-pointer">
              <summary className="text-blue-600 hover:text-blue-800">View Details</summary>
              <pre className="mt-2 text-xs bg-gray-100 p-2 rounded overflow-x-auto">
                {JSON.stringify(value, null, 2)}
              </pre>
            </details>
          )
        }
        return <span className="text-gray-800">{String(value)}</span>
      },
    }))
  }, [data])

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      globalFilter,
    },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    initialState: {
      pagination: {
        pageSize: 10,
      },
    },
  })

  return (
    <Card className="p-4">
      <div className="flex items-center justify-between mb-4">
        <h4 className="text-sm font-medium text-gray-700">Results ({table.getFilteredRowModel().rows.length} rows)</h4>
        <Input
          placeholder="Search all columns..."
          value={globalFilter ?? ""}
          onChange={(e) => setGlobalFilter(e.target.value)}
          className="max-w-xs h-9"
        />
      </div>

      <div className="overflow-x-auto rounded-lg border">
        <table className="w-full border-collapse">
          <thead>
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id} className="border-b-2 border-gray-200 bg-gray-50">
                {headerGroup.headers.map((header) => (
                  <th key={header.id} className="text-left p-3 font-semibold text-gray-700">
                    {header.isPlaceholder ? null : (
                      <div
                        className={`flex items-center gap-2 ${
                          header.column.getCanSort() ? "cursor-pointer select-none hover:text-gray-900" : ""
                        }`}
                        onClick={header.column.getToggleSortingHandler()}
                      >
                        {flexRender(header.column.columnDef.header, header.getContext())}
                        {header.column.getCanSort() && (
                          <span className="text-gray-400">
                            {header.column.getIsSorted() === "asc" ? (
                              <ChevronUp className="w-4 h-4" />
                            ) : header.column.getIsSorted() === "desc" ? (
                              <ChevronDown className="w-4 h-4" />
                            ) : (
                              <ChevronsUpDown className="w-4 h-4" />
                            )}
                          </span>
                        )}
                      </div>
                    )}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row, index) => (
              <tr key={row.id} className={`border-b hover:bg-gray-50 ${index % 2 === 0 ? "bg-white" : "bg-gray-25"}`}>
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className="p-3">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {table.getPageCount() > 1 && (
        <div className="flex items-center justify-between mt-4">
          <div className="text-sm text-gray-600">
            Page {table.getState().pagination.pageIndex + 1} of {table.getPageCount()}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
            >
              Previous
            </Button>
            <Button variant="outline" size="sm" onClick={() => table.nextPage()} disabled={!table.getCanNextPage()}>
              Next
            </Button>
          </div>
        </div>
      )}
    </Card>
  )
}

const DataVisualization = ({ data }: DataVisualizationProps) => {
  if (!data) return null

  const renderJsonTable = (jsonData: any) => {
    if (typeof jsonData === "object" && !Array.isArray(jsonData) && jsonData !== null) {
      const entries = Object.entries(jsonData)

      return (
        <Card className="p-4">
          <h4 className="text-sm font-medium mb-3 text-gray-700">Data Summary</h4>
          <div className="overflow-x-auto">
            <table className="w-full border-collapse">
              <thead>
                <tr className="border-b-2 border-gray-200 bg-gray-50">
                  <th className="text-left p-3 font-semibold text-gray-700">Property</th>
                  <th className="text-left p-3 font-semibold text-gray-700">Value</th>
                </tr>
              </thead>
              <tbody>
                {entries.map(([key, value], index) => (
                  <tr key={key} className={`border-b hover:bg-gray-50 ${index % 2 === 0 ? "bg-white" : "bg-gray-25"}`}>
                    <td className="p-3 font-medium text-gray-600 capitalize">
                      {key.replace(/([A-Z])/g, " $1").replace(/^./, (str) => str.toUpperCase())}
                    </td>
                    <td className="p-3">
                      {typeof value === "number" ? (
                        <span className="font-mono text-blue-600">{value.toLocaleString()}</span>
                      ) : typeof value === "boolean" ? (
                        <span
                          className={`px-2 py-1 rounded-full text-xs font-medium ${
                            value ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"
                          }`}
                        >
                          {value ? "Yes" : "No"}
                        </span>
                      ) : typeof value === "object" && value !== null ? (
                        <details className="cursor-pointer">
                          <summary className="text-blue-600 hover:text-blue-800">View Details</summary>
                          <pre className="mt-2 text-xs bg-gray-100 p-2 rounded overflow-x-auto">
                            {JSON.stringify(value, null, 2)}
                          </pre>
                        </details>
                      ) : (
                        <span className="text-gray-800">{String(value)}</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )
    }

    return (
      <Card className="p-4">
        <h4 className="text-sm font-medium mb-3 text-gray-700">Raw Data</h4>
        <div className="bg-gray-50 rounded-lg p-4 overflow-x-auto">
          <pre className="text-sm text-gray-800 whitespace-pre-wrap font-mono">{JSON.stringify(jsonData, null, 2)}</pre>
        </div>
      </Card>
    )
  }

  if (Array.isArray(data) && data.length > 0) {
    return <DataTable data={data} />
  }

  return renderJsonTable(data)
}

interface DataSourceConnection {
  type: string | null
  host: string
  username: string
  connected: boolean
  lastScanTime?: string
  scanJobId?: string
  scanProgress?: number
  scanStatus?: "in_progress" | "completed" | "failed"
}

interface ProgressStage {
  stage: string
  status: "in_progress" | "completed"
}

export default function Page() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [progressStages, setProgressStages] = useState<ProgressStage[]>([])
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const dataSrcId = useRef<string | null>(null)
  const [dataSource, setDataSource]= useState<DataSourceConnection>({
    type: "",
    host: "",
    username: "",
    connected: false,
  });

  const [connectionForm, setConnectionForm] = useState({
    type: "",
    host: "",
    username: "",
    password: "",
  })
  const [isConnecting, setIsConnecting] = useState(false)
  const [isScanning, setIsScanning] = useState(false)
  const [expandedSql, setExpandedSql] = useState<string | null>(null)
  const scanPollingInterval = useRef<NodeJS.Timeout | null>(null)

  useEffect(() => {
    if (dataSource?.scanJobId && dataSource.scanStatus === "in_progress") {
      scanPollingInterval.current = setInterval(async () => {
        try {
          const response = await fetch("/api/scan-progress", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ jobId: dataSource.scanJobId }),
          })

          const result = await response.json()

          console.log(result)

          if (response.ok) {
            setDataSource((prev) => {
              if (!prev) return prev;
              const updated = { ...prev, scanProgress: result.progress };
              if (result.progress >= 100) {
                updated.scanStatus = "completed";
                updated.lastScanTime = String(result.timestamp); // Convert to string if needed
              }
              return updated;
            });
 
            console.log(dataSource)

            if (result.progress >= 100) {
              if (scanPollingInterval.current) {
                clearInterval(scanPollingInterval.current)
                scanPollingInterval.current = null
              }
              console.log('scan completed')
              setIsScanning(false)
            }
          }
        } catch (error) {
          console.error("Error polling scan status:", error)
        }
      }, 3000)

      return () => {
        if (scanPollingInterval.current) {
          clearInterval(scanPollingInterval.current)
          scanPollingInterval.current = null
        }
      }
    }
  }, [dataSource.scanProgress])


  const handleConnect = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsConnecting(true)

    try {
      const response = await fetch("/api/connect-datasource", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(connectionForm),
      })

      const result = await response.json()

      if (!response.ok) {
        throw new Error(result.error || "Failed to connect")
      }

      dataSrcId.current = result.dataSrcId;

      setDataSource({
        type: connectionForm.type,
        host: connectionForm.host,
        username: connectionForm.username,
        connected: true,
        lastScanTime: result.lastScanTime,
      })
      setConnectionForm({ ...connectionForm, password: "" })
    } catch (error) {
      alert(`Connection failed: ${error instanceof Error ? error.message : "Unknown error"}`)
    } finally {
      setIsConnecting(false)
    }
  }

  const handleDisconnect = () => {
    // setDataSource(null)
    setConnectionForm({
      type: "",
      host: "",
      username: "",
      password: "",
    })
  }

  const handleScan = async () => {
    if (!dataSource?.connected) {
      alert("Please connect to a database first")
      return
    }

    try {
      const response = await fetch("/api/scan-datasource", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataSrcId: dataSrcId.current,
        }),
      })

      const result = await response.json()
      console.log(result)

      if (!response.ok) {
        throw new Error(result.error || "Failed to scan database")
      }

      setDataSource({
        ...dataSource,
        scanJobId: result.jobId,
        scanProgress: 0,
        scanStatus: "in_progress",
      })

      console.log(dataSource)
      setIsScanning(true)
      // alert(`Database scanned successfully! Found ${result.tables?.length || 0} tables.`)
    } catch (error) {
      alert(`Scan failed: ${error instanceof Error ? error.message : "Unknown error"}`)
    }    
    // finally {
    //   setIsScanning(false)
    // }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || isLoading) return

    const userMessage: Message = {
      id: Date.now().toString(),
      type: "user",
      content: input.trim(),
      timestamp: new Date(),
    }

    setMessages((prev) => [...prev, userMessage])
    setInput("")
    setIsLoading(true)
    setProgressStages([])

    console.log("[v0] Starting query:", userMessage.content)

    try {
      const response = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataSrcId: dataSrcId.current,
          query: userMessage.content,
        }),
      })

      // console.log("[v0] Response status:", response.status)
      // console.log("[v0] Response headers:", Object.fromEntries(response.headers.entries()))

      if (!response.ok) {
        throw new Error(`Failed to process query: ${response.status}`)
      }

      const result = await response.json()
      console.log("actual result: ", result.data)

      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: "assistant",
        content: result.response || "Here are your results:",
        data: result.data,
        sql: result.sql,
        timestamp: new Date(),
      }

      // const reader = response.body?.getReader()
      // if (!reader) {
      //   throw new Error("No response body")
      // }

      // const decoder = new TextDecoder()
      // let buffer = ""
      // let resultData: any = null

      // console.log("[v0] Starting to read stream")

      // while (true) {
      //   const { done, value } = await reader.read()

      //   if (done) {
      //     console.log("[v0] Stream reading complete")
      //     break
      //   }

      //   buffer += decoder.decode(value, { stream: true })
      //   console.log("[v0] Buffer:", buffer)

      //   const lines = buffer.split("\n\n")
      //   buffer = lines.pop() || ""

      //   for (const line of lines) {
      //     if (line.startsWith("data: ")) {
      //       try {
      //         const jsonStr = line.slice(6)
      //         console.log("[v0] Parsing JSON:", jsonStr)
      //         const data = JSON.parse(jsonStr)
      //         console.log("[v0] Parsed data:", data)

      //         if (data.type === "progress") {
      //           setProgressStages((prev) => {
      //             const existing = prev.find((s) => s.stage === data.stage)
      //             if (existing) {
      //               return prev.map((s) => (s.stage === data.stage ? { stage: data.stage, status: data.status } : s))
      //             }
      //             return [...prev, { stage: data.stage, status: data.status }]
      //           })
      //         } else if (data.type === "result") {
      //           console.log("[v0] Received result data")
      //           resultData = data
      //         } else if (data.type === "error") {
      //           throw new Error(data.error)
      //         }
      //       } catch (parseError) {
      //         console.error("[v0] Error parsing JSON:", parseError, "Line:", line)
      //       }
      //     }
      //   }
      // }

      // console.log("[v0] Final result data:", resultData)

      // if (resultData) {
      //   const assistantMessage: Message = {
      //     id: (Date.now() + 1).toString(),
      //     type: "assistant",
      //     content: resultData.message || "Here are your results:",
      //     data: resultData.data,
      //     sql: resultData.sql,
      //     timestamp: new Date(),
      //   }

        setMessages((prev) => [...prev, assistantMessage])
        console.log("[v0] Added assistant message")
      // } else {
      //   throw new Error("No result data received from server")
      // }
    } catch (error) {
      console.error("[v0] Error in handleSubmit:", error)
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: "assistant",
        content: `Sorry, I encountered an error: ${error instanceof Error ? error.message : "Unknown error"}. Please try again.`,
        timestamp: new Date(),
      }
      setMessages((prev) => [...prev, errorMessage])
    } finally {
      setIsLoading(false)
      setProgressStages([])
      console.log("[v0] Query complete")
    }
  }

  return (
    <div className="min-h-screen bg-gray-50 flex">
      <div
        className={`fixed left-0 top-0 h-full bg-white border-r shadow-lg transition-transform duration-300 ease-in-out z-50 ${
          sidebarOpen ? "translate-x-0" : "-translate-x-full"
        }`}
        style={{ width: "320px" }}
      >
        <div className="flex flex-col h-full">
          <div className="flex items-center justify-between p-4 border-b">
            <div className="flex items-center gap-2">
              <Database className="w-5 h-5 text-blue-500" />
              <h2 className="font-semibold text-gray-800">Data Source</h2>
            </div>
            <Button variant="ghost" size="sm" onClick={() => setSidebarOpen(false)} className="h-8 w-8 p-0">
              <X className="w-4 h-4" />
            </Button>
          </div>

          <div className="flex-1 overflow-y-auto p-4">
            {!dataSource?.connected && (
              <form onSubmit={handleConnect} className="space-y-4">
                <div>
                  <Label htmlFor="type" className="text-sm font-medium">
                    Database Type
                  </Label>
                  <Input
                    id="type"
                    type="text"
                    placeholder="e.g., PostgreSQL, MySQL"
                    value={connectionForm.type}
                    onChange={(e) => setConnectionForm({ ...connectionForm, type: e.target.value })}
                    className="mt-1"
                    required
                  />
                </div>

                <div>
                  <Label htmlFor="host" className="text-sm font-medium">
                    Host IP
                  </Label>
                  <Input
                    id="host"
                    type="text"
                    placeholder="e.g., 192.168.1.100:5432"
                    value={connectionForm.host}
                    onChange={(e) => setConnectionForm({ ...connectionForm, host: e.target.value })}
                    className="mt-1"
                    required
                  />
                </div>

                <div>
                  <Label htmlFor="username" className="text-sm font-medium">
                    Username
                  </Label>
                  <Input
                    id="username"
                    type="text"
                    placeholder="Database username"
                    value={connectionForm.username}
                    onChange={(e) => setConnectionForm({ ...connectionForm, username: e.target.value })}
                    className="mt-1"
                    // required
                  />
                </div>

                <div>
                  <Label htmlFor="password" className="text-sm font-medium">
                    Password
                  </Label>
                  <Input
                    id="password"
                    type="password"
                    placeholder="Database password"
                    value={connectionForm.password}
                    onChange={(e) => setConnectionForm({ ...connectionForm, password: e.target.value })}
                    className="mt-1"
                    // required
                  />
                </div>

                <Button type="submit" className="w-full" disabled={isConnecting}>
                  {isConnecting ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Connecting...
                    </>
                  ) : (
                    "Connect"
                  )}
                </Button>
              </form>
            )}

            {dataSource && dataSource.connected &&  (
              <div className="space-y-3">
                <Button
                  onClick={handleScan}
                  variant="outline"
                  className="w-full bg-transparent"
                  disabled={isScanning || dataSource.scanStatus === "in_progress"}
                >
                  {isScanning || dataSource.scanStatus === "in_progress" ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Scanning...
                    </>
                  ) : (
                    <>
                      <Search className="w-4 h-4 mr-2" />
                      Scan Database
                    </>
                  )}
                </Button>

                {dataSource?.scanStatus === "in_progress" && (
                  <div className="space-y-2">
                    <div className="flex items-center justify-between text-xs text-gray-600">
                      <span>Scanning database...</span>
                      <span className="font-medium">{dataSource.scanProgress || 0}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2 overflow-hidden">
                      <div
                        className="bg-blue-500 h-full transition-all duration-300 ease-out rounded-full"
                        style={{ width: `${dataSource.scanProgress || 0}%` }}
                      />
                    </div>
                  </div>
                )}

                {dataSource.scanStatus === "completed" && 
                 dataSource.scanProgress && dataSource.scanProgress >= 100 && (
                  <div className="flex items-center gap-2 text-sm text-green-600 bg-green-50 p-2 rounded">
                    <Check className="w-4 h-4" />
                    <span>Scan completed successfully</span>
                  </div>
                )}

                <Button onClick={handleDisconnect} variant="destructive" className="w-full">
                  Disconnect
                </Button>
              </div>
            )}
          </div>

          {dataSource && dataSource.connected && (
            <div className="p-4 border-t bg-gray-50">
              <div className="text-xs text-gray-500 mb-1">Connected to:</div>
              <div className="flex items-center gap-2 mb-2">
                <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                <div>
                  <div className="font-medium text-sm text-gray-800">{dataSource.type}</div>
                  <div className="text-xs text-gray-600">{dataSource.host}</div>
                </div>
              </div>
              {dataSource.lastScanTime && (
                <div className="text-xs text-gray-500 mt-2 pt-2 border-t">
                  <div className="flex items-center gap-1">
                    <Search className="w-3 h-3" />
                    <span>Last scan: {dataSource.lastScanTime}</span>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      <div className="flex-1 flex flex-col">
        <div className="bg-white border-b px-4 py-3">
          <div className="flex items-center gap-3">
            <Button variant="ghost" size="sm" onClick={() => setSidebarOpen(true)} className="h-8 w-8 p-0">
              <Menu className="w-5 h-5" />
            </Button>
            <div>
              <h1 className="text-xl font-semibold text-gray-800">AI Database Assistant</h1>
              <p className="text-sm text-gray-600">Ask questions about your data in natural language</p>
            </div>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto">
          <div className="max-w-4xl mx-auto px-4 py-6">
            {messages.length === 0 && (
              <div className="text-center py-12">
                <Bot className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                <h2 className="text-xl font-medium text-gray-700 mb-2">Welcome to AI Database Assistant</h2>
                <p className="text-gray-500 mb-8">Ask me anything about your data using natural language</p>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 max-w-2xl mx-auto">
                  <Card
                    className="p-4 text-left cursor-pointer hover:shadow-md transition-shadow"
                    onClick={() => setInput("Show me sales by region")}
                  >
                    <h3 className="font-medium mb-2">📊 Sales Analysis</h3>
                    <p className="text-sm text-gray-600">"Show me sales by region"</p>
                  </Card>
                  <Card
                    className="p-4 text-left cursor-pointer hover:shadow-md transition-shadow"
                    onClick={() => setInput("What are the top 10 customers?")}
                  >
                    <h3 className="font-medium mb-2">👥 Customer Insights</h3>
                    <p className="text-sm text-gray-600">"What are the top 10 customers?"</p>
                  </Card>
                  <Card
                    className="p-4 text-left cursor-pointer hover:shadow-md transition-shadow"
                    onClick={() => setInput("Revenue trends last 6 months")}
                  >
                    <h3 className="font-medium mb-2">📈 Revenue Trends</h3>
                    <p className="text-sm text-gray-600">"Revenue trends last 6 months"</p>
                  </Card>
                  <Card
                    className="p-4 text-left cursor-pointer hover:shadow-md transition-shadow"
                    onClick={() => setInput("Show inventory levels by product")}
                  >
                    <h3 className="font-medium mb-2">📦 Inventory Status</h3>
                    <p className="text-sm text-gray-600">"Show inventory levels by product"</p>
                  </Card>
                </div>
              </div>
            )}

            {messages.map((message) => (
              <div key={message.id} className="mb-6">
                <div className={`flex items-start gap-3 ${message.type === "user" ? "justify-end" : "justify-start"}`}>
                  {message.type === "assistant" && (
                    <div className="w-8 h-8 bg-blue-500 rounded-full flex items-center justify-center flex-shrink-0">
                      <Bot className="w-4 h-4 text-white" />
                    </div>
                  )}

                  <div className={`max-w-3xl ${message.type === "user" ? "order-first" : ""}`}>
                    <div
                      className={`rounded-lg px-4 py-3 ${
                        message.type === "user"
                          ? "bg-blue-500 text-white ml-auto max-w-md"
                          : "bg-white border shadow-sm"
                      }`}
                    >
                      <p className="whitespace-pre-wrap">{message.content}</p>
                    </div>

                    {message.data && (
                      <div className="mt-3">
                        <DataVisualization data={message.data} />
                        {message.sql && (
                          <div className="mt-2">
                            <button
                              onClick={() => setExpandedSql(expandedSql === message.id ? null : message.id)}
                              className="inline-flex items-center gap-1 px-2 py-1 text-xs font-mono bg-gray-100 hover:bg-gray-200 text-gray-700 rounded border border-gray-300 transition-colors"
                            >
                              <Database className="w-3 h-3" />
                              <span>SQL</span>
                              <span className="text-gray-500">{expandedSql === message.id ? "▼" : "▶"}</span>
                            </button>
                            {expandedSql === message.id && (
                              <div className="mt-2 bg-gray-900 rounded-lg p-4 overflow-x-auto">
                                <pre className="text-sm text-green-400 font-mono">{message.sql}</pre>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  {message.type === "user" && (
                    <div className="w-8 h-8 bg-gray-500 rounded-full flex items-center justify-center flex-shrink-0">
                      <User className="w-4 h-4 text-white" />
                    </div>
                  )}
                </div>
              </div>
            ))}

            {isLoading && (
              <div className="mb-6">
                <div className="flex items-start gap-3">
                  <div className="w-8 h-8 bg-blue-500 rounded-full flex items-center justify-center flex-shrink-0">
                    <Bot className="w-4 h-4 text-white" />
                  </div>
                  <div className="max-w-3xl">
                    <div className="bg-white border shadow-sm rounded-lg px-4 py-3">
                      <div className="space-y-2">
                        {progressStages.map((stage, index) => (
                          <div key={index} className="flex items-center gap-2 text-sm">
                            {stage.status === "completed" ? (
                              <Check className="w-4 h-4 text-green-500 flex-shrink-0" />
                            ) : (
                              <Loader2 className="w-4 h-4 animate-spin text-blue-500 flex-shrink-0" />
                            )}
                            <span className={stage.status === "completed" ? "text-gray-500" : "text-gray-800"}>
                              {stage.stage}
                            </span>
                          </div>
                        ))}
                      </div>
                      {progressStages.length > 0 && (
                        <div className="mt-3">
                          {/* Placeholder for loading skeleton */}
                          <div className="space-y-3">
                            <div className="animate-pulse">
                              <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
                              <div className="h-4 bg-gray-200 rounded w-1/2 mb-2"></div>
                              <div className="h-32 bg-gray-200 rounded"></div>
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )}

            <div className="mt-8 pt-6 border-t bg-gradient-to-t from-gray-50 to-transparent">
              <div className="bg-white rounded-2xl shadow-lg border border-gray-200 p-4">
                <form onSubmit={handleSubmit} className="flex gap-3 items-end">
                  <div className="flex-1">
                    <Textarea
                      value={input}
                      onChange={(e) => setInput(e.target.value)}
                      placeholder="Ask me anything about your data..."
                      className="border-0 shadow-none resize-none min-h-[44px] max-h-32 focus-visible:ring-0 p-0 text-base"
                      onKeyDown={(e) => {
                        if (e.key === "Enter" && !e.shiftKey) {
                          e.preventDefault()
                          handleSubmit(e)
                        }
                      }}
                      disabled={isLoading}
                    />
                  </div>
                  <Button
                    type="submit"
                    disabled={!input.trim() || isLoading}
                    size="sm"
                    className="rounded-xl h-10 w-10 p-0 flex-shrink-0"
                  >
                    {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
                  </Button>
                </form>
                {input.trim() && (
                  <p className="text-xs text-gray-500 mt-2 text-center">
                    Press Enter to send, Shift+Enter for new line
                  </p>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

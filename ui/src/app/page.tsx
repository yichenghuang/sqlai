"use client"

import type React from "react"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Card } from "@/components/ui/card"
import { Loader2, Send, User, Bot } from "lucide-react"

interface Message {
  id: string
  type: "user" | "assistant"
  content: string
  data?: any
  timestamp: Date
}

interface DataVisualizationProps {
  data: any
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
    const firstItem = data[0]
    const keys = Object.keys(firstItem)

    return (
      <Card className="p-4">
        <h4 className="text-sm font-medium mb-3 text-gray-700">Results ({data.length} rows)</h4>
        <div className="overflow-x-auto">
          <table className="w-full border-collapse">
            <thead>
              <tr className="border-b-2 border-gray-200 bg-gray-50">
                {keys.map((key) => (
                  <th key={key} className="text-left p-3 font-semibold text-gray-700 capitalize">
                    {key.replace(/([A-Z])/g, " $1").replace(/^./, (str) => str.toUpperCase())}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((item: any, index: number) => (
                <tr key={index} className={`border-b hover:bg-gray-50 ${index % 2 === 0 ? "bg-white" : "bg-gray-25"}`}>
                  {keys.map((key) => (
                    <td key={key} className="p-3">
                      {typeof item[key] === "number" ? (
                        <span className="font-mono text-blue-600">{item[key].toLocaleString()}</span>
                      ) : typeof item[key] === "boolean" ? (
                        <span
                          className={`px-2 py-1 rounded-full text-xs font-medium ${
                            item[key] ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"
                          }`}
                        >
                          {item[key] ? "Yes" : "No"}
                        </span>
                      ) : (
                        <span className="text-gray-800">{String(item[key])}</span>
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    )
  }

  return renderJsonTable(data)
}

const LoadingSkeleton = () => (
  <div className="space-y-3">
    <div className="animate-pulse">
      <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
      <div className="h-4 bg-gray-200 rounded w-1/2 mb-2"></div>
      <div className="h-32 bg-gray-200 rounded"></div>
    </div>
  </div>
)

export default function Page() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [loadingPhase, setLoadingPhase] = useState("")

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
    setLoadingPhase("Processing your query...")

    try {
      setTimeout(() => setLoadingPhase("Connecting to database..."), 1000)
      setTimeout(() => setLoadingPhase("Executing query..."), 2000)
      setTimeout(() => setLoadingPhase("Formatting results..."), 3000)

      const response = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: userMessage.content }),
      })

      const result = await response.json()

      if (!response.ok) {
        throw new Error(result.error || "Failed to process query")
      }

      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: "assistant",
        content: result.response || "Here are your results:",
        data: result.data,
        timestamp: new Date(),
      }

      setMessages((prev) => [...prev, assistantMessage])
    } catch (error) {
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: "assistant",
        content: `Sorry, I encountered an error: ${error instanceof Error ? error.message : "Unknown error"}`,
        timestamp: new Date(),
      }
      setMessages((prev) => [...prev, errorMessage])
    } finally {
      setIsLoading(false)
      setLoadingPhase("")
    }
  }

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      <div className="bg-white border-b px-4 py-3">
        <h1 className="text-xl font-semibold text-gray-800">AI Database Assistant</h1>
        <p className="text-sm text-gray-600">Ask questions about your data in natural language</p>
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
                      message.type === "user" ? "bg-blue-500 text-white ml-auto max-w-md" : "bg-white border shadow-sm"
                    }`}
                  >
                    <p className="whitespace-pre-wrap">{message.content}</p>
                  </div>

                  {message.data && (
                    <div className="mt-3">
                      <DataVisualization data={message.data} />
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
                    <div className="flex items-center gap-2">
                      <Loader2 className="w-4 h-4 animate-spin" />
                      <span>{loadingPhase}</span>
                    </div>
                    <div className="mt-3">
                      <LoadingSkeleton />
                    </div>
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
                <p className="text-xs text-gray-500 mt-2 text-center">Press Enter to send, Shift+Enter for new line</p>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

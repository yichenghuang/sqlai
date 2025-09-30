SQLAI engine

Requirements:

    uv python install 3.12
    uv pip install openai
    uv pip install google-generativeai
    uv pip install wcwidth
    uv pip install sentence-transformers
    uv pip install pymilvus
    # sudo apt-get install python3-dev libmysqlclient-dev
    uv pip install mysqlclient
    uv add fastmcp

Installing the sqlai package in development mode to run tests:
    uv pip install --editable .


To Run MCP server:

    Using Streamable HTTP transport:
        uv run fastmcp run mcp_server.py --transport http

        To run insepector:
            npx @modelcontextprotocol/inspector

        then connect inspectr to: http://localhost:8000/mcp


    Using stdio transport: 
        To run mcp server with inspector:
            uv run fastmcp dev mcp_server.py



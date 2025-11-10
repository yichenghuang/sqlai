SQLAI engine

Requirements:

    sudo apt-get install pkg-config
    sudo apt-get install python3-dev default-libmysqlclient-dev

    uv python install 3.12
    uv add fastmcp
    uv add openai
    uv add google-generativeai
    uv add wcwidth
    uv add sentence-transformers
    uv add pymilvus
    uv add mysqlclient
    uv add readerwriterlock


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


Docker Image:

    To optimize the Docker image build process, precompiled wheels are generated
    using the following script:
    
        build_wheels.sh
    
    To build docker image:

        docker build -t bigobject/sqlai-mcp-server:latest .

    To run the Docker container:

        docker run -d --name mcp -p 8000:8000 bigobject/sqlai-mcp-server

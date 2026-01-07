<html>
<head>
    <title>TARN Dashboard</title>
    <style>
        body { font-family: sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .header { background-color: #4CAF50; color: white; padding: 10px; margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class='header'><h1>TARN: Triton on YARN Dashboard</h1></div>

    <h2>Global Resources (Available for AM)</h2>
    <ul>
        <li>Memory: ${availableResources.memorySize} MB</li>
        <li>vCores: ${availableResources.virtualCores}</li>
        <li>Target Containers: ${targetNumContainers}</li>
    </ul>

    <h2>Running Containers</h2>
    <table>
        <tr><th>Container ID</th><th>Host</th><th>Load</th><th>Status</th></tr>
        <#list containers as c>
        <tr>
            <td>${c.id}</td>
            <td>${c.host}</td>
            <td>${c.load?string["0.00%"]}</td>
            <td>RUNNING</td>
        </tr>
        </#list>
    </table>

    <h2>Available Models (HDFS)</h2>
    <ul>
        <#list availableModels as model>
        <li>${model}</li>
        </#list>
    </ul>

    <#if sampleHost??>
    <h2>GPU Details (Sample from ${sampleHost})</h2>
    <#if gpuMetrics?has_content>
    <ul>
        <#list gpuMetrics?keys as key>
        <li>${key}: ${gpuMetrics[key]}</li>
        </#list>
    </ul>
    <#else>
    <p>No GPU metrics available.</p>
    </#if>

    <h2>Loaded Models (Sample from ${sampleHost})</h2>
    <pre>${loadedModelsJson}</pre>
    </#if>
</body>
</html>

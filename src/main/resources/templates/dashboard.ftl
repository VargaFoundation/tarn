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
        <li>Memory: ${availableResources.memorySize?c} MB</li>
        <li>vCores: ${availableResources.virtualCores?c}</li>
        <li>Target Containers: ${targetNumContainers?c}</li>
    </ul>

    <h2>Running Containers</h2>
    <table>
        <tr>
            <th>Container ID</th>
            <th>Host</th>
            <th>Resources</th>
            <th>Load</th>
            <th>GPU Usage</th>
            <th>Status</th>
        </tr>
        <#list containers as c>
        <tr>
            <td>${c.id}</td>
            <td>${c.host}</td>
            <td>${c.memory?c} MB / ${c.vcores?c} vCores</td>
            <td>${c.load?string["0.00%"]}</td>
            <td>
                <#if c.gpus?has_content>
                    <#list c.gpus?keys as gpuId>
                        <strong>GPU ${gpuId}:</strong> 
                        utilization=${c.gpus[gpuId].utilization!0}%, 
                        mem=${c.gpus[gpuId].memory_used!0}/${c.gpus[gpuId].memory_total!0}
                        <br/>
                    </#list>
                <#else>
                    N/A
                </#if>
            </td>
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
    <h2>Loaded Models (Sample from ${sampleHost})</h2>
    <pre>${loadedModelsJson}</pre>
    </#if>
</body>
</html>

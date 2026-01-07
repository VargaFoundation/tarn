<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TARN Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        dark: {
                            bg: '#121212',
                            card: '#1e1e1e',
                            border: '#333333',
                            text: '#e0e0e0',
                            accent: '#4CAF50'
                        }
                    }
                }
            }
        }
    </script>
</head>
<body class="bg-dark-bg text-dark-text font-sans p-6">
    <div class="max-w-7xl mx-auto">
        <header class="bg-dark-accent p-4 rounded-lg shadow-lg mb-8 flex justify-between items-center">
            <h1 class="text-3xl font-bold text-white italic">TARN: Triton on YARN</h1>
            <div class="text-sm bg-black bg-opacity-20 px-3 py-1 rounded">Status: Active</div>
        </header>

        <section class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
            <div class="bg-dark-card p-6 rounded-lg border border-dark-border shadow-md">
                <h2 class="text-xl font-semibold mb-4 text-dark-accent">Global Resources</h2>
                <div class="space-y-2">
                    <p class="flex justify-between"><span>Memory:</span> <span class="font-mono">${availableResources.memorySize?c} MB</span></p>
                    <p class="flex justify-between"><span>vCores:</span> <span class="font-mono">${availableResources.virtualCores?c}</span></p>
                    <p class="flex justify-between border-t border-dark-border pt-2 mt-2"><span>Target Containers:</span> <span class="font-mono">${targetNumContainers?c}</span></p>
                </div>
            </div>
            
            <div class="md:col-span-2 bg-dark-card p-6 rounded-lg border border-dark-border shadow-md">
                <h2 class="text-xl font-semibold mb-4 text-dark-accent">Available Models (HDFS)</h2>
                <div class="flex flex-wrap gap-2">
                    <#list availableModels as model>
                        <span class="bg-dark-border px-3 py-1 rounded-full text-sm">${model}</span>
                    <#else>
                        <span class="text-gray-500 italic">No models found on HDFS</span>
                    </#list>
                </div>
            </div>
        </section>

        <section class="bg-dark-card rounded-lg border border-dark-border shadow-md overflow-hidden mb-8">
            <h2 class="text-xl font-semibold p-6 bg-black bg-opacity-10 border-b border-dark-border">Running Containers</h2>
            <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse">
                    <thead>
                        <tr class="bg-dark-border text-gray-400 uppercase text-xs">
                            <th class="p-4">Container ID</th>
                            <th class="p-4">Host</th>
                            <th class="p-4 text-right">Resources</th>
                            <th class="p-4 text-center">Load</th>
                            <th class="p-4">GPU Usage</th>
                            <th class="p-4 text-center">Status</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-dark-border">
                        <#list containers as c>
                        <tr class="hover:bg-black hover:bg-opacity-20 transition-colors">
                            <td class="p-4 font-mono text-xs text-dark-accent">${c.id}</td>
                            <td class="p-4">${c.host}</td>
                            <td class="p-4 text-right text-sm">
                                <div class="font-mono">${c.memory?c} MB</div>
                                <div class="text-gray-500 text-xs">${c.vcores?c} vCores</div>
                            </td>
                            <td class="p-4">
                                <div class="w-full bg-dark-border rounded-full h-2.5 mb-1">
                                    <div class="bg-dark-accent h-2.5 rounded-full" style="width: ${c.load * 100}%"></div>
                                </div>
                                <div class="text-center text-xs font-mono">${c.load?string["0.00%"]}</div>
                            </td>
                            <td class="p-4 text-xs">
                                <#if c.gpus?has_content>
                                    <#list c.gpus?keys as gpuId>
                                        <div class="mb-1">
                                            <span class="font-bold text-dark-accent">GPU ${gpuId}:</span>
                                            util:${c.gpus[gpuId].utilization!0}%, 
                                            mem:${c.gpus[gpuId].memory_used!0}/${c.gpus[gpuId].memory_total!0}
                                        </div>
                                    </#list>
                                <#else>
                                    <span class="text-gray-600">N/A</span>
                                </#if>
                            </td>
                            <td class="p-4 text-center">
                                <span class="bg-green-900 text-green-300 px-2 py-1 rounded text-[10px] font-bold uppercase tracking-wider">Running</span>
                            </td>
                        </tr>
                        </#list>
                    </tbody>
                </table>
            </div>
        </section>

        <#if sampleHost??>
        <section class="bg-dark-card rounded-lg border border-dark-border shadow-md">
            <h2 class="text-xl font-semibold p-6 border-b border-dark-border">Loaded Models <span class="text-sm font-normal text-gray-500">(Sample from ${sampleHost})</span></h2>
            <div class="p-6">
                <pre class="bg-black p-4 rounded border border-dark-border overflow-x-auto text-xs text-green-500">${loadedModelsJson}</pre>
            </div>
        </section>
        </#if>

        <footer class="mt-12 pt-8 border-t border-dark-border text-center text-gray-600 text-sm">
            Triton on YARN Orchestrator - Open Inference Protocol Compatible
        </footer>
    </div>
</body>
</html>

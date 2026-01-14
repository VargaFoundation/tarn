<#--
 #%L
 Weave
 %%
 Copyright (C) 2025 Varga Foundation
 %%
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 #L%
  -->

<#-- To render the third-party file.
 Available context :
 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)

 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->

<#function artifactFormat p>
    <#if p.name?index_of('Unnamed') &gt; -1>
        <#return p.artifactId + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
    <#else>
        <#return p.name + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
    </#if>
</#function>

<#if licenseMap?size == 0>
    The project has no dependencies.
<#else>

    ===============================================================================

    SAS SUBCOMPONENTS:

    The SAS binary distribution includes a number of subcomponents
    with separate copyright notices and license terms. Your use of the
    code for the these subcomponents is subject to the terms and
    conditions of the following licenses.

    ===============================================================================

    We have listed all of these third party libraries and their licenses
    below. This file can be regenerated at any time by simply running:

    mvn org.codehaus.mojo:license-maven-plugin:aggregate-add-third-party@aggregate-add-third-party

    ---------------------------------------------------
    Third party Java libraries listed by License type.

    PLEASE NOTE: Some dependencies may be listed under multiple licenses if they
    are dual-licensed. This is especially true of anything listed as
    "GNU General Public Library" below.
    ---------------------------------------------------
    <#list licenseMap as e>
        <#assign license = e.getKey()/>
        <#assign projects = e.getValue()/>
        <#if projects?size &gt; 0>

            ${license}:

            <#list projects as project>
                * ${artifactFormat(project)}
            </#list>
        </#if>
    </#list>
</#if>

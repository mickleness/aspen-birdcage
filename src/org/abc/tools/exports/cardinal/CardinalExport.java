package org.abc.tools.exports.cardinal;

import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.abc.tools.Tool;
import org.abc.util.SqlResultHandler;

import com.follett.fsc.core.k12.beans.DataFieldConfig;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.beans.path.BeanTablePath;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.business.dictionary.DataDictionaryField;
import com.follett.fsc.core.k12.business.dictionary.DataDictionaryTable;
import com.follett.fsc.core.k12.tools.ToolInput;
import com.follett.fsc.core.k12.tools.ToolJavaSource;
import com.x2dev.utils.ThreadUtils;
import com.x2dev.utils.X2BaseException;

@Tool(id = "ABC-CARDINAL-EXP", name = "Cardinal Export", type = "export", input = "CardinalExportInput.xml", comment = "This export produces a webpage that helps you construct model property paths and navigate tables/relationships.")
public class CardinalExport extends ToolJavaSource {
	private static final long serialVersionUID = 1L;

	protected static final String PARAM_INCLUDE_TABLE_ROW_COUNT = "includeTableRowCount";
	
	protected static final String keywordReplacementKey = "%DEFINE_INITIALIZE_TABLE_INFO%";
	
    protected static final String htmlTemplate = "<html><head>\n" +
    		"  <title>Cardinal Database Navigator</title>"+
            "  <link href='https://fonts.googleapis.com/css?family=Open Sans' rel='stylesheet'>\n" +
            "  <script src=\"https://unpkg.com/popper.js@1\"></script>\n" +
            "  <script src=\"https://unpkg.com/tippy.js@4\"></script>\n" +
            "\n" +
            "<style>\n" +
            "body {\n" +
            "    font-family: 'Open Sans', Arial, sans-serif;font-size: 16px;\n" +
            "}\n" +
            "\n" +
            "td.inspector-label {\n" +
            "  text-align: right;\n" +
            "  white-space: nowrap;\n" +
            "  padding: 0px;\n" +
            "}\n" +
            "\n" +
            "ol {\n" +
            " list-style: none;\n" +
            " margin-top: 50px;\n" +
            " margin-left: 10px;\n" +
            " margin-right: 10px;\n" +
            " margin-bottom: 10px;\n" +
            " padding: 0;\n" +
            " }\n" +
            "\n" +
            " ol li {\n" +
            "  background: #ffffff;\n" +
            " }\n" +
            "  ol li.selected {\n" +
            "   background: #DF9EEF;\n" +
            "  }\n" +
            "\n" +
            "div.horizontal-scrollbox {\n" +
            "  background-color: #CCC;\n" +
            "  overflow: auto;\n" +
            "  white-space: nowrap;\n" +
            "}\n" +
            "\n" +
            ".header {\n" +
            "  position: fixed;\n" +
            "  width: 100%;\n" +
            "  padding: 10px 16px;\n" +
            "  background: #555;\n" +
            "  color: #f1f1f1;\n" +
            "}\n" +
            "\n" +
            ".vertical-menu {\n" +
            "  display: inline-block;\n" +
            "  overflow-y: scroll;\n" +
            "  height: 380px;\n" +
            "}\n" +
            "\n" +
            ".vertical-menu a {\n" +
            "  color: black;\n" +
            "  display: inline-block;\n" +
            "  padding: 4px;\n" +
            "  text-decoration: none;\n" +
            "  width: 100%;\n" +
            "}\n" +
            "\n" +
            ".vertical-menu a:hover {\n" +
            "  background-color: #E9D3EE;\n" +
            "}\n" +
            "\n" +
            "</style>\n" +
            "\n" +
            "<script>\n" +
            "function removeColumn(columnNumber) {\n" +
            "  while(true) {\n" +
            "    var columnID = \"column-\"+columnNumber;\n" +
            "    var column = document.getElementById(columnID);\n" +
            "    if(column===null)\n" +
            "      return;\n" +
            "    while (column.firstChild) {\n" +
            "        column.removeChild(column.firstChild);\n" +
            "    }\n" +
            "    column.parentElement.removeChild(column);\n" +
            "\n" +
            "    columnNumber++;\n" +
            "  }\n" +
            "}\n" +
            "\n" +
            "function getJavaName(modelPropertyTerm) {\n" +
            "  if(relationshipMap[modelPropertyTerm]==null) {\n" +
            "    return columnMap[modelPropertyTerm][0];\n" +
            "  }\n" +
            "  return relationshipMap[modelPropertyTerm][1];\n" +
            "}\n" +
            "\n" +
            "function prepareColumn(columnNumber, modelPropertyTerm, header) {\n" +
            "  removeColumn(columnNumber);\n" +
            "  var columnID = \"column-\"+columnNumber;\n" +
            "  column = document.createElement('div');\n" +
            "  column.id = columnID;\n" +
            "  column.setAttribute(\"class\", \"vertical-menu\");\n" +
            "  horizScrollbox = document.getElementById('column-container');\n" +
            "  horizScrollbox.appendChild(column);\n" +
            "\n" +
            "\n" +
            "  headerElement = document.createElement('div');\n" +
            "  headerElement.innerHTML = header\n" +
            "  headerElement.setAttribute(\"class\", \"header\");\n" +
            "  column.appendChild(headerElement);\n" +
            "\n" +
            "  modelPropertyInput = document.getElementById(\"model-property-text\");\n" +
            "  javaInput = document.getElementById(\"java-path-text\");\n" +
            "  if(modelPropertyTerm===null || columnNumber===1) {\n" +
            "    modelPropertyInput.value = \"\";\n" +
            "    javaInput.value = \"\";\n" +
            "  } else if(columnNumber===2) {\n" +
            "    modelPropertyInput.value = modelPropertyTerm;\n" +
            "    javaInput.value = getJavaName(modelPropertyTerm);\n" +
            "  } else {\n" +
            "    modelPropertyStr = modelPropertyInput.value;\n" +
            "    oldTerms = modelPropertyStr.split(\".\");\n" +
            "    newTerms = new Array();\n" +
            "    for (var i = 0; i < columnNumber; i++) {\n" +
            "      newTerms[i] = oldTerms[i];\n" +
            "    }\n" +
            "    newTerms[columnNumber-2] = modelPropertyTerm;\n" +
            "    modelPropertyInput.value = newTerms[0];\n" +
            "    for(var i = 1; i<newTerms.length - 1; i++) {\n" +
            "      modelPropertyInput.value = modelPropertyInput.value + \".\"+newTerms[i];\n" +
            "    }\n" +
            "\n" +
            "    var javaName = \"\";\n" +
            "    var modelTerms = modelPropertyInput.value.split(\".\");\n" +
            "    for(var i = 0; i<modelTerms.length; i++) {\n" +
            "      if(i===0) {\n" +
            "        javaName = getJavaName(modelTerms[i]);\n" +
            "      } else {\n" +
            "        javaName += \".\"+getJavaName(modelTerms[i]);\n" +
            "      }\n" +
            "    }\n" +
            "    javaInput.value = javaName;\n" +
            "  }\n" +
            "  return column;\n" +
            "}\n" +
            "\n" +
            "function selectColumn(columnNumber, columnID, parentListItemID, scrollToSelection) {\n" +
            "  console.log(\"selectColumn( \"+columnNumber+\", \"+columnID+\", \"+parentListItemID+\", \"+scrollToSelection+\")\");\n" +
            "  var header = columnID;\n" +
            "\n" +
            "  sortBy = document.getElementById('data-sort').value;\n" +
            "  if(\"java-name\"===sortBy) {\n" +
            "    header = columnMap[columnID][0];\n" +
            "  }\n" +
            "  if(\"udf-name\"===sortBy) {\n" +
            "    header = columnMap[columnID][5];\n" +
            "  }\n" +
            "\n" +
            "  column = prepareColumn(columnNumber, columnID, header);\n" +
            "  blurb = document.createElement('div');\n" +
            "\n" +
            "  var tableHTML = \"<table style=\\\"margin-top: 80px;\\\"><tr><td class='inspector-label'>Database Name:</td><td> &nbsp; \"+columnMap[columnID][1]+\"</td></tr>\"+\n" +
            "                          \"<tr><td class='inspector-label'>Database Type:</td><td> &nbsp; \"+columnMap[columnID][3]+\"</td></tr>\"+\n" +
            "                          \"<tr><td class='inspector-label'>Database Length:</td><td> &nbsp; \"+columnMap[columnID][4]+\"</td></tr>\"+\n" +
            "                          \"<tr><td class='inspector-label'>Java Name:</td><td> &nbsp; \"+columnMap[columnID][0]+\"</td></tr>\"+\n" +
            "                          \"<tr><td class='inspector-label'>Java Type:</td><td> &nbsp; \"+columnMap[columnID][2]+\"</td></tr>\"+\n" +
            "  						   \"<tr><td class='inspector-label'>Short Name:</td><td> &nbsp; \"+columnMap[columnID][5]+\"</td></tr>\"+\n" +
            "  						   \"<tr><td class='inspector-label'>Long Name:</td><td> &nbsp; \"+columnMap[columnID][6]+\"</td></tr>\";\n" +
            "  if(columnMap[columnID][7].indexOf(\"enabled\") >= 0) {\n" +
            "    tableHTML += \"<tr><td class='inspector-label'>Enabled:</td><td> &nbsp; true</td></tr>\";\n" +
            "  } else {\n" +
            "      tableHTML += \"<tr><td class='inspector-label'>Enabled:</td><td> &nbsp; false</td></tr>\";\n" +
            "  }\n" +
            "\n" +
            "  if(columnMap[columnID][7].indexOf(\"required\") >= 0) {\n" +
            "    tableHTML += \"<tr><td class='inspector-label'>Required:</td><td> &nbsp; true</td></tr>\";\n" +
            "  } else {\n" +
            "      tableHTML += \"<tr><td class='inspector-label'>Required:</td><td> &nbsp; false</td></tr>\";\n" +
            "  }\n" +
            "\n" +
            "  if(columnMap[columnID][7].indexOf(\"read-only\") >= 0) {\n" +
            "    tableHTML += \"<tr><td class='inspector-label'>Read Only:</td><td> &nbsp; true</td></tr>\";\n" +
            "  } else {\n" +
            "      tableHTML += \"<tr><td class='inspector-label'>Read Only:</td><td> &nbsp; false</td></tr>\";\n" +
            "  }\n" +
            "\n" +
            "  if(columnMap[columnID][7].indexOf(\"system-use-only\") >= 0) {\n" +
            "    tableHTML += \"<tr><td class='inspector-label'>System Use Only:</td><td> &nbsp; true</td></tr>\";\n" +
            "  } else {\n" +
            "      tableHTML += \"<tr><td class='inspector-label'>System Use Only:</td><td> &nbsp; false</td></tr>\";\n" +
            "  }\n" +
            "  tableHTML += \"</table>\"\n" +
            "\n" +
            "acrLastModTime: ['lastModifiedTime', 'ACR_LAST_MODIFIED',  'long',  'BIGINT',  '14',  'system-use-only'],\n" +
            "\n" +
            "  blurb.innerHTML= tableHTML;\n" +
            "  column.appendChild(blurb);\n" +
            "  selectListItem(parentListItemID, scrollToSelection);\n" +
            "}\n" +
            "\n" +
            "function refreshFromModelProperty() {\n" +
            "  var modelPropertyStr = document.getElementById(\"model-property-text\").value;\n" +
            "  console.log(\"refreshFromModelProperty for \\\"\"+modelPropertyStr+\"\\\"\");\n" +
            "  initializeUI();\n" +
            "  if(modelPropertyStr.length > 0) {\n" +
            "    var modelPropertyTerms = modelPropertyStr.split(\".\");\n" +
            "    if(modelPropertyTerms[0].length>6) {\n" +
            "      if(relationshipMap[modelPropertyTerms[0]]==null) {\n" +
            "        tablePrefix = modelPropertyTerms[0].substring(0,3).toUpperCase();\n" +
            "      } else {\n" +
            "        tablePrefix = modelPropertyTerms[0].substring(3,6).toUpperCase();\n" +
            "      }\n" +
            "      selectTable(1, null, tablePrefix, \"table-list-\"+tablePrefix, true);\n" +
            "      for(var i = 0; i<modelPropertyTerms.length; i++) {\n" +
            "        if(relationshipMap[modelPropertyTerms[i]]==null) {\n" +
            "          selectColumn(i+2, modelPropertyTerms[i], \"column-\"+(i+1)+\"-list-\"+modelPropertyTerms[i], true);\n" +
            "        } else {\n" +
            "          tablePrefix = modelPropertyTerms[i].substring(6,9).toUpperCase();\n" +
            "          selectTable(i+2, modelPropertyTerms[i], tablePrefix, \"column-\"+(i+1)+\"-list-\"+modelPropertyTerms[i], true);\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}\n" +
            "\n" +
            "function getTableIDByPrefix(tablePrefix) {\n" +
            "    for (var table in tableMap) {\n" +
            "      if(tableMap[table][1].toUpperCase()===tablePrefix.toUpperCase())\n" +
            "        return table;\n" +
            "    }\n" +
            "    return null;\n" +
            "}\n" +
            "\n" +
            "function selectTable(columnNumber, relationshipID, tablePrefix, parentListItemID, scrollToSelection) {\n" +
            "  console.log(\"selectTable( \"+columnNumber+\", \"+relationshipID+\", \"+tablePrefix+\", \"+parentListItemID+\", \"+scrollToSelection+\")\");\n" +
            "  sortBy = document.getElementById('data-sort').value;\n" +
            "  tableID = getTableIDByPrefix(tablePrefix);\n" +
            "  if(\"model-property\"===sortBy) {\n" +
            "    headerText = tableMap[tableID][1];\n" +
            "  } if(\"java-name\"===sortBy || \"udf-name\"===sortBy) {\n" +
            "    headerText = tableMap[tableID][2];\n" +
            "    headerText = headerText.substring(headerText.lastIndexOf(\".\")+1);\n" +
            "  }\n" +
            "  column = prepareColumn(columnNumber, relationshipID, headerText);\n" +
            "\n" +
            "  column.tablePrefix = tablePrefix;\n" +
            "\n" +
            "  var listID = column.id + \"-list\";\n" +
            "  list = document.createElement('ol');\n" +
            "  list.id = listID;\n" +
            "  column.appendChild(list);\n" +
            "\n" +
            "  for (var relationship in relationshipMap) {\n" +
            "    var relationshipPrefix = relationship.substring(3,6);\n" +
            "    if(relationshipPrefix.toUpperCase() === tablePrefix.toUpperCase()) {\n" +
            "        var li = document.createElement('li');\n" +
            "        li.id = listID+\"-\"+relationship;\n" +
            "        list.appendChild(li);\n" +
            "\n" +
            "        if(\"model-property\"===sortBy) {\n" +
            "          text = relationship;\n" +
            "        } if(\"java-name\"===sortBy || \"udf-name\"===sortBy) {\n" +
            "          text = relationshipMap[relationship][1];\n" +
            "        }\n" +
            "\n" +
            "        var action = \"javascript:selectTable(\"+(columnNumber+1)+\", '\"+relationship+\"', '\"+relationship.substring(6,9)+\"', '\"+li.id+\"', false)\";\n" +
            "\n" +
            "        if(relationshipMap[relationship][5]===\"CHILD\") {\n" +
            "          text = text + \" &#xFFE9;\";\n" +
            "        } else if(relationshipMap[relationship][5]===\"PARENT\") {\n" +
            "          text = text + \" &#xFFEB;\";\n" +
            "        } else if(relationshipMap[relationship][5]===\"PARTNER\") {\n" +
            "          text = text + \" &#x21C4;\";\n" +
            "        }\n" +
            "        var relTableID = getTableIDByPrefix(relationship.substring(6,9));\n" +
            "        var tooltip = createTableTooltip(relationshipMap[relationship][0], relTableID);\n" +
            "        var link = createLink(text, tooltip, action);\n" +
            "\n" +
            "        li.setAttribute(\"sort-by\", text);\n" +
            "        if(tableMap[relTableID][3]==='0') {\n" +
            "          if(document.getElementById(\"show-unused-tables\").value===\"show\") {\n" +
            "            link.style = \"text-decoration: line-through;\";\n" +
            "            li.appendChild(link);\n" +
            "          }\n" +
            "        } else {\n" +
            "          li.appendChild(link);\n" +
            "        }\n" +
            "\n" +
            "    }\n" +
            "  }\n" +
            "\n" +
            "  for (var column in columnMap) {\n" +
            "    var columnTablePrefix = column.substring(0, 3);\n" +
            "    if(columnTablePrefix.toUpperCase() === tablePrefix.toUpperCase()) {\n" +
            "        var li = document.createElement('li');\n" +
            "        li.id = listID+\"-\"+column;\n" +
            "        list.appendChild(li);\n" +
            "\n" +
            "        if(\"model-property\"===sortBy) {\n" +
            "          text = column;\n" +
            "        } else if(\"java-name\"===sortBy) {\n" +
            "          text = columnMap[column][0];\n" +
            "        } else if(\"udf-name\"===sortBy) {\n" +
            "			if(columnMap[column][5]) {\n"  +
            "          text = columnMap[column][5];\n" +
            "		   } else {\n"  +
            "			text = columnMap[column][0];\n"  +
            "			}\n"  +
            "        }\n" +
            "\n" +
            "        var action = \"javascript:selectColumn(\"+(columnNumber+1)+\", '\"+column+\"', '\"+li.id+\"', false)\";\n" +
            "        var isEnabled = columnMap[column][7].indexOf('enabled') >= 0;\n" +
            "\n" +
            "        var tooltip = columnMap[column][3]+\"<br>\"+columnMap[column][2]+\"<br>\";\n" +
            "        if(isEnabled) {\n" +
            "          tooltip += \"Enabled\";\n" +
            "        } else {\n" +
            "          tooltip += \"Disabled\";\n" +
            "        }\n" +
            "\n" +
            "        var link = createLink(text, tooltip, action);\n" +
            "        li.setAttribute(\"sort-by\", text);\n" +
            "\n" +
            "        if(!isEnabled) {\n" +
            "            if(document.getElementById(\"show-disabled-fields\").value===\"show\") {\n" +
            "              link.style = \"text-decoration: line-through;\";\n" +
            "              li.appendChild(link);\n" +
            "            }\n" +
            "        } else {\n" +
            "          li.appendChild(link);\n" +
            "        }\n" +
            "    }\n" +
            "  }\n" +
            "\n" +
            "  sortList(listID);\n" +
            "  selectListItem(parentListItemID, scrollToSelection);\n" +
            "  tippy('a');\n" +
            "}\n" +
            "\n" +
            "function selectListItem(itemID, scrollToSelection) {\n" +
            "  parentList = document.getElementById(itemID).parentNode;\n" +
            "  var selectedListItem = null;\n" +
            "  var selectedListIndex = -1;\n" +
            "  for (var i = 0; i < parentList.childNodes.length; i++) {\n" +
            "    listItem = parentList.childNodes[i];\n" +
            "    if(listItem != null) {\n" +
            "      if(listItem.id===itemID) {\n" +
            "        listItem.setAttribute(\"class\", \"selected\");\n" +
            "        selectedListItem = listItem;\n" +
            "        selectedListIndex = i;\n" +
            "      } else {\n" +
            "        listItem.setAttribute(\"class\", \"\");\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "\n" +
            "  if(scrollToSelection) {\n" +
            "    selectedListItem.scrollIntoView();\n" +
            "    if(selectedListIndex<parentList.childNodes.length - 5)\n" +
            "      parentList.parentNode.scrollBy(0,-100);\n" +
            "  }\n" +
            "}\n" +
            "\n" +
            "function createLink(text, tooltip, javascriptAction) {\n" +
            "  var link = document.createElement('a');\n" +
            "  link.innerHTML = text;\n" +
            "  link.setAttribute(\"href\", javascriptAction);\n" +
            "  link.setAttribute(\"data-tippy-content\", tooltip);\n" +
            "  link.setAttribute(\"data-tippy-placement\", 'right');\n" +
            "  link.setAttribute(\"data-tippy-offset\", '90');\n" +
            "  return link;\n" +
            "}\n" +
            "\n" +
            "function sortList(id) {\n" +
            "  list = document.getElementById(id);\n" +
            "  var sort_by_name = function(a, b) {\n" +
            "    return a.getAttribute(\"sort-by\").toLowerCase().localeCompare(b.getAttribute(\"sort-by\").toLowerCase());\n" +
            "  }\n" +
            "\n" +
            "  var z = list.childNodes;\n" +
            "  var x = new Array();\n" +
            "  for (var i = 0; i < list.childNodes.length; i++) {\n" +
            "    x[i] = list.childNodes[i];\n" +
            "  }\n" +
            "  x.sort(sort_by_name);\n" +
            "  for (var i = 0; i < x.length; i++) {\n" +
            "      x[i].parentNode.appendChild(x[i]);\n" +
            "  }\n" +
            "}\n" +
            "\n" +
            "function formatNumber(number) {\n" +
            "  var nf = Intl.NumberFormat();\n" +
            "  return nf.format(number);\n" +
            "}\n" +
            "\n" +
            "function createTableTooltip(firstRow, tableKey) {\n" +
            "  var tt = \"\";\n" +
            "  if(firstRow != null) {\n" +
            "    tt += firstRow+\"<br>\";\n" +
            "    var sortBy = document.getElementById('table-sort').value;\n" +
            "\n" +
            "    if(sortBy === \"id\") {\n" +
            "      tt += tableKey+\"<br>\";\n" +
            "    } else if(sortBy === \"prefix\") {\n" +
            "      tt += tableMap[tableKey][1]+\"<br>\";\n" +
            "    } else if(sortBy === \"database-name\") {\n" +
            "      tt += tableMap[tableKey][0]+\"<br>\";\n" +
            "    } else if(sortBy === \"java-class\") {\n" +
            "      tt += tableMap[tableKey][2]+\"<br>\";\n" +
            "    }\n" +
            "  }\n" +
            "  if(tableMap[tableKey][3]==='-1') {\n" +
            "  } else {\n" +
            "    tt += formatNumber(tableMap[tableKey][3])+\" record(s)<br>\";\n" +
            "  }\n" +
            "  if(tableMap[tableKey][4]==='true') {\n" +
            "    tt += \"Audited\";\n" +
            "  } else {\n" +
            "    tt += \"Not Audited\";\n" +
            "  }\n" +
            "  return tt;\n" +
            "}\n" +
            "\n" +
            "initializeUI = function initializeUI() {\n" +
            "  tableRoot = document.getElementById('table-view');\n" +
            "\n" +
            "  list = document.getElementById('table-list');\n" +
            "  if(list!=null) {\n" +
            "    while (list.firstChild) {\n" +
            "      list.removeChild(list.firstChild);\n" +
            "    }\n" +
            "    list.parentElement.removeChild(list);\n" +
            "  }\n" +
            "  removeColumn(1);\n" +
            "\n" +
            "  sortBy = document.getElementById('table-sort').value;\n" +
            "  list = document.createElement('ol');\n" +
            "  list.id = \"table-list\";\n" +
            "  tableRoot.appendChild(list);\n" +
            "  for (var tableKey in tableMap) {\n" +
            "    var li = document.createElement('li');\n" +
            "    li.id = list.id+\"-\"+tableMap[tableKey][1];\n" +
            "    list.appendChild(li);\n" +
            "\n" +
            "    if(\"id\"===sortBy) {\n" +
            "      text = tableKey;\n" +
            "    } if(\"database-name\"===sortBy) {\n" +
            "      text = tableMap[tableKey][0];\n" +
            "    } if(\"prefix\"===sortBy) {\n" +
            "      text = tableMap[tableKey][1];\n" +
            "    } if(\"java-class\"===sortBy) {\n" +
            "      text = tableMap[tableKey][2];\n" +
            "    }\n" +
            "\n" +
            "    var tooltip = createTableTooltip(null, tableKey);\n" +
            "    var action = \"javascript:selectTable(1, null, '\"+tableMap[tableKey][1]+\"', '\"+li.id+\"', false)\";\n" +
            "    var link = createLink(text, tooltip, action);\n" +
            "    li.setAttribute(\"sort-by\", text);\n" +
            "    if(tableMap[tableKey][3]==='0') {\n" +
            "      if(document.getElementById(\"show-unused-tables\").value===\"show\") {\n" +
            "        link.style = \"text-decoration: line-through;\";\n" +
            "        li.appendChild(link);\n" +
            "      }\n" +
            "    } else {\n" +
            "      li.appendChild(link);\n" +
            "    }\n" +
            "  }\n" +
            "  sortList('table-list');\n" +
            "  tippy('a');\n" +
            "  tippy('button');\n" +
            "}\n" +
            "\n" +
            "function copyText(id) {\n" +
            "var copyText = document.getElementById(id);\n" +
            "copyText.select();\n" +
            "document.execCommand(\"copy\");\n" +
            "}\n" +
            "\n" +
            "%DEFINE_INITIALIZE_TABLE_INFO%\n" +
            "\n" +
            "    initializeTableInfo();\n" +
            "    </script>\n" +
            "</head>\n" +
            "\n" +
            "  <div class=\"horizontal-scrollbox\" id=\"column-container\">\n" +
            "     <div class=\"vertical-menu\" id=\"table-view\">\n" +
            "       <div class=\"header\">Tables</div>\n" +
            "     </div>\n" +
            "  </div>\n" +
            "\n" +
            "  <table style=\"width:100%;\">\n" +
            "    <tr><td class=\"inspector-label\">Model Property:</td><td style=\"width:99%;padding: 5px;\"><input style=\"width:100%; font-family: 'Open Sans', Arial, sans-serif;font-size: 18px;\" spellcheck=\"false\" id=\"model-property-text\" type=\"text\" value=\"\" onchange=\"refreshFromModelProperty();\"></input>\n" +
            "    </td><td>\n" +
            "      <button onclick=\"copyText('model-property-text')\" data-tippy-content=\"Copy Model Property\">\n" +
            "        Copy\n" +
            "      </button>\n" +
            "    </td></tr>\n" +
            "    <tr><td  class=\"inspector-label\">Java Bean Path:</td><td style=\"width:99%;padding: 5px;\"><input style=\"width:100%; font-family: 'Open Sans', Arial, sans-serif;font-size: 18px;\" spellcheck=\"false\" id=\"java-path-text\" type=\"text\" value=\"\" readonly></input>\n" +
            "    </td><td>\n" +
            "      <button onclick=\"copyText('java-path-text')\" data-tippy-content=\"Copy Java Bean Path\">\n" +
            "        Copy\n" +
            "      </button>\n" +
            "    </td></tr>\n" +
            "  </table>\n" +
            "  <table>\n" +
            "    <tr><td class=\"inspector-label\">Display:</td>\n" +
            "      <td style=\"padding: 5px;\">\n" +
            "     <select style=\"font-family: 'Open Sans', Arial, sans-serif;font-size: 18px;\" id=\"data-sort\" onchange=\"refreshFromModelProperty();\">\n" +
            "      <option value=\"udf-name\">UDF Name or Java Name</option>\n" +
            "      <option value=\"model-property\">Model Property</option>\n" +
            "      <option value=\"java-name\">Java Name</option>\n" +
            "     </select></td>\n" +
            "     <td class=\"inspector-label\" style=\"padding: 5px 5px 5px 100px;\">Unused Tables:</td>\n" +
            "     <td><select style=\"font-family: 'Open Sans', Arial, sans-serif;font-size: 18px;\" id=\"show-unused-tables\" onchange=\"refreshFromModelProperty();\">\n" +
            "      <option value=\"hide\">Hide</option>\n" +
            "      <option value=\"show\">Show</option>\n" +
            "     </select></td>\n" +
            "  </tr>\n" +
            "    <tr><td  class=\"inspector-label\">Sort Tables By:</td>\n" +
            "      <td style=\"padding: 5px;\">\n" +
            "      <select style=\"font-family: 'Open Sans', Arial, sans-serif;font-size: 18px;\" id=\"table-sort\" onchange=\"refreshFromModelProperty();\">\n" +
            "       <option value=\"id\">ID</option>\n" +
            "       <option value=\"prefix\">Prefix</option>\n" +
            "       <option value=\"database-name\">Database Name</option>\n" +
            "       <option value=\"java-class\">Java Class Name</option>\n" +
            "    </select></td>\n" +
            "    <td class=\"inspector-label\" style=\"padding: 5px 5px 5px 100px;\">Disabled Fields:</td>\n" +
            "    <td><select style=\"font-family: 'Open Sans', Arial, sans-serif;font-size: 18px;\" id=\"show-disabled-fields\" onchange=\"refreshFromModelProperty();\">\n" +
            "     <option value=\"hide\">Hide</option>\n" +
            "     <option value=\"show\">Show</option>\n" +
            "    </select></td></tr>\n" +
            "  </table>\n" +
            "  <p>\n"+
            "  <div style=\"text-align: right;\"><a href=\"https://github.com/mickleness/aspen-birdcage/wiki/Cardinal-Database-Navigator\" data-tippy-content=\"Open a wiki containing notes about how to use this page.\">See Documentation</a></div>\n"+
            "  <script>\n" +
            "    initializeUI();\n" +
            "  </script>\n" +
            "</html>\n";

	private String customFileName;
	
	@Override
	protected void run() throws Exception {
		try(OutputStream out = getResultHandler().getOutputStream()) {
			String html = CardinalExport.htmlTemplate;
			html = html.replace(CardinalExport.keywordReplacementKey, createJavascript());
			out.write(html.getBytes(Charset.forName("UTF-8")));
		}
	}

	private int getRecordCount(BeanTablePath btp) throws Exception {
		BeanColumnPath oidPath = btp.getColumn("oid");
		String sql = "SELECT COUNT("+oidPath.getDatabaseName()+") FROM "+btp.getDatabaseName()+";";
		final AtomicReference<String> result = new AtomicReference<>();
		SqlResultHandler h = new SqlResultHandler() {
			@Override
			public boolean process(X2Broker broker, String sql,
					ResultSet resultSet) throws SQLException {
				result.set(resultSet.getString(1));
				return true;
			}
		};
		h.executeSQL(getBroker(), sql);

		return Integer.parseInt(result.get());
	}
	
    private void initializeFileInfo() {
        Random random = new Random();
        customFileName = "cardinal" + random.nextInt(1000) + ".html";
        getJob().getInput().setFormat(ToolInput.HTML_FORMAT);
    }

    @Override
    public String getCustomFileName() {
        initializeFileInfo();
        if (customFileName != null) {
            return customFileName;
        }
        return super.getCustomFileName();
    }

	@Override
	protected void initialize() throws X2BaseException {
		super.initialize();
		initializeFileInfo();
	}

	/**
	 * Create a block of javascript that begins with "function initializeTableInfo() {"
	 */
	private String createJavascript() throws Exception {
		boolean includeTableRowCount = Boolean.parseBoolean( String.valueOf(getParameter(PARAM_INCLUDE_TABLE_ROW_COUNT)) );
		StringWriter writer = new StringWriter();
		List<BeanColumnPath> allColumns = new ArrayList<>();
		List<BeanTablePath> allRelationships = new ArrayList<>();
		for(BeanTablePath btp : BeanTablePath.getAllTables()) {
			allColumns.addAll(btp.getColumns());
			allRelationships.addAll(btp.getTables());
		}
		
		writer.write("function initializeTableInfo() {\n");
		writer.write("tableMap = {\n");
		List<BeanTablePath> tables = new ArrayList<>();
		tables.addAll(BeanTablePath.getAllTables());
		for(int a = 0; a<tables.size(); a++) {
			ThreadUtils.checkInterrupt();
			BeanTablePath btp = tables.get(a);
			DataDictionaryTable ddt = btp.getDataDictionaryTable(getBroker());
			
			StringBuilder sb = new StringBuilder();
			sb.append(btp.getDictionaryID()+": [");
			sb.append("'"+btp.getDatabaseName()+"', ");
			sb.append("'"+btp.getObjectPrefix()+"', ");
			sb.append("'"+btp.getValueType().getSimpleName()+"', ");
			if(includeTableRowCount) {
				sb.append("'"+getRecordCount(btp)+"', ");
			} else {
				sb.append("'-1', ");
			}
			sb.append("'"+ddt.isAudited()+"'");
			
			sb.append("]");
			if(a<allColumns.size()-1)
				sb.append(",");
			sb.append("\n");

			writer.write("  "+sb.toString());
		}
		writer.write("};\n");
		
		writer.write("columnMap = {\n");
		
		for(int a = 0; a<allColumns.size(); a++) {
			ThreadUtils.checkInterrupt();
			BeanColumnPath bcp = allColumns.get(a);
			DataDictionaryField field = bcp.getField(getBroker().getPersistenceKey());

			DataFieldConfig fdd = field.getDataFieldConfig();
			String shortName = "";
			String longName = "";
			if(fdd != null)
			{
				shortName = field.getUserShortName().replace("'","");
				longName = field.getUserLongName().replace("'","");
			}
			
			StringBuilder sb = new StringBuilder();
			sb.append(bcp.getColumnOid()+": [");
			sb.append("'"+bcp.toString()+"',");
			sb.append("'"+bcp.getDatabaseName()+"',");
			
			sb.append("'"+getSimpleName(field.getJavaType())+"',");
			sb.append("'"+field.getJdbcType()+"', ");
			sb.append("'"+field.getDatabaseLength()+"',");
			if(fdd != null)
			{
				sb.append("'"+shortName+"',");
				sb.append("'"+longName+"',");
			}
			else
			{
				sb.append("\",\"");
				sb.append("\",\"");
			}

			StringBuilder flags = new StringBuilder();
			if(field.isEnabled()) {
				flags.append(" enabled");
			}
			if(field.isReadOnly()) {
				flags.append(" read-only");
			}
			if(field.isRequired()) {
				flags.append(" required");
			}
			if(field.isSystemUseOnly()) {
				flags.append(" system-use-only");
			}
			sb.append(" '"+flags.toString().trim()+"'");
			
			sb.append("]");
			if(a<allColumns.size()-1)
				sb.append(",");
			sb.append("\n");
			
			writer.write("  "+sb.toString());
		}
		writer.write("};\n");
		
		writer.write("relationshipMap = {\n");
		for(int a = 0; a<allRelationships.size(); a++) {
			ThreadUtils.checkInterrupt();
			BeanTablePath btp = allRelationships.get(a);
			DataDictionaryTable ddt = btp.getDataDictionaryTable(getBroker());

			StringBuilder sb = new StringBuilder();
			//this method exists, but not in this version of aspen-xr
			String relationshipOid = (String) BeanTablePath.class.getMethod("getRelationshipOid").invoke(btp);
			sb.append(relationshipOid+": ");
			sb.append("['"+ddt.getDisplayString()+"', ");
			
			sb.append("'"+btp.getPath()+"',");
			sb.append("'"+btp.getPrimaryColumn()+"',");
			sb.append("'"+btp.getPrimaryIndex()+"',");
			sb.append("'"+btp.getRelatedIndex()+"',");
			sb.append("'"+btp.getRelationshipType()+"'");
			sb.append("]");
			if(a<allRelationships.size()-1)
				sb.append(",");
			sb.append("\n");
			
			writer.write("  "+sb.toString());
		}
		writer.write("};\n");
		
		writer.write("};\n");
		return writer.toString();
	}
	
	private String getSimpleName(String name) {
		int i = name.lastIndexOf('.');
		if(i==-1)
			return name;
		return name.substring(i+1);
	}
}
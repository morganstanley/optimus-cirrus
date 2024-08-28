/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
function layoutRow(name, level, r) {
    var html = ""
    var margin = "<td style='padding-left: " + (2 + level * 20) + ";'>"
    for(p in r) {
        var value = r[p]
        if(p === "result") continue
        if(p === "title") continue
        if(p === "highlight") continue

        if('object' === typeof(value)) {
            var style = value.highlight ? "class='highlight'" : ""
            var tr = 'string' === typeof(value.title) ? "<tr title='" + value.title + "'" : "<tr"
            tr += style + ">"

            html = html + tr + margin + p + "</td><td>" + value.result + "</td></tr>"
            html = html + layoutRow(p, level + 1, value)
        }
        else {
          html = html + margin + p + "</td><td>" + value + "</td></tr>"
        }
    }
    return html
}

function loadTables(id) {
  if (typeof(gridprofiler) === 'undefined') return;

  var html = ""

  for (name in gridprofiler) {
    var g = gridprofiler[name]
    html += "<table class='tableStyle'>"
    html += "<h3>" + g.title + "</h3>"
    html += "<tbody id='stats'>"

    html += layoutRow("", 0, g)
    html += "</tbody></table>"
  }

  document.getElementById(id).innerHTML = html
}

function layoutConcurrency(values, topN, expandable) {
  var html = ""
  for(i in values) {
    if(i == topN) return html
    var e = values[i]
    var desc = e.desc.split("\n")
    var topDisplayFrame = displayFrame(desc)
    var title = "title='" + e.desc + "'"
    if(expandable) {
      rowId = "row_hidden_" + i
      btnId = "toggle_btn_" + rowId
      linkId = btnId + "frame"
      html += "<tr " + title + " onclick='copy_title(this)'><td>" +
            "<button type=\"button\" id=\"" + btnId + "\" class=\"toggleBtn\" aria-expanded=\"false\"" +
            " onclick=\"toggle(this.id, '" + rowId + "');\" aria-controls=\"" + rowId + "\" >" +
            "&#x3e;" + "</button></td>"
      html += "<td id=\""+ linkId + "\">" + e.plugin + "</td>"
      html += "<td>" + e.count + "</td>"
      html += "<td>" + e.module + "</td>"
      html += "<td>" + topDisplayFrame + "</td></tr>"

      //  add the hidden expandable row
      html += "<tr id=\"" + rowId + "\" class=\"hide\"><td></td><td></td><td></td>"
      html += "<td>"
      for(idx in desc)
        html += "<p class=\"frameStyle\">" + desc[idx] + "</p>"
      html += "</td></tr>"
    }
    else {
      html += "<tr " + title + " onclick='copy_title(this)'>" + "<td>" + e.plugin + "</td><td>" + e.count + "</td><td>" + desc[0] + "</td></tr>"
    }
  }
  return html
}

function displayFrame(frames) {
  var frame = frames[0]
  for(idx in frames) {
    if(!frames[idx].includes("optimus.platform.AsyncBase") && !frames[idx].includes("optimus.core.CoreAPI.given$withNode")
    && !frames[idx].includes("optimus.platform.package$.given$withNode")) {
      frame = frames[idx]
      break
    }
  }
  return frame
}

function toggle(btnID, rowID) {
  var buttonToggle = document.getElementById(btnID);
  var rowHidden = document.getElementById(rowID);
  var ariaExpanded = buttonToggle.getAttribute("aria-expanded");
  if(ariaExpanded === 'false') {
    rowHidden.classList.remove("hidden");
    rowHidden.classList.add("show");
    buttonToggle.setAttribute('aria-expanded', 'true');
  }
  else {
    rowHidden.classList.remove("show")
    rowHidden.classList.add("hidden")
    buttonToggle.setAttribute('aria-expanded', 'false')
  }
}

function copy_title(elm) {
  navigator.clipboard.writeText(elm.title)
}

function loadLostConcurrencyTable(divid, tbodyId, values, expandable) {
  if (values === undefined || values.length == 0)
    document.getElementById(divid).hidden = true
  else
    document.getElementById(tbodyId).innerHTML = layoutConcurrency(values, 10, expandable)
}

function loadLostConcurrences() {
  if (typeof(lost_cc) === 'undefined' || (lost_cc.css === undefined && lost_cc.ncl === undefined)) {
    document.getElementById("lost_cc_header").hidden = true
    return ;
  }

 loadLostConcurrencyTable("lost_cc_css_div", "lost_cc_css", lost_cc.css, true)
 loadLostConcurrencyTable("lost_cc_ncl_div", "lost_cc_ncl", lost_cc.ncl, false)
}

function hotspotsTable() {
  var html = ""
  for (i in hotspots) {
     var e = hotspots[i]
     html += "<tr>"
     var highlight = false
     for (prop in e) {
        var value = e[prop]
        if (value === undefined) html += "<td></td>"
        else {
          if (prop === "highlight") highlight = true
          else {
            if (prop === "benefit" && highlight) html += "<td title = 'Consider disabling caching for this node' class='highlight'>"
            else html += "<td>"
            html += value
            html += "</td>"
          }
        }
     }
     html += "</tr>"
  }
  return html
}

function loadHotspots() {
  if (typeof(hotspots) === 'undefined' || hotspots.length == 0) {
    document.getElementById("hotspots_header").hidden = true
    return ;
  }

  document.getElementById("hotspots_body").innerHTML = hotspotsTable()
}

function loadRTFrameViolations(tbodyId, violations) {
  var html = ""
  for(idx in violations) {
    var violation = violations[idx]
    var stackTraceViolations = violation.stackTraces
    if (stackTraceViolations.length == 1) {
      // no need for an inner table row if we only got one stack trace for the frame
      html += loadRTStackTraceViolations(tbodyId + idx, stackTraceViolations)
    } else {
      rowId = "row_hidden_" + tbodyId + idx
      btnId = "toggle_btn_" + tbodyId + rowId
      linkId = btnId + "frame"
      html += "<tr><td>" +
            "<button type=\"button\" id=\"" + btnId + "\" class=\"toggleBtn\" aria-expanded=\"false\"" +
            " onclick=\"toggle(this.id, '" + rowId + "');\" aria-controls=\"" + rowId + "\" >" +
            "&#x3e;" + "</button></td>"
      html += "<td>" + violation.count + "</td>"
      html += "<td>" + violation.owner + "</td>"
      html += "<td>" + violation.frame + "</td></tr>"

      //  add the hidden expandable inner table row
      inner_tbodyId = tbodyId + idx + "_body"
      html += "<tr id=\"" + rowId + "\" class=\"hide\"><td></td>"
      html += "<td colspan=\"3\">"
      html += "<table class=\"innerTableStyle\">"
      html += "<tbody id=\"" + inner_tbodyId + "\"></tbody>"
      html += loadRTStackTraceViolations(inner_tbodyId, stackTraceViolations)
      html += "</table>"
      html += "</td></tr>"
    }
  }
  return html
}

function loadRTStackTraceViolations(tbodyId, violations) {
  var html = ""
  for(idx in violations) {
    var violation = violations[idx]
    var desc = violation.stackTrace.split("\n")
    var title = "title='" + violation.stackTrace + "'"
    rowId = "row_hidden_" + tbodyId + idx
    btnId = "toggle_btn_" + tbodyId + rowId
    linkId = btnId + "frame"
    html += "<tr " + title + " onclick='copy_title(this)'><td>" +
          "<button type=\"button\" id=\"" + btnId + "\" class=\"toggleBtn\" aria-expanded=\"false\"" +
          " onclick=\"toggle(this.id, '" + rowId + "');\" aria-controls=\"" + rowId + "\" >" +
          "&#x3e;" + "</button></td>"
    html += "<td>" + violation.count + "</td>"
    html += "<td>" + violation.owner + "</td>"
    html += "<td>" + violation.frame + "</td></tr>"

    //  add the hidden expandable row
    html += "<tr id=\"" + rowId + "\" class=\"hide\"><td></td><td></td><td></td>"
    html += "<td>"
    for(idx in desc)
      html += "<p class=\"frameStyle\">" + desc[idx] + "</p>"
    html += "</td></tr>"
  }
  return html
}

function generateOwnerCheckboxes() {
  var owners = [];
  Object.values(rtViolations).flat().forEach(function (violation) {
    if (owners.indexOf(violation.owner) === -1) owners.push(violation.owner)
  });

  var html = ""
  owners.sort().forEach(function (owner) {
    html += `<label><input type="checkbox" name="rtv_owner" value="${owner}" onclick="generateRTVTables()" checked /> ${owner}</label>`
  });
  var rtvOwnersCheckboxes = document.getElementById("rtv_owners_checkboxes")
  var rows = Math.max(3, Math.floor(owners.length / 4));
  rtvOwnersCheckboxes.style.gridTemplateRows = `repeat(${rows}, 1fr)`;
  rtvOwnersCheckboxes.innerHTML = html
}

function generateRTVTable(category, values) {
  var divPrefix = `rtv_${category.name.toLowerCase()}`
  var html = `<div id="${divPrefix}_div">\
                <h3>RT VIOLATION: ${category.name}<sup title="Click on table row to copy stacktrace to clipboard" style="font-size: small">*</sup></h3>\
                <p class="description">${category.description}</p>\
                <table class="tableStyle">\
                    <thead><tr><th></th><th>Count</th><th>${category.ownerType}</th><th>Frame</th></tr></thead>\
                    <tbody id="${divPrefix}_body">\
                      ${loadRTFrameViolations(divPrefix + "_body", values)}
                    </tbody>\
                </table>\
            </div>`
  return html
}

function generateRTVTables() {
  var html = ""
  var checked = document.querySelectorAll('input[name=rtv_owner]:checked')
  var owners = Array.from(checked).map(c => c.value)

  rtvCategories.forEach(function(category) {
    var values = rtViolations[category.name]
    if (values === undefined || values.length == 0) return ;

    var interestingValues = values.filter(v => owners.includes(v.owner));
    if (interestingValues.length > 0) html += generateRTVTable(category, interestingValues)
  });

  document.getElementById("rtv_tables").innerHTML = html
}

function loadRTViolations() {
  if (typeof(rtViolations) === 'undefined') {
    document.getElementById("rtv_header").hidden = true
    return ;
  }

  generateOwnerCheckboxes();
  generateRTVTables();
}

function setAllRTVOwners(checked) {
  document.getElementsByName("rtv_owner").forEach(function (owner) {
      owner.checked=checked;
  });
}
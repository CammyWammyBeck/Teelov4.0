// Admin SQL Editor - client-side logic

var editor = CodeMirror.fromTextArea(document.getElementById('sql-editor'), {
    mode: 'text/x-sql',
    lineNumbers: true,
    matchBrackets: true,
    indentWithTabs: false,
    tabSize: 2,
    autofocus: true,
    viewportMargin: Infinity,
});

// State
var currentQuery = '';
var sortColumn = null;
var sortDir = 'asc';
var currentData = null; // shared reference for event handlers

// DOM refs
var runBtn = document.getElementById('run-btn');
var statusEl = document.getElementById('query-status');
var resultsPanel = document.getElementById('results-panel');
var resultsHead = document.getElementById('results-head');
var resultsBody = document.getElementById('results-body');
var resultsInfo = document.getElementById('results-info');
var errorPanel = document.getElementById('error-panel');
var errorMessage = document.getElementById('error-message');
var successPanel = document.getElementById('success-panel');
var successMessage = document.getElementById('success-message');
var previewPanel = document.getElementById('preview-panel');
var previewInfo = document.getElementById('preview-info');
var confirmBtn = document.getElementById('confirm-btn');
var cancelBtn = document.getElementById('cancel-btn');
var copyToast = document.getElementById('copy-toast');

function hideAll() {
    resultsPanel.classList.add('hidden');
    errorPanel.classList.add('hidden');
    successPanel.classList.add('hidden');
    previewPanel.classList.add('hidden');
}

function showError(msg) {
    hideAll();
    errorMessage.textContent = msg;
    errorPanel.classList.remove('hidden');
}

function showSuccess(msg) {
    hideAll();
    successMessage.textContent = msg;
    successPanel.classList.remove('hidden');
}

function escapeHtml(str) {
    var div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function showToast(msg, isError) {
    copyToast.textContent = msg;
    copyToast.style.background = isError ? '#dc2626' : '#16a34a';
    copyToast.classList.add('show');
    setTimeout(function() { copyToast.classList.remove('show'); }, 2000);
}


// Escape a SQL string value (basic quoting)
function sqlEscape(val) {
    if (val === 'NULL') return 'NULL';
    return "'" + val.replace(/'/g, "''") + "'";
}

async function runQuery(action, options) {
    action = action || 'execute';
    options = options || {};
    var sql = editor.getValue().trim();
    if (!sql) return;

    currentQuery = sql;
    statusEl.textContent = 'Running...';
    runBtn.disabled = true;

    try {
        var resp = await fetch('/admin/sql/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: sql, action: action }),
        });
        var data = await resp.json();

        if (!resp.ok) {
            showError(data.error || 'Query failed');
            statusEl.textContent = 'Error';
            return;
        }

        if (data.type === 'count_check') {
            if (data.total_rows > 1000) {
                hideAll();
                previewInfo.textContent = 'This query will return ' + data.total_rows + ' rows. Load all results anyway?';
                previewPanel.classList.remove('hidden');
                // Temporarily rewire confirm button to force-load
                confirmBtn.onclick = function() {
                    confirmBtn.onclick = null;
                    runQuery('force');
                };
                cancelBtn.onclick = function() {
                    cancelBtn.onclick = null;
                    previewPanel.classList.add('hidden');
                    statusEl.textContent = 'Cancelled';
                    // Restore default cancel behavior
                    cancelBtn.addEventListener('click', defaultCancelHandler);
                };
                statusEl.textContent = data.total_rows + ' rows — confirm to load';
            } else {
                // Under threshold, just load
                runQuery('force');
            }
            return;
        }

        if (data.type === 'select') {
            currentData = data;
            renderResults(data);
            statusEl.textContent = data.total_rows + ' row' + (data.total_rows !== 1 ? 's' : '') + ' returned';
        } else if (data.type === 'preview') {
            hideAll();
            previewInfo.textContent = 'This query will affect ' + data.affected_rows + ' row(s). Review and confirm to execute.';
            previewPanel.classList.remove('hidden');
            statusEl.textContent = 'Preview';
        } else if (data.type === 'mutation') {
            showSuccess('Query executed successfully. ' + data.affected_rows + ' row(s) affected.');
            statusEl.textContent = 'Done';
        }
    } catch (e) {
        showError('Network error: ' + e.message);
        statusEl.textContent = 'Error';
    } finally {
        runBtn.disabled = false;
    }
}

// Inline edit: send UPDATE and save immediately
async function saveInlineEdit(tableName, pkColumns, columns, row, colIdx, newValue) {
    // Build WHERE clause from PK columns
    var whereParts = [];
    for (var p = 0; p < pkColumns.length; p++) {
        var pkIdx = columns.indexOf(pkColumns[p]);
        if (pkIdx === -1) return { error: 'Primary key column "' + pkColumns[p] + '" not in SELECT results. Include it in your query.' };
        var pkVal = row[pkIdx];
        whereParts.push(pkColumns[p] + ' = ' + sqlEscape(String(pkVal)));
    }

    var colName = columns[colIdx];
    var sql = 'UPDATE ' + tableName + ' SET ' + colName + ' = ' + sqlEscape(newValue) + ' WHERE ' + whereParts.join(' AND ');

    var resp = await fetch('/admin/sql/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: sql, action: 'confirm' }),
    });
    var result = await resp.json();
    if (!resp.ok) {
        return { error: result.error || 'Update failed' };
    }
    if (result.affected_rows === 0) {
        return { error: 'No rows matched — update had no effect.' };
    }
    return result;
}

function canInlineEdit(data) {
    return data.table_name && data.pk_columns && data.pk_columns.length > 0;
}

function renderResults(data) {
    hideAll();
    var editable = canInlineEdit(data);
    // Update shared reference so event handlers always use latest data
    currentData = data;

    // Header
    var headerHtml = '<tr>';
    for (var i = 0; i < data.columns.length; i++) {
        headerHtml += '<th class="px-4 py-2 cursor-pointer hover:text-teelo-dark select-none" data-col="' + escapeHtml(data.columns[i]) + '">' + escapeHtml(data.columns[i]) + '</th>';
    }
    headerHtml += '</tr>';
    resultsHead.innerHTML = headerHtml;

    // Body
    var bodyHtml = '';
    for (var r = 0; r < data.rows.length; r++) {
        bodyHtml += '<tr class="hover:bg-gray-50" data-row-idx="' + r + '">';
        for (var c = 0; c < data.rows[r].length; c++) {
            var cell = data.rows[r][c];
            var isPk = data.pk_columns && data.pk_columns.indexOf(data.columns[c]) !== -1;
            var display = cell === null ? '<span class="text-gray-300 italic">NULL</span>' : escapeHtml(String(cell));
            var editClass = (editable && !isPk) ? ' cursor-text' : '';
            bodyHtml += '<td class="px-4 py-2 whitespace-nowrap max-w-xs truncate' + editClass + '" data-col-idx="' + c + '">' + display + '</td>';
        }
        bodyHtml += '</tr>';
    }
    resultsBody.innerHTML = bodyHtml;

    resultsInfo.textContent = data.total_rows + ' row' + (data.total_rows !== 1 ? 's' : '');
    if (editable) {
        resultsInfo.textContent += ' (double-click to edit)';
    }
    resultsPanel.classList.remove('hidden');

    // Sort handlers on headers (these are on fresh DOM elements, so no duplication)
    var ths = resultsHead.querySelectorAll('th');
    for (var t = 0; t < ths.length; t++) {
        (function(th) {
            th.addEventListener('click', function() {
                var col = th.dataset.col;
                var colIdx = currentData.columns.indexOf(col);
                if (colIdx === -1) return;

                if (sortColumn === col) {
                    sortDir = sortDir === 'asc' ? 'desc' : 'asc';
                } else {
                    sortColumn = col;
                    sortDir = 'asc';
                }

                currentData.rows.sort(function(a, b) {
                    var va = a[colIdx], vb = b[colIdx];
                    if (va === null) return 1;
                    if (vb === null) return -1;
                    if (typeof va === 'number' && typeof vb === 'number') {
                        return sortDir === 'asc' ? va - vb : vb - va;
                    }
                    va = String(va); vb = String(vb);
                    return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
                });
                renderResults(currentData);
            });
        })(ths[t]);
    }
}

// ---- Event delegation on resultsBody (set up ONCE, not per render) ----

resultsBody.addEventListener('dblclick', function(e) {
    var td = e.target.closest('td');
    if (!td || !currentData) return;
    if (!canInlineEdit(currentData)) return;

    var colIdx = parseInt(td.dataset.colIdx, 10);
    var isPk = currentData.pk_columns.indexOf(currentData.columns[colIdx]) !== -1;
    if (isPk) return;

    // Don't re-enter edit mode
    if (td.querySelector('input')) return;

    var tr = td.closest('tr');
    var rowIdx = parseInt(tr.dataset.rowIdx, 10);
    var currentVal = currentData.rows[rowIdx][colIdx];
    var displayVal = currentVal === null ? '' : String(currentVal);

    // Create input
    var input = document.createElement('input');
    input.type = 'text';
    input.value = displayVal;
    input.className = 'w-full px-1 py-0.5 text-sm border border-teelo-lime rounded focus:outline-none focus:ring-2 focus:ring-teelo-lime/50 bg-yellow-50';
    input.style.minWidth = '60px';

    td.innerHTML = '';
    td.appendChild(input);
    input.focus();
    input.select();

    var saving = false;

    function commitEdit() {
        if (saving) return;
        var newVal = input.value;

        // No change
        if (newVal === displayVal) {
            restoreCell();
            return;
        }

        saving = true;
        input.disabled = true;
        td.style.opacity = '0.5';

        saveInlineEdit(currentData.table_name, currentData.pk_columns, currentData.columns, currentData.rows[rowIdx], colIdx, newVal).then(function(result) {
            if (result.error) {
                showToast(result.error, true);
                restoreCell();
            } else {
                // Update local data
                var parsed = newVal === 'NULL' ? null : newVal;
                currentData.rows[rowIdx][colIdx] = parsed;
                var display = parsed === null ? '<span class="text-gray-300 italic">NULL</span>' : escapeHtml(String(parsed));
                td.innerHTML = display;
                td.style.opacity = '';
                td.style.background = '#f0fdf4';
                setTimeout(function() { td.style.background = ''; }, 1500);
                showToast('Saved');
            }
            saving = false;
        }).catch(function(err) {
            showToast('Error: ' + err.message, true);
            restoreCell();
            saving = false;
        });
    }

    function restoreCell() {
        var display = currentVal === null ? '<span class="text-gray-300 italic">NULL</span>' : escapeHtml(String(currentVal));
        td.innerHTML = display;
        td.style.opacity = '';
    }

    input.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            commitEdit();
        } else if (e.key === 'Escape') {
            e.preventDefault();
            restoreCell();
        }
    });

    input.addEventListener('blur', function() {
        setTimeout(function() {
            if (!saving) commitEdit();
        }, 100);
    });
});

// ---- Other event listeners ----

function defaultCancelHandler() {
    previewPanel.classList.add('hidden');
    statusEl.textContent = 'Cancelled';
}

runBtn.addEventListener('click', function() { runQuery('check_count'); });
editor.setOption('extraKeys', {
    'Ctrl-Enter': function() { runQuery('check_count'); },
    'Cmd-Enter': function() { runQuery('check_count'); },
});

confirmBtn.addEventListener('click', function() { runQuery('confirm'); });
cancelBtn.addEventListener('click', defaultCancelHandler);

// Schema browser
var schemaBtns = document.querySelectorAll('.schema-table-btn');
for (var s = 0; s < schemaBtns.length; s++) {
    (function(btn) {
        btn.addEventListener('click', function() {
            var colsDiv = btn.nextElementSibling;
            var chevron = btn.querySelector('.schema-chevron');
            var isOpen = !colsDiv.classList.contains('hidden');

            colsDiv.classList.toggle('hidden');
            chevron.style.transform = isOpen ? '' : 'rotate(90deg)';
        });

        btn.addEventListener('dblclick', function() {
            var table = btn.dataset.table;
            editor.setValue('SELECT * FROM ' + table + ' LIMIT 50;');
            editor.focus();
        });
    })(schemaBtns[s]);
}

// Recent queries
var recentQueriesEl = document.getElementById('recent-queries');

async function loadRecentQueries() {
    try {
        var resp = await fetch('/admin/sql/recent');
        var data = await resp.json();
        if (!resp.ok || !data.queries || data.queries.length === 0) {
            recentQueriesEl.innerHTML = '<p class="text-xs text-gray-400 italic">No recent queries</p>';
            return;
        }
        var html = '';
        for (var i = 0; i < data.queries.length; i++) {
            var q = data.queries[i];
            var truncated = q.query.length > 80 ? q.query.substring(0, 80) + '...' : q.query;
            var ago = q.last_run ? timeAgo(new Date(q.last_run)) : '';
            html += '<button class="recent-query-btn w-full text-left px-2 py-1.5 rounded-lg text-xs font-mono text-gray-600 hover:bg-gray-100 hover:text-teelo-dark transition truncate block" title="' + escapeHtml(q.query).replace(/"/g, '&quot;') + '">';
            html += '<span class="block truncate">' + escapeHtml(truncated) + '</span>';
            if (ago) html += '<span class="block text-[10px] text-gray-400 mt-0.5">' + escapeHtml(ago) + '</span>';
            html += '</button>';
        }
        recentQueriesEl.innerHTML = html;

        // Click handlers
        var btns = recentQueriesEl.querySelectorAll('.recent-query-btn');
        for (var b = 0; b < btns.length; b++) {
            (function(btn, query) {
                btn.addEventListener('click', function() {
                    editor.setValue(query);
                    editor.focus();
                    editor.setCursor(editor.lineCount(), 0);
                });
            })(btns[b], data.queries[b].query);
        }
    } catch (e) {
        recentQueriesEl.innerHTML = '<p class="text-xs text-gray-400 italic">Failed to load</p>';
    }
}

function timeAgo(date) {
    var seconds = Math.floor((new Date() - date) / 1000);
    if (seconds < 60) return 'just now';
    var minutes = Math.floor(seconds / 60);
    if (minutes < 60) return minutes + 'm ago';
    var hours = Math.floor(minutes / 60);
    if (hours < 24) return hours + 'h ago';
    var days = Math.floor(hours / 24);
    if (days < 7) return days + 'd ago';
    return date.toLocaleDateString();
}

loadRecentQueries();

// Reload recent queries after each successful query execution
var _origRunQuery = runQuery;
runQuery = async function(action, options) {
    await _origRunQuery(action, options);
    // Reload after force (full select) or confirm (mutation)
    if (action === 'force' || action === 'confirm') {
        loadRecentQueries();
    }
};

// Re-init lucide icons
lucide.createIcons();

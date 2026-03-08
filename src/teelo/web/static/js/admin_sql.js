// Admin SQL Editor - client-side logic

const editor = CodeMirror.fromTextArea(document.getElementById('sql-editor'), {
    mode: 'text/x-sql',
    lineNumbers: true,
    matchBrackets: true,
    indentWithTabs: false,
    tabSize: 2,
    autofocus: true,
    viewportMargin: Infinity,
});

// State
let currentPage = 1;
let currentQuery = '';
let sortColumn = null;
let sortDir = 'asc';
let lastResultData = null;

// DOM refs
const runBtn = document.getElementById('run-btn');
const statusEl = document.getElementById('query-status');
const resultsPanel = document.getElementById('results-panel');
const resultsHead = document.getElementById('results-head');
const resultsBody = document.getElementById('results-body');
const resultsInfo = document.getElementById('results-info');
const paginationEl = document.getElementById('pagination');
const prevBtn = document.getElementById('prev-page');
const nextBtn = document.getElementById('next-page');
const pageInfo = document.getElementById('page-info');
const errorPanel = document.getElementById('error-panel');
const errorMessage = document.getElementById('error-message');
const successPanel = document.getElementById('success-panel');
const successMessage = document.getElementById('success-message');
const previewPanel = document.getElementById('preview-panel');
const previewInfo = document.getElementById('preview-info');
const confirmBtn = document.getElementById('confirm-btn');
const cancelBtn = document.getElementById('cancel-btn');
const copyToast = document.getElementById('copy-toast');

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
    const div = document.createElement('div');
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
    if (val === '' || val === 'NULL') return 'NULL';
    return "'" + val.replace(/'/g, "''") + "'";
}

async function runQuery(action, page) {
    action = action || 'execute';
    page = page || 1;
    var sql = editor.getValue().trim();
    if (!sql) return;

    currentQuery = sql;
    currentPage = page;
    statusEl.textContent = 'Running...';
    runBtn.disabled = true;

    try {
        var resp = await fetch('/admin/sql/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: sql, action: action, page: page }),
        });
        var data = await resp.json();

        if (!resp.ok) {
            showError(data.error || 'Query failed');
            statusEl.textContent = 'Error';
            return;
        }

        if (data.type === 'select') {
            lastResultData = data;
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
    var data = await resp.json();
    if (!resp.ok) {
        return { error: data.error || 'Update failed' };
    }
    return data;
}

function canInlineEdit(data) {
    return data.table_name && data.pk_columns && data.pk_columns.length > 0;
}

function renderResults(data) {
    hideAll();
    var editable = canInlineEdit(data);

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

    // Pagination
    if (data.total_pages > 1) {
        paginationEl.classList.remove('hidden');
        pageInfo.textContent = 'Page ' + data.page + ' of ' + data.total_pages;
        prevBtn.disabled = data.page <= 1;
        nextBtn.disabled = data.page >= data.total_pages;
    } else {
        paginationEl.classList.add('hidden');
    }

    // Sort handlers on headers
    var ths = resultsHead.querySelectorAll('th');
    for (var t = 0; t < ths.length; t++) {
        (function(th) {
            th.addEventListener('click', function() {
                var col = th.dataset.col;
                var colIdx = data.columns.indexOf(col);
                if (colIdx === -1) return;

                if (sortColumn === col) {
                    sortDir = sortDir === 'asc' ? 'desc' : 'asc';
                } else {
                    sortColumn = col;
                    sortDir = 'asc';
                }

                data.rows.sort(function(a, b) {
                    var va = a[colIdx], vb = b[colIdx];
                    if (va === null) return 1;
                    if (vb === null) return -1;
                    if (typeof va === 'number' && typeof vb === 'number') {
                        return sortDir === 'asc' ? va - vb : vb - va;
                    }
                    va = String(va); vb = String(vb);
                    return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
                });
                renderResults(Object.assign({}, data));
            });
        })(ths[t]);
    }

    // Cell interactions via event delegation on tbody
    resultsBody.addEventListener('click', function(e) {
        var td = e.target.closest('td');
        if (!td) return;
        // Single click = copy
        navigator.clipboard.writeText(td.textContent).then(function() {
            showToast('Copied!');
        });
    });

    resultsBody.addEventListener('dblclick', function(e) {
        var td = e.target.closest('td');
        if (!td || !editable) return;

        var colIdx = parseInt(td.dataset.colIdx, 10);
        var isPk = data.pk_columns.indexOf(data.columns[colIdx]) !== -1;
        if (isPk) return; // Don't allow editing PK columns

        // Don't re-enter edit mode
        if (td.querySelector('input')) return;

        var tr = td.closest('tr');
        var rowIdx = parseInt(tr.dataset.rowIdx, 10);
        var currentVal = data.rows[rowIdx][colIdx];
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

            saveInlineEdit(data.table_name, data.pk_columns, data.columns, data.rows[rowIdx], colIdx, newVal).then(function(result) {
                if (result.error) {
                    showToast(result.error, true);
                    restoreCell();
                } else {
                    // Update local data
                    var parsed = newVal === 'NULL' ? null : newVal;
                    data.rows[rowIdx][colIdx] = parsed;
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
            // Small delay to allow click on other elements
            setTimeout(function() {
                if (!saving) commitEdit();
            }, 100);
        });
    });
}

// Event listeners
runBtn.addEventListener('click', function() { runQuery('execute'); });
editor.setOption('extraKeys', {
    'Ctrl-Enter': function() { runQuery('execute'); },
    'Cmd-Enter': function() { runQuery('execute'); },
});

confirmBtn.addEventListener('click', function() { runQuery('confirm'); });
cancelBtn.addEventListener('click', function() {
    previewPanel.classList.add('hidden');
    statusEl.textContent = 'Cancelled';
});

prevBtn.addEventListener('click', function() { runQuery('execute', currentPage - 1); });
nextBtn.addEventListener('click', function() { runQuery('execute', currentPage + 1); });

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

// Re-init lucide icons
lucide.createIcons();

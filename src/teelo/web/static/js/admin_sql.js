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

function showToast(msg) {
    copyToast.textContent = msg;
    copyToast.classList.add('show');
    setTimeout(() => copyToast.classList.remove('show'), 1500);
}

async function runQuery(action, page) {
    action = action || 'execute';
    page = page || 1;
    const sql = editor.getValue().trim();
    if (!sql) return;

    currentQuery = sql;
    currentPage = page;
    statusEl.textContent = 'Running...';
    runBtn.disabled = true;

    try {
        const resp = await fetch('/admin/sql/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: sql, action: action, page: page }),
        });
        const data = await resp.json();

        if (!resp.ok) {
            showError(data.error || 'Query failed');
            statusEl.textContent = 'Error';
            return;
        }

        if (data.type === 'select') {
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

function renderResults(data) {
    hideAll();

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
        bodyHtml += '<tr class="hover:bg-gray-50">';
        for (var c = 0; c < data.rows[r].length; c++) {
            var cell = data.rows[r][c];
            var display = cell === null ? '<span class="text-gray-300 italic">NULL</span>' : escapeHtml(String(cell));
            bodyHtml += '<td class="px-4 py-2 whitespace-nowrap max-w-xs truncate">' + display + '</td>';
        }
        bodyHtml += '</tr>';
    }
    resultsBody.innerHTML = bodyHtml;

    resultsInfo.textContent = data.total_rows + ' row' + (data.total_rows !== 1 ? 's' : '');
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

    // Copy on click
    var tds = resultsBody.querySelectorAll('td');
    for (var d = 0; d < tds.length; d++) {
        (function(td) {
            td.addEventListener('click', function() {
                navigator.clipboard.writeText(td.textContent).then(function() {
                    showToast('Copied!');
                });
            });
        })(tds[d]);
    }
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

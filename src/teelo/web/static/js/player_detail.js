/* Player detail page: load and render overview payload. */
(function () {
    var root = document.getElementById('player-page-root');
    if (!root) return;

    var playerId = root.getAttribute('data-player-id');
    if (!playerId) return;
    var selectedRange = 'career';
    var selectedSurface = 'overall';
    var historyChart = null;
    var matchesState = {
        page: 1,
        hasMore: false,
        loading: false
    };

    function setText(id, value) {
        var el = document.getElementById(id);
        if (!el) return;
        el.textContent = value == null || value === '' ? '—' : String(value);
    }

    function setLoading(id, isLoading) {
        var el = document.getElementById(id);
        if (!el) return;
        if (isLoading) el.classList.remove('hidden');
        else el.classList.add('hidden');
    }

    function setVisible(id, isVisible) {
        var el = document.getElementById(id);
        if (!el) return;
        if (isVisible) el.classList.remove('hidden');
        else el.classList.add('hidden');
    }

    function setSectionLoading(spinnerId, contentIds, isLoading) {
        setLoading(spinnerId, isLoading);
        for (var i = 0; i < contentIds.length; i++) {
            setVisible(contentIds[i], !isLoading);
        }
    }

    function renderRecord(id, block) {
        if (!block) {
            setText(id, '—');
            return;
        }
        setText(id, block.wins + '-' + block.losses);
    }

    function renderSurfaceCards(items) {
        var container = document.getElementById('surface-cards');
        if (!container) return;
        if (!items || items.length === 0) {
            container.innerHTML = '<p class="text-sm text-gray-400">No surface Elo data yet.</p>';
            return;
        }

        var surfaceAccent = {
            'Hard': 'border-t-2 surface-border-hard',
            'Clay': 'border-t-2 surface-border-clay',
            'Grass': 'border-t-2 surface-border-grass',
            'Indoor': 'border-t-2 surface-border-indoor'
        };
        container.innerHTML = items.map(function (row) {
            var ratingText = row.rating == null ? '—' : row.rating;
            var rankText = row.rank == null ? '—' : ('#' + row.rank);
            var peakText = row.career_peak == null ? '—' : row.career_peak;
            var countText = row.match_count == null ? 0 : row.match_count;
            var accentCls = surfaceAccent[row.surface] || '';
            return (
                '<div class="rounded-xl border border-gray-100 p-4 bg-gray-50/40 ' + accentCls + '">'
                + '<p class="text-[11px] uppercase tracking-wider text-gray-400 font-semibold">' + escapeHtml(row.surface) + '</p>'
                + '<p class="text-2xl font-bold text-teelo-dark mt-1 tabular-nums">' + ratingText + '</p>'
                + '<p class="text-xs text-gray-500 mt-1">Rank: ' + rankText + '</p>'
                + '<p class="text-xs text-gray-500 mt-1">Peak: ' + peakText + '</p>'
                + '<p class="text-xs text-gray-500">Matches: ' + countText + '</p>'
                + '</div>'
            );
        }).join('');
    }

    function escapeHtml(str) {
        if (!str) return '';
        var div = document.createElement('div');
        div.textContent = String(str);
        return div.innerHTML;
    }

    function buildFallbackMatchHtml(matches) {
        var tableHtml = matches.map(function (m) {
            return (
                '<tr class="hover:bg-gray-50/50 transition-colors duration-75 group border-l-4 border-transparent hover:border-teelo-lime">'
                + '<td class="px-5 py-3">'
                + '<span class="text-sm font-semibold text-teelo-dark">' + escapeHtml(m.tournament_name || 'Unknown') + '</span>'
                + '<span class="text-xs text-gray-400 ml-2">' + escapeHtml(m.round || '') + '</span>'
                + '</td>'
                + '<td class="px-5 py-3 text-right"><span class="text-sm text-teelo-dark">' + escapeHtml(m.player_a && m.player_a.name ? m.player_a.name : 'Unknown') + '</span></td>'
                + '<td class="px-5 py-3 text-center"><span class="inline-block px-2.5 py-1 bg-gray-50 rounded-md text-xs font-mono text-teelo-dark">' + escapeHtml(m.score || 'vs') + '</span></td>'
                + '<td class="px-5 py-3"><span class="text-sm text-teelo-dark">' + escapeHtml(m.player_b && m.player_b.name ? m.player_b.name : 'Unknown') + '</span></td>'
                + '<td class="px-5 py-3 text-right"><span class="text-xs text-gray-400 whitespace-nowrap">' + escapeHtml(m.match_date_display || '') + '</span></td>'
                + '</tr>'
            );
        }).join('');

        var cardHtml = matches.map(function (m) {
            return (
                '<div class="px-4 py-3 border-b border-gray-100 last:border-b-0">'
                + '<div class="text-[13px] font-semibold text-teelo-dark truncate">' + escapeHtml(m.tournament_name || 'Unknown') + '</div>'
                + '<div class="text-[11px] text-gray-400 mb-2">' + escapeHtml(m.round || '') + ' · ' + escapeHtml(m.match_date_display || '') + '</div>'
                + '<div class="text-[13px] text-teelo-dark">' + escapeHtml(m.player_a && m.player_a.name ? m.player_a.name : 'Unknown') + '</div>'
                + '<div class="text-[13px] text-teelo-dark">' + escapeHtml(m.player_b && m.player_b.name ? m.player_b.name : 'Unknown') + '</div>'
                + '</div>'
            );
        }).join('');

        return { tableHtml: tableHtml, cardHtml: cardHtml };
    }

    function renderHistoryChart(data) {
        var points = (data && data.points) || [];
        var canvas = document.getElementById('history-chart-canvas');
        var meta = document.getElementById('history-meta');
        var minEl = document.getElementById('history-min');
        var maxEl = document.getElementById('history-max');
        if (!canvas || !meta || !minEl || !maxEl) return;

        if (historyChart) {
            historyChart.destroy();
            historyChart = null;
        }

        if (!points.length) {
            minEl.textContent = 'Min: —';
            maxEl.textContent = 'Max: —';
            meta.textContent = 'No history for selection';
            return;
        }

        var min = Number(data.min_rating);
        var max = Number(data.max_rating);
        var ctx = canvas.getContext('2d');
        if (!ctx || typeof Chart === 'undefined') {
            minEl.textContent = 'Min: ' + Math.round(min);
            maxEl.textContent = 'Max: ' + Math.round(max);
            meta.textContent = 'Chart library failed to load';
            return;
        }

        var firstTimestamp = Date.now();
        for (var k = 0; k < points.length; k++) {
            if (points[k] && points[k].date) {
                var parsed = Date.parse(points[k].date);
                if (Number.isFinite(parsed)) {
                    firstTimestamp = parsed;
                    break;
                }
            }
        }
        var oneDayMs = 24 * 60 * 60 * 1000;
        var chartPoints = points.map(function (p, idx) {
            var ts = p.date ? Date.parse(p.date) : NaN;
            var xValue = Number.isFinite(ts) ? ts : (firstTimestamp + (idx * oneDayMs));
            return {
                x: xValue,
                y: Number(p.rating),
                date: p.date || null,
                match_id: p.match_id,
                tournament_name: p.tournament_name || null,
                round: p.round || null,
                surface: p.surface || null,
                opponent_name: p.opponent_name || null,
                result: p.result || null,
                score: p.score || null,
                elo_change: p.elo_change
            };
        });
        var realDateValues = chartPoints
            .map(function (p) { return p.date ? Date.parse(p.date) : NaN; })
            .filter(function (v) { return Number.isFinite(v); });
        var realMinX = realDateValues.length ? Math.min.apply(null, realDateValues) : null;
        var realMaxX = realDateValues.length ? Math.max.apply(null, realDateValues) : null;

        function formatShortDate(value) {
            if (!Number.isFinite(value)) return '';
            var d = new Date(value);
            return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
        }

        historyChart = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    label: 'Elo',
                    data: chartPoints,
                    borderColor: '#1A1A1A',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHoverBackgroundColor: '#CCFF00',
                    pointHoverBorderColor: '#1A1A1A',
                    pointHoverBorderWidth: 2,
                    tension: 0,
                    fill: true,
                    backgroundColor: function (context) {
                        var chart = context.chart;
                        var area = chart.chartArea;
                        if (!area) return 'rgba(204, 255, 0, 0.16)';
                        var gradient = chart.ctx.createLinearGradient(0, area.top, 0, area.bottom);
                        gradient.addColorStop(0, 'rgba(204, 255, 0, 0.25)');
                        gradient.addColorStop(1, 'rgba(204, 255, 0, 0)');
                        return gradient;
                    }
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                parsing: false,
                interaction: {
                    mode: 'nearest',
                    intersect: false
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: function (items) {
                                if (!items.length) return '';
                                var raw = items[0].raw || {};
                                if (raw.date) {
                                    var ts = Date.parse(raw.date);
                                    if (Number.isFinite(ts)) return formatShortDate(ts);
                                }
                                return '';
                            },
                            label: function (item) {
                                var raw = item.raw || {};
                                var lines = [];
                                var eloLine = 'Elo ' + Math.round(Number(raw.y));
                                if (raw.elo_change != null) {
                                    var sign = raw.elo_change > 0 ? '+' : '';
                                    eloLine += ' (' + sign + raw.elo_change + ')';
                                }
                                if (raw.result) eloLine += ' • ' + raw.result;
                                lines.push(eloLine);

                                var eventBits = [];
                                if (raw.tournament_name) eventBits.push(raw.tournament_name);
                                if (raw.surface) eventBits.push(raw.surface);
                                if (raw.round) eventBits.push(raw.round);
                                if (eventBits.length) lines.push(eventBits.join(' • '));

                                if (raw.opponent_name) lines.push('vs ' + raw.opponent_name);
                                return lines;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'linear',
                        min: realMinX != null ? realMinX : undefined,
                        max: realMaxX != null ? realMaxX : undefined,
                        title: {
                            display: true,
                            text: 'Date',
                            color: '#6B7280',
                            font: { size: 11, weight: '600' }
                        },
                        grid: {
                            color: 'rgba(15, 17, 21, 0.08)'
                        },
                        ticks: {
                            maxTicksLimit: 6,
                            callback: function (value) {
                                return formatShortDate(Number(value));
                            }
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Elo',
                            color: '#6B7280',
                            font: { size: 11, weight: '600' }
                        },
                        grid: {
                            color: 'rgba(15, 17, 21, 0.08)'
                        },
                        ticks: {
                            maxTicksLimit: 6
                        }
                    }
                }
            }
        });

        minEl.textContent = 'Min: ' + Math.round(min);
        maxEl.textContent = 'Max: ' + Math.round(max);
        meta.textContent = 'Showing ' + data.returned_points + ' / ' + data.total_points + ' points';
    }

    function loadHistory() {
        setSectionLoading('history-loading', ['history-content'], true);
        var url = '/api/players/' + encodeURIComponent(playerId)
            + '/elo-history?range=' + encodeURIComponent(selectedRange)
            + '&surface=' + encodeURIComponent(selectedSurface);
        fetch(url)
            .then(function (res) {
                if (!res.ok) throw new Error('Failed to load history');
                return res.json();
            })
            .then(renderHistoryChart)
            .catch(function (err) {
                console.error(err);
                renderHistoryChart({ points: [] });
            })
            .finally(function () {
                setSectionLoading('history-loading', ['history-content'], false);
            });
    }

    function renderMatches(data, append) {
        var tableBody = document.getElementById('player-matches-table-body');
        var cardsContainer = document.getElementById('player-matches-cards-container');
        var emptyState = document.getElementById('player-matches-empty');
        var countEl = document.getElementById('player-matches-count');
        if (!tableBody || !cardsContainer || !emptyState) return;

        var matches = (data && data.matches) || [];
        if (!append) {
            tableBody.innerHTML = '';
            cardsContainer.innerHTML = '';
            emptyState.classList.add('hidden');
        }

        if (!matches.length && !append) {
            emptyState.classList.remove('hidden');
            if (countEl) countEl.textContent = '0 matches';
            return;
        }

        emptyState.classList.add('hidden');
        if (countEl && data) {
            var total = Number(data.total || 0);
            countEl.textContent = total + ' match' + (total === 1 ? '' : 'es');
        }

        var tableHtml = (data && data.table_rows_html) || '';
        var cardHtml = (data && data.cards_html) || '';
        if (matches.length && (!tableHtml.trim() || !cardHtml.trim())) {
            var fallback = buildFallbackMatchHtml(matches);
            if (!tableHtml.trim()) tableHtml = fallback.tableHtml;
            if (!cardHtml.trim()) cardHtml = fallback.cardHtml;
        }
        if (append) {
            tableBody.insertAdjacentHTML('beforeend', tableHtml);
            cardsContainer.insertAdjacentHTML('beforeend', cardHtml);
        } else {
            tableBody.innerHTML = tableHtml;
            cardsContainer.innerHTML = cardHtml;
        }
        if (typeof lucide !== 'undefined') lucide.createIcons();
    }

    function loadMatches(append) {
        if (matchesState.loading) return;
        matchesState.loading = true;
        if (!append) setSectionLoading('matches-loading', ['matches-content'], true);
        var resultSelect = document.getElementById('matches-result');
        var surfaceSelect = document.getElementById('matches-surface');
        var resultValue = resultSelect ? resultSelect.value : 'all';
        var surfaceValue = surfaceSelect ? surfaceSelect.value : '';
        var page = append ? matchesState.page : 1;
        var url = '/api/players/' + encodeURIComponent(playerId)
            + '/matches?page=' + encodeURIComponent(page)
            + '&per_page=10'
            + '&result=' + encodeURIComponent(resultValue);
        if (surfaceValue) {
            url += '&surface=' + encodeURIComponent(surfaceValue);
        }

        fetch(url)
            .then(function (res) {
                if (!res.ok) throw new Error('Failed to load matches');
                return res.json();
            })
            .then(function (data) {
                renderMatches(data, append);
                matchesState.page = (data.page || 1) + 1;
                matchesState.hasMore = !!data.has_more;
                var moreBtn = document.getElementById('player-matches-more');
                if (moreBtn) {
                    if (matchesState.hasMore) moreBtn.classList.remove('hidden');
                    else moreBtn.classList.add('hidden');
                }
            })
            .catch(function (err) {
                console.error(err);
                if (!append) {
                    renderMatches({ matches: [], total: 0 }, false);
                }
            })
            .finally(function () {
                matchesState.loading = false;
                if (!append) setSectionLoading('matches-loading', ['matches-content'], false);
            });
    }

    function loadTournaments() {
        setSectionLoading('tournaments-loading', ['tournaments-content'], true);
        var allTournaments = [];
        var resultFilter = 'all';
        var surfaceFilter = 'all';
        var tournamentPage = 1;
        var TOURNAMENTS_PER_PAGE = 10;

        fetch('/api/players/' + encodeURIComponent(playerId) + '/tournaments')
            .then(function (res) {
                if (!res.ok) throw new Error('Failed to load tournaments');
                return res.json();
            })
            .then(function (data) {
                allTournaments = data.tournaments || [];
                renderTournamentSummary(data.summary || {});
                applyTournamentFilters();
                setupTournamentFilters();
            })
            .catch(function (err) {
                console.error(err);
                var body = document.getElementById('player-tournaments-body');
                var cards = document.getElementById('player-tournaments-cards');
                if (body) body.innerHTML = '<tr><td colspan="6" class="py-4 text-gray-400">Failed to load tournament data.</td></tr>';
                if (cards) cards.innerHTML = '<div class="py-4 text-center text-gray-400">Failed to load tournament data.</div>';
            })
            .finally(function () {
                setSectionLoading('tournaments-loading', ['tournaments-content'], false);
            });

        function renderTournamentSummary(summary) {
            var el = document.getElementById('tournament-summary');
            if (!el) return;
            var chips = [
                { label: 'Titles', value: summary.titles || 0, bg: '#ccff00', color: '#1a1a1a' },
                { label: 'Finals', value: summary.finals || 0, bg: '#fef08a', color: '#713f12' },
                { label: 'Semis', value: summary.semifinals || 0, bg: '#bfdbfe', color: '#1e3a5f' },
                { label: 'Quarters', value: summary.quarterfinals || 0, bg: '#fed7aa', color: '#7c2d12' },
            ];
            el.innerHTML = chips.map(function (c) {
                return '<span class="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold" style="background:' + c.bg + ';color:' + c.color + '">'
                    + escapeHtml(c.label) + ' <span class="font-bold">' + c.value + '</span></span>';
            }).join('');
        }

        function getResultBadge(result, wonTitle) {
            var label = wonTitle ? 'W' : result;
            var styles = {
                'W': 'background:#ccff00;color:#1a1a1a',
                'F': 'background:#fef08a;color:#713f12',
                'SF': 'background:#bfdbfe;color:#1e3a5f',
                'QF': 'background:#fed7aa;color:#7c2d12',
                'RR': 'background:#e9d5ff;color:#581c87',
            };
            var style = styles[label] || 'background:#f3f4f6;color:#6b7280';
            return '<span class="inline-block px-2 py-0.5 rounded text-xs font-bold" style="' + style + '">' + escapeHtml(label) + '</span>';
        }

        function getSurfaceDot(surface) {
            var colors = { 'Hard': '#3b82f6', 'Clay': '#ea580c', 'Grass': '#16a34a' };
            var color = colors[surface] || '#6b7280';
            return '<span class="inline-block w-2 h-2 rounded-full mr-1" style="background:' + color + '"></span>';
        }

        function renderTournamentTable(tournaments) {
            var body = document.getElementById('player-tournaments-body');
            var cards = document.getElementById('player-tournaments-cards');
            if (body) {
                if (!tournaments.length) {
                    body.innerHTML = '<tr><td colspan="6" class="py-8 text-center text-gray-400">No results found.</td></tr>';
                } else {
                    body.innerHTML = tournaments.map(function (t) {
                        var oppPrefix = t.opponent_name ? (t.won_title ? 'def. ' : 'lost to ') : '';
                        var opp = t.opponent_name ? oppPrefix + escapeHtml(t.opponent_name) : '—';
                        var surface = t.surface || '—';
                        return '<tr class="border-b border-gray-100 hover:bg-gray-50/50">'
                            + '<td class="py-2 pr-4 tabular-nums text-gray-600">' + (t.year || '—') + '</td>'
                            + '<td class="py-2 pr-4 font-medium text-gray-800">' + escapeHtml(t.tournament_name || '—') + '</td>'
                            + '<td class="py-2 pr-4 text-gray-500 text-xs">' + escapeHtml(t.tournament_level || '—') + '</td>'
                            + '<td class="py-2 pr-4 text-gray-600 text-xs">' + getSurfaceDot(surface) + escapeHtml(surface) + '</td>'
                            + '<td class="py-2 pr-4">' + getResultBadge(t.result, t.won_title) + '</td>'
                            + '<td class="py-2 text-gray-500 text-xs">' + opp + '</td>'
                            + '</tr>';
                    }).join('');
                }
            }
            if (cards) {
                if (!tournaments.length) {
                    cards.innerHTML = '<div class="py-8 text-center text-gray-400">No results found.</div>';
                } else {
                    cards.innerHTML = tournaments.map(function (t) {
                        var oppPrefix = t.opponent_name ? (t.won_title ? 'def. ' : 'lost to ') : '';
                        var opp = t.opponent_name ? oppPrefix + escapeHtml(t.opponent_name) : '—';
                        var surface = t.surface || '—';
                        return '<div class="py-3">'
                            + '<div class="flex items-center justify-between mb-1">'
                            + '<span class="font-semibold text-gray-800 text-sm">' + escapeHtml(t.tournament_name || '—') + '</span>'
                            + getResultBadge(t.result, t.won_title)
                            + '</div>'
                            + '<div class="text-xs text-gray-500 mb-0.5">' + (t.year || '—') + ' · ' + escapeHtml(t.tournament_level || '—') + ' · ' + getSurfaceDot(surface) + escapeHtml(surface) + '</div>'
                            + '<div class="text-xs text-gray-400">' + opp + '</div>'
                            + '</div>';
                    }).join('');
                }
            }
        }

        function applyTournamentFilters() {
            var filtered = allTournaments.filter(function (t) {
                var resultMatch = true;
                if (resultFilter === 'title') resultMatch = t.won_title;
                else if (resultFilter === 'final') resultMatch = t.result === 'F' && !t.won_title;
                else if (resultFilter === 'semi') resultMatch = t.result === 'SF';
                else if (resultFilter === 'quarter') resultMatch = t.result === 'QF';
                var surfaceMatch = surfaceFilter === 'all' || t.surface === surfaceFilter;
                return resultMatch && surfaceMatch;
            });
            var visible = filtered.slice(0, tournamentPage * TOURNAMENTS_PER_PAGE);
            renderTournamentTable(visible);
            var loadMoreEl = document.getElementById('tournaments-load-more');
            if (loadMoreEl) {
                if (visible.length < filtered.length) {
                    loadMoreEl.classList.remove('hidden');
                } else {
                    loadMoreEl.classList.add('hidden');
                }
            }
        }

        function setupTournamentFilters() {
            document.querySelectorAll('[data-tournament-result-filter]').forEach(function (btn) {
                btn.addEventListener('click', function () {
                    document.querySelectorAll('[data-tournament-result-filter]').forEach(function (b) { b.classList.remove('active'); });
                    btn.classList.add('active');
                    resultFilter = btn.getAttribute('data-tournament-result-filter');
                    tournamentPage = 1;
                    applyTournamentFilters();
                });
            });
            document.querySelectorAll('[data-tournament-surface-filter]').forEach(function (btn) {
                btn.addEventListener('click', function () {
                    document.querySelectorAll('[data-tournament-surface-filter]').forEach(function (b) { b.classList.remove('active'); });
                    btn.classList.add('active');
                    surfaceFilter = btn.getAttribute('data-tournament-surface-filter');
                    tournamentPage = 1;
                    applyTournamentFilters();
                });
            });
            var loadMoreBtn = document.getElementById('tournaments-load-more-btn');
            if (loadMoreBtn) {
                loadMoreBtn.addEventListener('click', function () {
                    tournamentPage += 1;
                    applyTournamentFilters();
                });
            }
        }
    }

    function loadOverviewKpis() {
        setSectionLoading('kpi-loading', ['overview-kpis'], true);
        fetch('/api/players/' + encodeURIComponent(playerId) + '/profile-overview')
            .then(function (res) {
                if (!res.ok) throw new Error('Failed to load player overview');
                return res.json();
            })
            .then(function (data) {
                var player = data.player || {};
                var overall = data.overall_elo || {};

                setText('player-name', player.name);
                var meta = [];
                if (player.nationality) meta.push(player.nationality);
                if (player.age_years != null) meta.push(player.age_years + 'y');
                if (player.hand) meta.push(player.hand + '-handed');
                setText('player-meta', meta.join(' • ') || 'No bio data available');

                setText('kpi-elo', overall.rating);
                setText('kpi-rank', overall.rank ? '#' + overall.rank : null);
                setText('kpi-peak', overall.career_peak);
            })
            .catch(function (err) {
                console.error(err);
                setText('player-meta', 'Failed to load player overview');
            })
            .finally(function () {
                setSectionLoading('kpi-loading', ['overview-kpis'], false);
            });
    }

    function loadSurfaceElo() {
        setSectionLoading('surface-loading', ['surface-cards'], true);
        fetch('/api/players/' + encodeURIComponent(playerId) + '/surface-elo')
            .then(function (res) {
                if (!res.ok) throw new Error('Failed to load surface Elo');
                return res.json();
            })
            .then(function (data) {
                renderSurfaceCards(data.surface_elo || []);
                setText('surface-updated-at', data.generated_at ? 'Updated: ' + data.generated_at.replace('T', ' ').slice(0, 19) + ' UTC' : null);
            })
            .catch(function (err) {
                console.error(err);
                renderSurfaceCards([]);
                setText('surface-updated-at', 'Failed to load');
            })
            .finally(function () {
                setSectionLoading('surface-loading', ['surface-cards'], false);
            });
    }

    function loadRecords() {
        setSectionLoading('records-loading', ['record-cards'], true);
        fetch('/api/players/' + encodeURIComponent(playerId) + '/records')
            .then(function (res) {
                if (!res.ok) throw new Error('Failed to load records');
                return res.json();
            })
            .then(function (data) {
                var records = data.records || {};
                renderRecord('record-career', records.career);
                renderRecord('record-52w', records.last_52_weeks);
                renderRecord('record-last10', records.last_10);
                var last10 = records.last_10 || null;
                setText('kpi-last10', last10 ? (last10.wins + '-' + last10.losses) : null);
            })
            .catch(function (err) {
                console.error(err);
                renderRecord('record-career', null);
                renderRecord('record-52w', null);
                renderRecord('record-last10', null);
                setText('kpi-last10', null);
            })
            .finally(function () {
                setSectionLoading('records-loading', ['record-cards'], false);
            });
    }

    var rangeButtons = root.querySelectorAll('.history-range-btn');
    for (var i = 0; i < rangeButtons.length; i++) {
        rangeButtons[i].addEventListener('click', function (ev) {
            selectedRange = ev.currentTarget.getAttribute('data-range') || '1y';
            for (var j = 0; j < rangeButtons.length; j++) {
                rangeButtons[j].classList.remove('active');
            }
            ev.currentTarget.classList.add('active');
            loadHistory();
        });
    }

    var surfaceSelect = document.getElementById('history-surface');
    if (surfaceSelect) {
        surfaceSelect.addEventListener('change', function () {
            selectedSurface = surfaceSelect.value || 'overall';
            loadHistory();
        });
    }

    var matchesResult = document.getElementById('matches-result');
    var matchesSurface = document.getElementById('matches-surface');
    var matchesMore = document.getElementById('player-matches-more');
    if (matchesResult) {
        matchesResult.addEventListener('change', function () {
            matchesState.page = 1;
            loadMatches(false);
        });
    }
    if (matchesSurface) {
        matchesSurface.addEventListener('change', function () {
            matchesState.page = 1;
            loadMatches(false);
        });
    }
    if (matchesMore) {
        matchesMore.addEventListener('click', function () {
            loadMatches(true);
        });
    }

    loadOverviewKpis();
    loadSurfaceElo();
    loadRecords();
    loadHistory();
    loadMatches(false);
    loadTournaments();
    if (typeof lucide !== 'undefined') lucide.createIcons();
})();

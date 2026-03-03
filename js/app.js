(function () {
  "use strict";

  let allMatches = [];
  let filteredMatches = [];

  const dom = {};

  function init() {
    dom.lastUpdated = document.getElementById("last-updated");
    dom.statTotal = document.getElementById("stat-total");
    dom.statToday = document.getElementById("stat-today");
    dom.statPodcasts = document.getElementById("stat-podcasts");
    dom.statTrending = document.getElementById("stat-trending");
    dom.searchInput = document.getElementById("filter-search");
    dom.keywordSelect = document.getElementById("filter-keyword");
    dom.podcastSelect = document.getElementById("filter-podcast");
    dom.categorySelect = document.getElementById("filter-category");
    dom.dateSelect = document.getElementById("filter-date");
    dom.resetBtn = document.getElementById("filter-reset");
    dom.resultsGrid = document.getElementById("results-grid");
    dom.loading = document.getElementById("loading");

    dom.searchInput.addEventListener("input", applyFilters);
    dom.keywordSelect.addEventListener("change", applyFilters);
    dom.podcastSelect.addEventListener("change", applyFilters);
    dom.categorySelect.addEventListener("change", applyFilters);
    dom.dateSelect.addEventListener("change", applyFilters);
    dom.resetBtn.addEventListener("click", resetFilters);

    loadData();
  }

  async function loadData() {
    try {
      const resp = await fetch("data/matches.json");
      if (!resp.ok) throw new Error("Failed to load matches.json");
      const data = await resp.json();

      allMatches = data.matches || [];
      if (data.generated_at) {
        const d = new Date(data.generated_at);
        dom.lastUpdated.textContent =
          "Last scan: " + d.toLocaleString();
      }

      populateDropdowns();
      applyFilters();
    } catch (err) {
      console.error(err);
      dom.resultsGrid.innerHTML =
        '<div class="no-results">Failed to load data. Ensure data/matches.json exists.</div>';
    } finally {
      dom.loading.style.display = "none";
    }
  }

  function populateDropdowns() {
    const keywords = [...new Set(allMatches.map((m) => m.keyword))].sort();
    const podcasts = [...new Set(allMatches.map((m) => m.podcast_name))].sort();
    const categories = [
      ...new Set(allMatches.map((m) => m.category).filter(Boolean)),
    ].sort();

    fillSelect(dom.keywordSelect, keywords, "All Keywords");
    fillSelect(dom.podcastSelect, podcasts, "All Podcasts");
    fillSelect(dom.categorySelect, categories, "All Categories");
  }

  function fillSelect(el, items, placeholder) {
    el.innerHTML = '<option value="">' + placeholder + "</option>";
    items.forEach((item) => {
      const opt = document.createElement("option");
      opt.value = item;
      opt.textContent = item;
      el.appendChild(opt);
    });
  }

  function applyFilters() {
    const search = dom.searchInput.value.toLowerCase().trim();
    const keyword = dom.keywordSelect.value;
    const podcast = dom.podcastSelect.value;
    const category = dom.categorySelect.value;
    const days = parseInt(dom.dateSelect.value, 10) || 30;
    const cutoff = new Date(Date.now() - days * 86400000);

    filteredMatches = allMatches.filter((m) => {
      if (keyword && m.keyword !== keyword) return false;
      if (podcast && m.podcast_name !== podcast) return false;
      if (category && m.category !== category) return false;
      if (m.published && new Date(m.published) < cutoff) return false;
      if (
        search &&
        !m.keyword.toLowerCase().includes(search) &&
        !m.podcast_name.toLowerCase().includes(search) &&
        !m.episode_title.toLowerCase().includes(search) &&
        !m.snippet.toLowerCase().includes(search)
      ) {
        return false;
      }
      return true;
    });

    updateStats();
    renderCards();
  }

  function updateStats() {
    dom.statTotal.textContent = filteredMatches.length;

    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const todayCount = filteredMatches.filter(
      (m) => m.published && new Date(m.published) >= today
    ).length;
    dom.statToday.textContent = todayCount;

    const podcastSet = new Set(filteredMatches.map((m) => m.podcast_name));
    dom.statPodcasts.textContent = podcastSet.size;

    // Trending: most frequent keyword in the last 7 days
    const weekAgo = new Date(Date.now() - 7 * 86400000);
    const recentKws = filteredMatches
      .filter((m) => m.published && new Date(m.published) >= weekAgo)
      .map((m) => m.keyword);
    const freq = {};
    recentKws.forEach((k) => (freq[k] = (freq[k] || 0) + 1));
    const trending = Object.entries(freq).sort((a, b) => b[1] - a[1]);
    dom.statTrending.textContent = trending.length > 0 ? trending[0][0] : "—";
  }

  function renderCards() {
    if (filteredMatches.length === 0) {
      dom.resultsGrid.innerHTML =
        '<div class="no-results">No matches found. Adjust your filters or wait for the next scan.</div>';
      return;
    }

    const frag = document.createDocumentFragment();
    filteredMatches.forEach((m) => frag.appendChild(createCard(m)));
    dom.resultsGrid.innerHTML = "";
    dom.resultsGrid.appendChild(frag);
  }

  function createCard(match) {
    const card = document.createElement("div");
    card.className = "match-card";

    const pubDate = match.published
      ? new Date(match.published).toLocaleDateString("en-US", {
          month: "short",
          day: "numeric",
          year: "numeric",
        })
      : "";

    const tsMin = Math.floor(match.timestamp_seconds / 60);
    const tsSec = Math.floor(match.timestamp_seconds % 60);
    const tsLabel =
      String(tsMin).padStart(2, "0") + ":" + String(tsSec).padStart(2, "0");

    const snippet = highlightKeyword(escapeHtml(match.snippet), match.keyword);

    const catAttr = match.category || "";

    card.innerHTML =
      '<div class="card-meta">' +
      '<span class="card-podcast">' +
      escapeHtml(match.podcast_name) +
      "</span>" +
      '<span class="card-date">' +
      pubDate +
      "</span>" +
      "</div>" +
      '<div class="card-title"><a href="' +
      escapeHtml(match.youtube_url) +
      '" target="_blank" rel="noopener">' +
      escapeHtml(match.episode_title) +
      "</a></div>" +
      '<div class="card-badges">' +
      '<span class="badge badge-category" data-cat="' +
      escapeHtml(catAttr) +
      '">' +
      escapeHtml(catAttr || "General") +
      "</span>" +
      '<span class="badge badge-keyword">' +
      escapeHtml(match.keyword) +
      "</span>" +
      "</div>" +
      '<div class="card-snippet">' +
      snippet +
      "</div>" +
      '<a class="card-link" href="' +
      escapeHtml(match.youtube_url) +
      '" target="_blank" rel="noopener">Listen at ' +
      tsLabel +
      " &#8594;</a>";

    return card;
  }

  function highlightKeyword(text, keyword) {
    if (!keyword) return text;
    const escaped = keyword.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp("(" + escaped + ")", "gi");
    return text.replace(re, '<mark class="kw-highlight">$1</mark>');
  }

  function escapeHtml(str) {
    if (!str) return "";
    return str
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function resetFilters() {
    dom.searchInput.value = "";
    dom.keywordSelect.value = "";
    dom.podcastSelect.value = "";
    dom.categorySelect.value = "";
    dom.dateSelect.value = "30";
    applyFilters();
  }

  document.addEventListener("DOMContentLoaded", init);
})();

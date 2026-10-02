"use strict";

const BUSINESS_ID = "sama_cod_pilot";
const ASK_ENDPOINT = "/api/v1/analyst/ask";
const OPPORTUNITIES_ENDPOINT = "/api/v1/analyst/opportunities";

const form = document.querySelector("#ask-form");
const questionInput = document.querySelector("#question");
const askButton = document.querySelector("#ask-button");
const buttonLabel = document.querySelector(".button-label");
const buttonLoading = document.querySelector(".button-loading");
const statusPanel = document.querySelector("#status-panel");
const errorPanel = document.querySelector("#error-panel");
const errorMessage = document.querySelector("#error-message");
const retryButton = document.querySelector("#retry-button");
const answerPanel = document.querySelector("#answer-panel");
let lastQuestion = "";
let lastOpportunityId = null;
let lastOpportunityTitle = "";

function clearNode(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function textElement(tag, text, className = "") {
  const element = document.createElement(tag);
  element.textContent = text;
  if (className) element.className = className;
  return element;
}

function setLoading(loading) {
  askButton.disabled = loading;
  questionInput.disabled = loading;
  buttonLabel.hidden = loading;
  buttonLoading.hidden = !loading;
  statusPanel.hidden = !loading;
  statusPanel.textContent = loading ? "Reviewing validated aggregate evidence..." : "";
}

function renderList(target, values, ordered = false) {
  clearNode(target);
  for (const value of values) {
    target.appendChild(textElement("li", value));
  }
  if (!values.length) {
    target.appendChild(textElement("li", ordered ? "No steps supplied." : "None supplied."));
  }
}

function appendDetailSection(target, heading, values, ordered = false) {
  const section = document.createElement("section");
  section.className = "opportunity-detail-section";
  section.appendChild(textElement("h4", heading));
  if (Array.isArray(values)) {
    const list = document.createElement(ordered ? "ol" : "ul");
    renderList(list, values, ordered);
    section.appendChild(list);
  } else {
    section.appendChild(textElement("p", values || "None supplied."));
  }
  target.appendChild(section);
}

function renderEvidenceDetails(opportunity) {
  const details = document.createElement("details");
  details.className = "opportunity-evidence";
  details.appendChild(textElement("summary", "Evidence references"));
  const groups = [
    ["Supporting", opportunity.supporting_evidence_refs],
    ["Counter", opportunity.counter_evidence_refs],
    ["Blocking", opportunity.blocking_evidence_refs],
  ];
  for (const [label, refs] of groups) {
    const group = document.createElement("div");
    group.className = "opportunity-evidence-group";
    group.appendChild(textElement("strong", label));
    if (refs.length) {
      for (const ref of refs) group.appendChild(textElement("code", ref));
    } else {
      group.appendChild(textElement("span", "None", "muted-copy"));
    }
    details.appendChild(group);
  }
  return details;
}

function formatImpact(opportunity) {
  if (opportunity.impact_proxy_value === null) return "";
  const name = opportunity.impact_proxy_name.replaceAll("_", " ");
  return `${name}: ${opportunity.impact_proxy_value.toLocaleString()} ${opportunity.impact_proxy_unit}`;
}

function opportunityAction(label, question, opportunity) {
  const button = textElement("button", label, "opportunity-action");
  button.type = "button";
  button.dataset.opportunityAction = question;
  button.addEventListener("click", () => {
    questionInput.value = question;
    askPulse(question, opportunity.opportunity_id, opportunity.title);
  });
  return button;
}

function renderOpportunityCard(opportunity) {
  const card = document.createElement("article");
  card.className = "opportunity-card";
  card.dataset.opportunityOrder = String(opportunity.opportunity_order);

  const header = document.createElement("div");
  header.className = "opportunity-card-header";
  const badges = document.createElement("div");
  badges.className = "badges opportunity-badges";
  badges.appendChild(textElement("span", opportunity.priority, `badge priority-${opportunity.priority.toLowerCase()}`));
  badges.appendChild(textElement("span", `${opportunity.confidence} confidence`, "badge"));
  badges.appendChild(textElement("span", "Untested", "badge badge-caution"));
  header.appendChild(badges);
  header.appendChild(textElement("span", `#${opportunity.opportunity_order}`, "opportunity-order"));
  card.appendChild(header);
  card.appendChild(textElement("h3", opportunity.title));
  card.appendChild(textElement("p", `${opportunity.scope_type.toLowerCase()} · ${opportunity.scope_name}`, "opportunity-scope"));
  card.appendChild(textElement("p", opportunity.observation_summary, "opportunity-observation"));
  const impact = formatImpact(opportunity);
  if (impact) card.appendChild(textElement("p", impact, "impact-proxy"));

  const actions = document.createElement("div");
  actions.className = "opportunity-actions";
  actions.appendChild(opportunityAction("Why this?", "Why is this an opportunity?", opportunity));
  actions.appendChild(opportunityAction("What would refute it?", "What would refute this hypothesis?", opportunity));
  actions.appendChild(opportunityAction("How should I investigate?", "What should I investigate for this opportunity?", opportunity));
  card.appendChild(actions);

  const details = document.createElement("details");
  details.className = "opportunity-details";
  details.appendChild(textElement("summary", "View opportunity details"));
  const content = document.createElement("div");
  content.className = "opportunity-details-content";
  appendDetailSection(content, "Hypothesis to test", opportunity.hypothesis_to_test);
  appendDetailSection(content, "Investigation steps", opportunity.investigation_steps, true);
  appendDetailSection(content, "What would confirm it", opportunity.confirmation_criteria);
  appendDetailSection(content, "What would refute it", opportunity.refutation_criteria);
  appendDetailSection(content, "Decision this evidence could unlock", opportunity.decision_unlocked);
  appendDetailSection(content, "Missing evidence", opportunity.missing_evidence);
  appendDetailSection(content, "Limitation", opportunity.limitation);
  content.appendChild(renderEvidenceDetails(opportunity));
  details.appendChild(content);
  card.appendChild(details);
  return card;
}

function renderOpportunities(payload) {
  const opportunities = payload.opportunities;
  document.querySelector("#opportunities-heading").textContent = `${payload.count} opportunities need attention`;
  document.querySelector("#opportunities-subtitle").textContent = `Active as of ${payload.as_of_date}. Ordered by the deterministic opportunity engine.`;
  const counts = document.querySelector("#opportunity-counts");
  clearNode(counts);
  for (const priority of ["HIGH", "MEDIUM", "LOW"]) {
    const count = opportunities.filter((item) => item.priority === priority).length;
    counts.appendChild(textElement("span", `${priority} ${count}`, `badge priority-${priority.toLowerCase()}`));
  }
  const cards = document.querySelector("#opportunity-cards");
  clearNode(cards);
  for (const opportunity of opportunities) {
    cards.appendChild(renderOpportunityCard(opportunity));
  }
  document.querySelector("#opportunities-status").textContent = opportunities.length ? "" : "No active opportunities.";
}

async function loadOpportunities() {
  const status = document.querySelector("#opportunities-status");
  try {
    const response = await fetch(`${OPPORTUNITIES_ENDPOINT}?business_id=${encodeURIComponent(BUSINESS_ID)}`);
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error?.message || "Opportunities are temporarily unavailable.");
    renderOpportunities(payload);
  } catch (error) {
    status.textContent = error instanceof Error ? error.message : "Opportunities are temporarily unavailable.";
  }
}

function renderAnswer(payload) {
  const answer = payload.answer;
  document.querySelector("#answer-summary").textContent = answer.answer_summary;
  document.querySelector("#confidence-badge").textContent = `${answer.confidence} confidence`;
  document.querySelector("#partial-badge").hidden = !answer.cannot_answer_fully;
  const opportunityContext = document.querySelector("#answer-opportunity-context");
  opportunityContext.hidden = !lastOpportunityId;
  opportunityContext.textContent = lastOpportunityId ? `Answer about: ${lastOpportunityTitle}` : "";

  const findings = document.querySelector("#findings-list");
  clearNode(findings);
  for (const finding of answer.findings) {
    const card = document.createElement("article");
    card.className = "finding-card";
    card.appendChild(textElement("p", finding.statement, "finding-statement"));
    const meta = document.createElement("div");
    meta.className = "finding-meta";
    meta.appendChild(textElement("span", `${finding.confidence} confidence`, "mini-badge"));
    for (const evidenceRef of finding.evidence_refs) {
      meta.appendChild(textElement("code", evidenceRef));
    }
    card.appendChild(meta);
    findings.appendChild(card);
  }

  renderList(document.querySelector("#investigation-list"), answer.investigation_steps, true);
  renderList(document.querySelector("#limitations-list"), answer.limitations);

  const evidence = document.querySelector("#evidence-list");
  clearNode(evidence);
  for (const evidenceRef of answer.evidence_refs) {
    evidence.appendChild(textElement("code", evidenceRef));
  }

  const safetyCard = document.querySelector("#safety-card");
  renderList(document.querySelector("#safety-list"), answer.safety_notes);
  safetyCard.hidden = answer.safety_notes.length === 0;

  const metadata = document.querySelector("#metadata-list");
  clearNode(metadata);
  const entries = [
    ["Provider", payload.meta.provider],
    ["Model", payload.meta.model],
    ["Evidence items", String(payload.meta.evidence_count)],
    ["Provider calls", String(payload.meta.provider_call_count)],
    ["Repair attempted", payload.meta.repair_attempted ? "Yes" : "No"],
    ["Fallback used", payload.meta.deterministic_fallback_used ? "Yes" : "No"],
    ["Fallback intent", payload.meta.fallback_intent || "None"],
    ["Answer source", payload.meta.answer_source],
    ["Latency", `${payload.meta.latency_ms.toFixed(2)} ms`],
    ["Request ID", payload.request_id],
  ];
  for (const [label, value] of entries) {
    metadata.appendChild(textElement("dt", label));
    metadata.appendChild(textElement("dd", value));
  }

  errorPanel.hidden = true;
  answerPanel.hidden = false;
  answerPanel.scrollIntoView({ behavior: "smooth", block: "start" });
}

function showError(message) {
  errorMessage.textContent = message;
  errorPanel.hidden = false;
  answerPanel.hidden = true;
}

async function askPulse(question, opportunityId = null, opportunityTitle = "") {
  const trimmed = question.trim();
  if (!trimmed) {
    showError("Enter an aggregate business question.");
    questionInput.focus();
    return;
  }
  lastQuestion = trimmed;
  lastOpportunityId = opportunityId;
  lastOpportunityTitle = opportunityTitle;
  errorPanel.hidden = true;
  setLoading(true);
  try {
    const requestBody = { business_id: BUSINESS_ID, question: trimmed };
    if (opportunityId) requestBody.opportunity_id = opportunityId;
    const response = await fetch(ASK_ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(requestBody),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error?.message || "Pulse could not answer safely.");
    }
    renderAnswer(payload);
  } catch (error) {
    showError(error instanceof Error ? error.message : "Pulse is temporarily unavailable.");
  } finally {
    setLoading(false);
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  askPulse(questionInput.value);
});

for (const suggestion of document.querySelectorAll("[data-question]")) {
  suggestion.addEventListener("click", () => {
    questionInput.value = suggestion.dataset.question;
    askPulse(questionInput.value);
  });
}

retryButton.addEventListener("click", () => {
  if (lastQuestion) askPulse(lastQuestion, lastOpportunityId, lastOpportunityTitle);
});

loadOpportunities();

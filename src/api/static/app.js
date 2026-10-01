"use strict";

const BUSINESS_ID = "sama_cod_pilot";
const ASK_ENDPOINT = "/api/v1/analyst/ask";

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

function renderAnswer(payload) {
  const answer = payload.answer;
  document.querySelector("#answer-summary").textContent = answer.answer_summary;
  document.querySelector("#confidence-badge").textContent = `${answer.confidence} confidence`;
  document.querySelector("#partial-badge").hidden = !answer.cannot_answer_fully;

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

async function askPulse(question) {
  const trimmed = question.trim();
  if (!trimmed) {
    showError("Enter an aggregate business question.");
    questionInput.focus();
    return;
  }
  lastQuestion = trimmed;
  errorPanel.hidden = true;
  setLoading(true);
  try {
    const response = await fetch(ASK_ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ business_id: BUSINESS_ID, question: trimmed }),
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
  if (lastQuestion) askPulse(lastQuestion);
});

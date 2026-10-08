"use strict";

const BUSINESS_ID = "sama_cod_pilot";
const ASK_ENDPOINT = "/api/v1/analyst/ask";
const OPPORTUNITIES_ENDPOINT = "/api/v1/analyst/opportunities";
const INVESTIGATIONS_ENDPOINT = "/api/v1/analyst/investigations";
const DECISIONS_ENDPOINT = "/api/v1/analyst/decisions";
const SEQUENCING_ENDPOINT = "/api/v1/analyst/sequencing";

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
let lastInvestigationTaskId = null;
let lastInvestigationTaskTitle = "";
let lastDecisionId = null;
let lastDecisionTitle = "";
let lastSequencingRequirementId = null;
let lastSequencingRequirementTitle = "";
let lastSequencingTaskId = null;
let lastSequencingTaskTitle = "";

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

function readinessLabel(readiness) {
  const labels = {
    READY_NOW: "READY NOW",
    PARTIALLY_READY: "PARTIALLY READY",
    BLOCKED_MISSING_EVIDENCE: "MISSING EVIDENCE",
    BLOCKED_BOUNDARY: "BLOCKED BY BOUNDARY",
  };
  return labels[readiness] || readiness.replaceAll("_", " ");
}

function decisionReadinessLabel(readiness) {
  const labels = {
    READY_FOR_HUMAN_REVIEW: "READY FOR HUMAN REVIEW",
    NEEDS_MORE_EVIDENCE: "NEEDS MORE EVIDENCE",
    BLOCKED_BY_BOUNDARY: "BLOCKED BY BOUNDARY",
  };
  return labels[readiness] || readiness.replaceAll("_", " ");
}

function decisionClassLabel(decisionClass) {
  const labels = {
    INVESTIGATION_DIRECTION: "Investigation direction",
    BUSINESS_CHANGE_CONSIDERATION: "Business change consideration",
    MEASUREMENT_GOVERNANCE: "Measurement governance",
    FINANCIAL_DECISION: "Financial decision",
  };
  return labels[decisionClass] || decisionClass.replaceAll("_", " ").toLowerCase();
}

function investigationAction(label, question, task) {
  const button = textElement("button", label, "opportunity-action investigation-action");
  button.type = "button";
  button.dataset.investigationTaskId = task.task_id;
  button.addEventListener("click", () => {
    questionInput.value = question;
    askPulse(question, null, "", task.task_id, task.title);
  });
  return button;
}

function decisionAction(label, question, assessment) {
  const button = textElement("button", label, "opportunity-action decision-action");
  button.type = "button";
  button.dataset.decisionId = assessment.decision_id;
  button.addEventListener("click", () => {
    questionInput.value = question;
    askPulse(
      question,
      null,
      "",
      null,
      "",
      assessment.decision_id,
      assessment.decision_question,
    );
  });
  return button;
}

function renderInvestigationTask(task) {
  const details = document.createElement("details");
  details.className = "investigation-task";
  const summary = document.createElement("summary");
  const summaryCopy = document.createElement("span");
  summaryCopy.className = "task-summary-copy";
  summaryCopy.appendChild(textElement("span", `${task.task_order}. ${task.title}`, "task-title"));
  summaryCopy.appendChild(textElement("span", task.objective, "task-objective"));
  summary.appendChild(summaryCopy);
  summary.appendChild(textElement(
    "span",
    readinessLabel(task.readiness),
    `badge readiness-${task.readiness.toLowerCase()}`,
  ));
  details.appendChild(summary);
  const body = document.createElement("div");
  body.className = "investigation-task-body";

  const actions = document.createElement("div");
  actions.className = "opportunity-actions task-actions";
  if (task.readiness === "READY_NOW") {
    actions.appendChild(investigationAction("Why ready?", "Why can I investigate this now?", task));
    actions.appendChild(investigationAction("What should this produce?", "What should this produce?", task));
    actions.appendChild(investigationAction("When is it complete?", "When is this task complete?", task));
  } else {
    actions.appendChild(investigationAction("Why blocked?", "Why is this blocked?", task));
    actions.appendChild(investigationAction("What data is missing?", "What data is missing?", task));
  }
  body.appendChild(actions);
  appendDetailSection(body, "Available evidence", task.available_evidence_refs);
  appendDetailSection(body, "Missing requirements", task.missing_requirement_ids);
  appendDetailSection(body, "Expected output", task.expected_output);
  appendDetailSection(body, "Completion criteria", task.completion_criteria);
  appendDetailSection(body, "Would strengthen", task.strengthens_criteria);
  appendDetailSection(body, "Would weaken", task.weakens_criteria);
  appendDetailSection(body, "Limitation", task.limitation);
  details.appendChild(body);
  return details;
}

function renderPlanSummary(plan) {
  const section = document.createElement("section");
  section.className = "plan-summary";
  if (!plan) {
    section.appendChild(textElement("h4", "Investigation plan"));
    section.appendChild(textElement("p", "Investigation plan unavailable.", "muted-copy"));
    return section;
  }
  const heading = document.createElement("div");
  heading.className = "plan-summary-heading";
  heading.appendChild(textElement("h4", "Investigation plan"));
  heading.appendChild(textElement("span", plan.status, `badge plan-${plan.status.toLowerCase()}`));
  section.appendChild(heading);
  const ready = plan.tasks.filter((task) => task.readiness === "READY_NOW").length;
  const partial = plan.tasks.filter((task) => task.readiness === "PARTIALLY_READY").length;
  const blocked = plan.tasks.filter((task) => task.readiness.startsWith("BLOCKED_")).length;
  section.appendChild(textElement(
    "p",
    `Ready now ${ready} · Partial ${partial} · Blocked ${blocked}`,
    "plan-task-counts",
  ));
  const recommended = plan.tasks.find(
    (task) => task.task_id === plan.recommended_start_task_id,
  );
  section.appendChild(textElement(
    "p",
    `Recommended start: ${recommended ? recommended.title : "None"}`,
    "plan-recommended",
  ));
  return section;
}

function renderPlanDetails(plan) {
  const section = document.createElement("section");
  section.className = "opportunity-detail-section investigation-plan-details";
  section.appendChild(textElement("h4", "Investigation plan"));
  if (!plan) {
    section.appendChild(textElement("p", "Investigation plan unavailable.", "muted-copy"));
    return section;
  }
  for (const task of plan.tasks) section.appendChild(renderInvestigationTask(task));
  appendDetailSection(section, "Decision this plan could unlock", plan.decision_unlocked);
  appendDetailSection(section, "Plan limitation", plan.limitation);
  return section;
}

function renderOpportunityDecisionSummary(assessments) {
  const section = document.createElement("section");
  section.className = "opportunity-decision-summary";
  section.appendChild(textElement("h4", "Decision readiness"));
  if (!assessments.length) {
    section.appendChild(textElement("p", "No active decision assessments.", "muted-copy"));
    return section;
  }
  const ready = assessments.filter(
    (item) => item.readiness === "READY_FOR_HUMAN_REVIEW",
  ).length;
  const needs = assessments.filter(
    (item) => item.readiness === "NEEDS_MORE_EVIDENCE",
  ).length;
  const blocked = assessments.filter(
    (item) => item.readiness === "BLOCKED_BY_BOUNDARY",
  ).length;
  section.appendChild(textElement(
    "p",
    `${ready} ready for human review · ${needs} need more evidence · ${blocked} blocked by boundary`,
    "decision-summary-counts",
  ));
  return section;
}

function renderDecisionCard(assessment, opportunity, plan) {
  const card = document.createElement("article");
  card.className = "decision-card";
  card.dataset.decisionOrder = String(assessment.decision_order);

  const heading = document.createElement("div");
  heading.className = "decision-card-heading";
  const badges = document.createElement("div");
  badges.className = "badges decision-badges";
  badges.appendChild(textElement(
    "span",
    decisionReadinessLabel(assessment.readiness),
    `badge decision-readiness-${assessment.readiness.toLowerCase()}`,
  ));
  badges.appendChild(textElement(
    "span",
    decisionClassLabel(assessment.decision_class),
    "badge",
  ));
  heading.appendChild(badges);
  heading.appendChild(textElement("span", `#${assessment.decision_order}`, "opportunity-order"));
  card.appendChild(heading);
  card.appendChild(textElement("h3", assessment.decision_question));
  card.appendChild(textElement(
    "p",
    opportunity
      ? `From ${opportunity.title} · ${opportunity.priority} priority / ${opportunity.confidence} confidence`
      : "Originating opportunity is unavailable in the current product payload.",
    "decision-origin",
  ));
  card.appendChild(textElement("p", assessment.rationale_summary, "decision-rationale"));
  card.appendChild(textElement("p", assessment.decision_boundary, "decision-boundary"));
  const humanReviewValue = assessment.human_review_required ? "Yes" : "No";
  const autonomousActionValue = assessment.autonomous_action_allowed ? "Yes" : "No";
  card.appendChild(textElement(
    "p",
    `Human review required: ${humanReviewValue} · Autonomous action allowed: ${autonomousActionValue}`,
    "decision-agency",
  ));

  const actions = document.createElement("div");
  actions.className = "opportunity-actions decision-actions";
  if (assessment.readiness === "READY_FOR_HUMAN_REVIEW") {
    actions.appendChild(decisionAction("Why ready?", "Why is this ready for human review?", assessment));
    actions.appendChild(decisionAction("What is the boundary?", "What is the decision boundary?", assessment));
    actions.appendChild(decisionAction("Does this recommend an action?", "Does ready mean Pulse recommends the change?", assessment));
  } else if (assessment.readiness === "BLOCKED_BY_BOUNDARY") {
    actions.appendChild(decisionAction("Why blocked?", "Why is this blocked by a boundary?", assessment));
    actions.appendChild(decisionAction("What is the boundary?", "What is the decision boundary?", assessment));
  } else {
    actions.appendChild(decisionAction("Why not ready?", "Why is this not ready?", assessment));
    actions.appendChild(decisionAction("What evidence is missing?", "What evidence is missing?", assessment));
    actions.appendChild(decisionAction("What could raise readiness?", "What could raise readiness?", assessment));
  }
  card.appendChild(actions);

  const details = document.createElement("details");
  details.className = "decision-details";
  details.appendChild(textElement("summary", "View decision details"));
  const content = document.createElement("div");
  content.className = "decision-details-content";
  appendDetailSection(content, "Why", assessment.rationale_summary);
  appendDetailSection(content, "Reason codes", assessment.readiness_reason_codes);
  appendDetailSection(content, "Supporting evidence", assessment.supporting_evidence_refs);
  appendDetailSection(content, "Counter evidence", assessment.counter_evidence_refs);
  appendDetailSection(content, "Blocking evidence", assessment.blocking_evidence_refs);
  appendDetailSection(content, "Required evidence", assessment.required_requirement_ids);
  appendDetailSection(content, "Unresolved evidence", assessment.unresolved_requirement_ids);
  appendDetailSection(content, "Tasks that could raise readiness", assessment.next_evidence_task_ids);
  appendDetailSection(content, "Decision boundary", assessment.decision_boundary);
  appendDetailSection(content, "Human review required", assessment.human_review_required ? "Yes" : "No");
  appendDetailSection(content, "Autonomous action allowed", assessment.autonomous_action_allowed ? "Yes" : "No");
  appendDetailSection(content, "Limitation", assessment.limitation);
  const technical = document.createElement("details");
  technical.className = "decision-technical";
  technical.appendChild(textElement("summary", "Technical associations"));
  appendDetailSection(technical, "Decision ID", assessment.decision_id);
  appendDetailSection(technical, "Opportunity ID", assessment.originating_opportunity_id);
  appendDetailSection(technical, "Investigation plan ID", assessment.investigation_plan_id);
  if (!plan) appendDetailSection(technical, "Plan state", "Plan unavailable in current product payload.");
  content.appendChild(technical);
  details.appendChild(content);
  card.appendChild(details);
  return card;
}

function renderOpportunityCard(opportunity, plan, assessments) {
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
  card.appendChild(renderPlanSummary(plan));
  card.appendChild(renderOpportunityDecisionSummary(assessments));

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
  content.appendChild(renderPlanDetails(plan));
  content.appendChild(renderEvidenceDetails(opportunity));
  details.appendChild(content);
  card.appendChild(details);
  return card;
}

function renderInvestigationPortfolio(portfolio) {
  const panel = document.querySelector("#investigation-portfolio");
  panel.hidden = false;
  const counts = document.querySelector("#investigation-counts");
  clearNode(counts);
  counts.appendChild(textElement("span", `Ready now ${portfolio.ready_task_count}`, "badge readiness-ready_now"));
  counts.appendChild(textElement("span", `Partial ${portfolio.partial_task_count}`, "badge readiness-partially_ready"));
  counts.appendChild(textElement("span", `Blocked ${portfolio.blocked_task_count}`, "badge readiness-blocked_missing_evidence"));
  counts.appendChild(textElement("span", `Evidence gaps ${portfolio.evidence_gaps.length}`, "badge"));

  const allTasks = portfolio.plans.flatMap((plan) => plan.tasks);
  const recommended = allTasks.find(
    (task) => task.task_id === portfolio.recommended_start_task_id,
  );
  document.querySelector("#recommended-investigation").textContent =
    `Investigation plan starting point: ${recommended ? recommended.title : "None currently available"}`;

  const gaps = document.querySelector("#evidence-gap-list");
  clearNode(gaps);
  if (!portfolio.evidence_gaps.length) {
    gaps.appendChild(textElement("p", "No evidence gaps in the current validated context.", "muted-copy"));
  }
  for (const gap of portfolio.evidence_gaps) {
    const item = document.createElement("article");
    item.className = "evidence-gap-item";
    const investigationCount = gap.affected_opportunity_ids.length;
    const taskCount = gap.affected_task_ids.length;
    item.appendChild(textElement("h4", gap.description));
    item.appendChild(textElement(
      "p",
      `Used by ${investigationCount} ${investigationCount === 1 ? "investigation" : "investigations"} and ${taskCount} investigation ${taskCount === 1 ? "task" : "tasks"}.`,
    ));
    item.appendChild(textElement(
      "p",
      `Priorities: ${gap.opportunity_priorities.join(", ")}. Not available in current validated context.`,
      "muted-copy",
    ));
    gaps.appendChild(item);
  }
}

function renderDecisionPortfolio(decisions, opportunities, investigations) {
  const section = document.querySelector("#decision-readiness-section");
  section.hidden = false;
  const counts = document.querySelector("#decision-counts");
  clearNode(counts);
  counts.appendChild(textElement(
    "span",
    `Ready for human review ${decisions.ready_for_human_review_count}`,
    "badge decision-readiness-ready_for_human_review",
  ));
  counts.appendChild(textElement(
    "span",
    `Need more evidence ${decisions.needs_more_evidence_count}`,
    "badge decision-readiness-needs_more_evidence",
  ));
  counts.appendChild(textElement(
    "span",
    `Blocked by boundary ${decisions.blocked_by_boundary_count}`,
    "badge decision-readiness-blocked_by_boundary",
  ));

  const first = decisions.assessments.find(
    (item) => item.decision_id === decisions.first_reviewable_decision_id,
  );
  document.querySelector("#first-reviewable-decision").textContent =
    `First reviewable decision: ${first ? first.decision_question : "None currently available"}`;

  const opportunitiesById = new Map(
    opportunities.opportunities.map((item) => [item.opportunity_id, item]),
  );
  const plansById = new Map(
    investigations.plans.map((item) => [item.plan_id, item]),
  );
  const cards = document.querySelector("#decision-cards");
  clearNode(cards);
  for (const assessment of decisions.assessments) {
    cards.appendChild(renderDecisionCard(
      assessment,
      opportunitiesById.get(assessment.originating_opportunity_id),
      plansById.get(assessment.investigation_plan_id),
    ));
  }
  document.querySelector("#decisions-status").textContent = decisions.assessments.length
    ? ""
    : "No active decision-readiness assessments.";
}

function sequencingStateLabel(state) {
  const labels = {
    READY_TASK_AVAILABLE: "Startable investigation available",
    EVIDENCE_GAP_FIRST: "Evidence gap first",
    BOUNDARY_ONLY: "Boundary-limited",
    NO_OPEN_READINESS_GAPS: "No open readiness gaps",
  };
  return labels[state] || "Unknown sequencing state";
}

function sequencingRequirementAction(label, question, item) {
  const button = textElement("button", label, "opportunity-action sequencing-action");
  button.type = "button";
  button.dataset.sequencingRequirementId = item.requirement_id;
  button.addEventListener("click", () => {
    questionInput.value = question;
    askPulse(
      question, null, "", null, "", null, "",
      item.requirement_id, item.requirement_name,
    );
  });
  return button;
}

function sequencingTaskAction(label, question, item) {
  const button = textElement("button", label, "opportunity-action sequencing-action");
  button.type = "button";
  button.dataset.sequencingTaskId = item.task_id;
  button.addEventListener("click", () => {
    questionInput.value = question;
    askPulse(
      question, null, "", null, "", null, "", null, "",
      item.task_id, item.task_title,
    );
  });
  return button;
}

function renderLeverageCard(item, decisionsById, sequenceById) {
  const card = document.createElement("article");
  card.className = "leverage-card";
  card.dataset.requirementId = item.requirement_id;
  const badges = document.createElement("div");
  badges.className = "badges leverage-badges";
  badges.appendChild(textElement(
    "span",
    `${item.highest_opportunity_priority} opportunity priority`,
    `badge priority-${item.highest_opportunity_priority.toLowerCase()}`,
  ));
  badges.appendChild(textElement(
    "span",
    item.requirement_status.replaceAll("_", " "),
    "badge badge-caution",
  ));
  if (item.shared_across_decisions) {
    badges.appendChild(textElement("span", "SHARED ACROSS DECISIONS", "badge"));
  }
  card.appendChild(badges);
  card.appendChild(textElement("h4", item.requirement_name));
  card.appendChild(textElement(
    "p",
    `${item.affected_decision_count} non-ready decisions · ${item.affected_opportunity_count} opportunities`,
    "leverage-counts",
  ));

  const actions = document.createElement("div");
  actions.className = "opportunity-actions sequencing-actions";
  actions.appendChild(sequencingRequirementAction(
    "Why this focus?", "Why this evidence focus?", item,
  ));
  actions.appendChild(sequencingRequirementAction(
    "Which decisions depend on this?", "Which decisions depend on this evidence?", item,
  ));
  actions.appendChild(sequencingRequirementAction(
    "Does this guarantee readiness?", "Will this evidence guarantee readiness?", item,
  ));
  card.appendChild(actions);

  const details = document.createElement("details");
  details.className = "sequencing-details";
  details.appendChild(textElement("summary", "View evidence focus details"));
  const content = document.createElement("div");
  content.className = "sequencing-details-content";
  appendDetailSection(content, "Description", item.description);
  appendDetailSection(
    content,
    "Affected decision questions",
    item.affected_decision_ids.map(
      (id) => decisionsById.get(id)?.decision_question || id,
    ),
  );
  appendDetailSection(
    content,
    "Related investigation tasks",
    item.related_task_ids.map((id) => sequenceById.get(id)?.task_title || id),
  );
  appendDetailSection(content, "Existing evidence gap", item.existing_gap_id || "None");
  appendDetailSection(content, "Limitation", item.limitation);
  const technical = document.createElement("details");
  technical.className = "sequencing-technical";
  technical.appendChild(textElement("summary", "Technical associations"));
  appendDetailSection(technical, "Requirement ID", item.requirement_id);
  appendDetailSection(technical, "Affected decision IDs", item.affected_decision_ids);
  appendDetailSection(technical, "Affected opportunity IDs", item.affected_opportunity_ids);
  appendDetailSection(technical, "Related task IDs", item.related_task_ids);
  content.appendChild(technical);
  details.appendChild(content);
  card.appendChild(details);
  return card;
}

function renderSequenceItem(item, decisionsById, leverageById, plansById, opportunitiesById) {
  const card = document.createElement("article");
  card.className = "sequence-item";
  card.dataset.sequenceOrder = String(item.sequence_order);
  card.dataset.taskId = item.task_id;
  const header = document.createElement("div");
  header.className = "sequence-item-heading";
  const badges = document.createElement("div");
  badges.className = "badges sequence-badges";
  badges.appendChild(textElement(
    "span",
    item.can_begin_now ? "CAN BEGIN NOW" : "BLOCKED BY CURRENT EVIDENCE",
    item.can_begin_now ? "badge readiness-ready_now" : "badge readiness-blocked_missing_evidence",
  ));
  badges.appendChild(textElement(
    "span",
    readinessLabel(item.task_readiness),
    `badge readiness-${item.task_readiness.toLowerCase()}`,
  ));
  header.appendChild(badges);
  header.appendChild(textElement("span", `#${item.sequence_order}`, "opportunity-order"));
  card.appendChild(header);
  card.appendChild(textElement("h4", item.task_title));
  card.appendChild(textElement(
    "p",
    `Can begin now: ${item.can_begin_now ? "Yes" : "No"} · ${item.affected_decision_count} affected decisions · ${item.opportunity_priority} priority`,
    "sequence-facts",
  ));
  card.appendChild(textElement("p", item.sequencing_reason, "sequence-reason"));

  const actions = document.createElement("div");
  actions.className = "opportunity-actions sequencing-actions";
  actions.appendChild(sequencingTaskAction(
    "Why is this ordered here?", "Why is this ordered here?", item,
  ));
  actions.appendChild(sequencingTaskAction(
    "Can this begin now?", "Can this investigation begin now?", item,
  ));
  actions.appendChild(sequencingTaskAction(
    "What could this help clarify?", "What could this investigation help clarify?", item,
  ));
  card.appendChild(actions);

  const details = document.createElement("details");
  details.className = "sequencing-details";
  details.appendChild(textElement("summary", "View sequence details"));
  const content = document.createElement("div");
  content.className = "sequencing-details-content";
  appendDetailSection(
    content,
    "Affected decision questions",
    item.affected_decision_ids.map(
      (id) => decisionsById.get(id)?.decision_question || id,
    ),
  );
  appendDetailSection(
    content,
    "Addressed requirements",
    item.addressed_requirement_ids.map(
      (id) => leverageById.get(id)?.requirement_name || id,
    ),
  );
  const plan = plansById.get(item.investigation_plan_id);
  const opportunity = opportunitiesById.get(item.opportunity_id);
  appendDetailSection(content, "Investigation plan", plan ? plan.decision_unlocked : "Unavailable in current product payload.");
  appendDetailSection(content, "Associated opportunity", opportunity ? opportunity.title : "Unavailable in current product payload.");
  appendDetailSection(content, "Limitation", item.limitation);
  const technical = document.createElement("details");
  technical.className = "sequencing-technical";
  technical.appendChild(textElement("summary", "Technical associations"));
  appendDetailSection(technical, "Task ID", item.task_id);
  appendDetailSection(technical, "Investigation plan ID", item.investigation_plan_id);
  appendDetailSection(technical, "Opportunity ID", item.opportunity_id);
  content.appendChild(technical);
  details.appendChild(content);
  card.appendChild(details);
  return card;
}

function renderSequencingPortfolio(sequencing, decisions, opportunities, investigations) {
  const section = document.querySelector("#sequencing-section");
  section.hidden = false;
  const leverageById = new Map(
    sequencing.evidence_leverage_items.map((item) => [item.requirement_id, item]),
  );
  const sequenceById = new Map(
    sequencing.sequence_items.map((item) => [item.task_id, item]),
  );
  const decisionsById = new Map(
    decisions.assessments.map((item) => [item.decision_id, item]),
  );
  const opportunitiesById = new Map(
    opportunities.opportunities.map((item) => [item.opportunity_id, item]),
  );
  const plansById = new Map(
    investigations.plans.map((item) => [item.plan_id, item]),
  );
  const top = sequencing.top_evidence_focus_requirement_id
    ? leverageById.get(sequencing.top_evidence_focus_requirement_id)
    : null;
  const recommended = sequencing.recommended_next_task_id
    ? sequenceById.get(sequencing.recommended_next_task_id)
    : null;

  document.querySelector("#sequencing-state").textContent = sequencingStateLabel(sequencing.state);
  document.querySelector("#sequencing-top-focus").textContent = top
    ? top.requirement_name
    : "None currently available";
  document.querySelector("#sequencing-relevance").textContent = top
    ? `${top.affected_decision_count} non-ready decisions · ${top.affected_opportunity_count} opportunities`
    : "No current evidence focus";
  document.querySelector("#sequencing-startable-count").textContent = String(
    sequencing.startable_task_count,
  );
  document.querySelector("#sequencing-next-task").textContent = sequencing.recommended_next_task_id
    ? (recommended?.task_title || "Unavailable in current product payload")
    : "None currently available";

  const noStartable = document.querySelector("#sequencing-no-startable");
  const showNoStartable = sequencing.state === "EVIDENCE_GAP_FIRST"
    && sequencing.recommended_next_task_id === null;
  noStartable.hidden = !showNoStartable;
  document.querySelector("#sequencing-no-startable-focus").textContent = top
    ? `Top evidence focus: ${top.requirement_name}`
    : "Top evidence focus unavailable.";
  const noStartableAsk = document.querySelector("#sequencing-no-startable-ask");
  noStartableAsk.hidden = !showNoStartable || !top;
  noStartableAsk.onclick = top ? () => {
    const question = "Why is there no next startable investigation?";
    questionInput.value = question;
    askPulse(
      question, null, "", null, "", null, "",
      top.requirement_id, top.requirement_name,
    );
  } : null;

  const leverageCards = document.querySelector("#leverage-cards");
  clearNode(leverageCards);
  for (const item of sequencing.evidence_leverage_items) {
    leverageCards.appendChild(renderLeverageCard(item, decisionsById, sequenceById));
  }
  const sequenceList = document.querySelector("#sequence-list");
  clearNode(sequenceList);
  for (const item of sequencing.sequence_items) {
    sequenceList.appendChild(renderSequenceItem(
      item, decisionsById, leverageById, plansById, opportunitiesById,
    ));
  }
  document.querySelector("#sequencing-status").textContent =
    sequencing.evidence_leverage_items.length || sequencing.sequence_items.length
      ? ""
      : "No open evidence-leverage or readiness-raising sequence items.";
  document.querySelector("#sequencing-limitation").textContent = sequencing.limitation;
}

function renderOpportunities(payload, portfolio, decisions) {
  const opportunities = payload.opportunities;
  const plansByOpportunityId = new Map(
    portfolio.plans.map((plan) => [plan.opportunity_id, plan]),
  );
  const decisionsByOpportunityId = new Map();
  for (const assessment of decisions.assessments) {
    const values = decisionsByOpportunityId.get(assessment.originating_opportunity_id) || [];
    values.push(assessment);
    decisionsByOpportunityId.set(assessment.originating_opportunity_id, values);
  }
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
    cards.appendChild(renderOpportunityCard(
      opportunity,
      plansByOpportunityId.get(opportunity.opportunity_id),
      decisionsByOpportunityId.get(opportunity.opportunity_id) || [],
    ));
  }
  document.querySelector("#opportunities-status").textContent = opportunities.length ? "" : "No active opportunities.";
}

async function loadProductData() {
  const status = document.querySelector("#opportunities-status");
  try {
    const [
      opportunitiesResponse,
      investigationsResponse,
      decisionsResponse,
      sequencingResponse,
    ] = await Promise.all([
      fetch(`${OPPORTUNITIES_ENDPOINT}?business_id=${encodeURIComponent(BUSINESS_ID)}`),
      fetch(`${INVESTIGATIONS_ENDPOINT}?business_id=${encodeURIComponent(BUSINESS_ID)}`),
      fetch(`${DECISIONS_ENDPOINT}?business_id=${encodeURIComponent(BUSINESS_ID)}`),
      fetch(`${SEQUENCING_ENDPOINT}?business_id=${encodeURIComponent(BUSINESS_ID)}`),
    ]);
    const [opportunities, portfolio, decisions, sequencing] = await Promise.all([
      opportunitiesResponse.json(),
      investigationsResponse.json(),
      decisionsResponse.json(),
      sequencingResponse.json(),
    ]);
    if (!opportunitiesResponse.ok) {
      throw new Error(opportunities.error?.message || "Opportunities are temporarily unavailable.");
    }
    if (!investigationsResponse.ok) {
      throw new Error(portfolio.error?.message || "Investigation plans are temporarily unavailable.");
    }
    if (!decisionsResponse.ok) {
      throw new Error(decisions.error?.message || "Decision readiness is temporarily unavailable.");
    }
    if (!sequencingResponse.ok) {
      throw new Error(sequencing.error?.message || "Evidence sequencing is temporarily unavailable.");
    }
    renderInvestigationPortfolio(portfolio);
    renderDecisionPortfolio(decisions, opportunities, portfolio);
    renderSequencingPortfolio(sequencing, decisions, opportunities, portfolio);
    renderOpportunities(opportunities, portfolio, decisions);
  } catch (error) {
    status.textContent = error instanceof Error ? error.message : "Investigation plans are temporarily unavailable.";
  }
}

function renderAnswer(payload) {
  const answer = payload.answer;
  document.querySelector("#answer-summary").textContent = answer.answer_summary;
  document.querySelector("#confidence-badge").textContent = `${answer.confidence} confidence`;
  document.querySelector("#partial-badge").hidden = !answer.cannot_answer_fully;
  const opportunityContext = document.querySelector("#answer-opportunity-context");
  opportunityContext.hidden = !lastOpportunityId && !lastInvestigationTaskId
    && !lastDecisionId && !lastSequencingRequirementId && !lastSequencingTaskId;
  opportunityContext.textContent = lastSequencingTaskId
    ? `Answer about investigation sequence: ${lastSequencingTaskTitle}`
    : (lastSequencingRequirementId
      ? `Answer about evidence focus: ${lastSequencingRequirementTitle}`
      : (lastDecisionId
        ? `Answer about decision: ${lastDecisionTitle}`
        : (lastInvestigationTaskId
          ? `Answer about investigation: ${lastInvestigationTaskTitle}`
          : (lastOpportunityId ? `Answer about: ${lastOpportunityTitle}` : ""))));

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

async function askPulse(
  question,
  opportunityId = null,
  opportunityTitle = "",
  investigationTaskId = null,
  investigationTaskTitle = "",
  decisionId = null,
  decisionTitle = "",
  sequencingRequirementId = null,
  sequencingRequirementTitle = "",
  sequencingTaskId = null,
  sequencingTaskTitle = "",
) {
  const trimmed = question.trim();
  if (!trimmed) {
    showError("Enter an aggregate business question.");
    questionInput.focus();
    return;
  }
  lastQuestion = trimmed;
  lastOpportunityId = opportunityId;
  lastOpportunityTitle = opportunityTitle;
  lastInvestigationTaskId = investigationTaskId;
  lastInvestigationTaskTitle = investigationTaskTitle;
  lastDecisionId = decisionId;
  lastDecisionTitle = decisionTitle;
  lastSequencingRequirementId = sequencingRequirementId;
  lastSequencingRequirementTitle = sequencingRequirementTitle;
  lastSequencingTaskId = sequencingTaskId;
  lastSequencingTaskTitle = sequencingTaskTitle;
  errorPanel.hidden = true;
  setLoading(true);
  try {
    const requestBody = { business_id: BUSINESS_ID, question: trimmed };
    if (sequencingRequirementId) {
      requestBody.sequencing_requirement_id = sequencingRequirementId;
    } else if (sequencingTaskId) requestBody.sequencing_task_id = sequencingTaskId;
    else if (decisionId) requestBody.decision_id = decisionId;
    else if (investigationTaskId) requestBody.investigation_task_id = investigationTaskId;
    else if (opportunityId) requestBody.opportunity_id = opportunityId;
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
  if (lastQuestion) {
    askPulse(
      lastQuestion,
      lastOpportunityId,
      lastOpportunityTitle,
      lastInvestigationTaskId,
      lastInvestigationTaskTitle,
      lastDecisionId,
      lastDecisionTitle,
      lastSequencingRequirementId,
      lastSequencingRequirementTitle,
      lastSequencingTaskId,
      lastSequencingTaskTitle,
    );
  }
});

loadProductData();

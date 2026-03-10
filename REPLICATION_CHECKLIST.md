# Replication Project Checklist

Use this checklist to verify that the project meets all grading criteria. Check off each item when complete.

---

## Deliverables & Documentation

[ ] **4/4** Generate a single LaTeX document that:
  - Briefly describes the nature of the replication project
  - Contains all tables and charts produced by the code
  - Gives a high-level overview of how the replication went (successes and challenges)
  - Explains the data sources used
  - Does **not** contain code snippets—only tables, charts, and high-level discussion

[ ] **4/4** Provide at least one Jupyter notebook that:
  - Gives a brief tour of the cleaned data and some of the analysis in the code
  - Acts as a “tour of the code” for the reader
  - May include code snippets
  - Is similar in style to course HW guides (e.g., HW Guide: Replicate Fama-French 1993)

---

## Replication Quality

- [ ] **20/20** Replicate the series, tables, and/or figures listed for the assigned project:
  - Choose a reasonable tolerance
  - Construct unit tests so that numbers match the paper’s within this tolerance

- [ ] **20/20** Reproduce the series, tables, and/or figures with **updated numbers**:
  - Recalculate using data up to the present (or most recently available)
  - I.e., not only replicate the paper’s sample period but also update with new data

- [ ] **20/20** Provide **your own** summary statistics table(s) AND chart(s):
  - Give sufficient understanding of the underlying data
  - Tables and figures must be typeset in LaTeX with captions
  - Captions must describe and motivate each table/figure (what to learn or take away)
  - Decide how many tables/figures are sufficient (usually at least one of each)

---

## Code & Data Organization

- [ ] **4/4** Tidy data step is separate:
  - A separate file or set of files exists whose **only** purpose is to clean data and put it in a “tidy” format
  - Analysis is kept in separate files from the data-cleaning code

- [ ] **4/4** All statistics in LaTeX tables are **automatically generated** from the code (no hand-typed numbers)

- [ ] **4/4** Project is **automated end-to-end** using PyDoit (e.g., `doit` runs the full pipeline)

---

## Testing

- [ ] **4/4** The repository uses **unit tests** to ensure the code works correctly, and the tests are well motivated

- [ ] **4/4** Each unit test has a **purpose**; there are no unnecessary or repetitive tests

---

## Template & Repo Setup

- [ ] **4/4** The project uses the **cookiecutter chartbook** template and was scaffolded with  
  `cruft create https://github.com/backofficedev/cookiecutter_chartbook`

---

## Repository Hygiene

- [ ] **4/4** GitHub repo and Git history are **free of copyrighted material** (e.g., raw data is not in the repo)

- [ ] **4/4** GitHub repo and Git history are **free of secrets** (e.g., API keys, passwords)

- [ ] **4/4** Project uses a **`.env` file** and **reasonable defaults in `settings.py`** for configuration (e.g., data directory, API keys, START_DATE, END_DATE). The required `.env` format is described in an **`.env.example`** file.

- [ ] **4/4** The project does **not** contain a trace of `.env` (it must not be in the Git commit history)

- [ ] **4/4** The project contains a **`requirements.txt`** (or equivalent) that describes the packages needed to run the code

- [ ] **4/4** (repeated) Repo and history are free of secrets (e.g., API keys)

---

## Collaboration & Workflow

[ ] **4/4** **Each group member** has made commits to the Git repo

[ ] **4/4** **Each group member** has made and merged a GitHub pull request

---

## Code Quality

- [ ] **4/4** **Each Python file** has a docstring at the top describing what the file does

- [ ] **4/4** **Each Python function** has a reasonably descriptive name and, when appropriate, a function docstring (code should be reasonably clean; no need to overdo it)

---

## Individual Contribution

- [ ] **50/50** I individually accomplished the tasks assigned to me by the group and contributed a substantial part of the code to the project, as evidenced by the commit history.

---

*When an item is done, change `- [ ]` to `- [x]` for that line.*

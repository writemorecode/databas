# Domain docs

## Before exploring

This repo uses a single context:

- Read root `GLOSSARY.md`.
- Read ADRs under `docs/adr/` relevant to the area being explored.

If these files or directories do not exist, proceed silently.
The domain-modeling skill creates them lazily when terminology or
decisions are resolved.

## Vocabulary

Use glossary terms in issue titles, proposals, hypotheses, and test names.
Avoid synonyms the glossary explicitly excludes.

If a needed concept is missing, reconsider whether it belongs to the
project's vocabulary or note the gap for domain-modeling.

## ADR conflicts

Explicitly flag proposals that contradict existing ADRs, identifying the
ADR and explaining why the decision may be worth reopening.

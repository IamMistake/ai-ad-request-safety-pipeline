# Phase 7: Moderation TF-IDF And OpenAI Gate

## Goal

Replace moderation behavior with a TF-IDF/cosine-similarity gate that calls
OpenAI only when needed or when selected for audit sampling.

Moderation consumes fraud-clean requests and emits either approved ad injection
events or blocked unsafe events.

## Routing

Input:

```text
requests.clean
```

Outputs:

```text
clean/safe -> ad.injection
unsafe     -> requests.fraud
```

## Reference Dataset

Use a manually maintained local dataset.

Recommended file:

```text
pipeline_consumers/data/unsafe_reference_set.json
```

Dataset contents:

```text
400 manually curated unsafe/fraud/hate/etc. reference prompts
100-200 manually curated bad indicator words/phrases
```

Recommended JSON shape:

```json
{
  "version": "unsafe-reference-v1",
  "categories": [
    "fraud",
    "hate",
    "violence",
    "self_harm",
    "sexual",
    "illicit",
    "malware",
    "spam"
  ],
  "bad_terms": [],
  "reference_prompts": []
}
```

## TF-IDF Flow

```text
load reference dataset
normalize reference prompts and bad terms
fit TF-IDF vectorizer
precompute reference vectors
normalize incoming prompt
vectorize incoming prompt with same vectorizer
calculate cosine similarity
```

Use one global similarity threshold in v1.

Recommended initial value:

```text
0.30
```

## OpenAI Call Policy

Call OpenAI in two cases:

```text
similarity >= threshold
similarity < threshold and selected by 2% random audit sample
```

Do not call OpenAI when:

```text
similarity < threshold and not selected for audit
```

When OpenAI is called, OpenAI gives the final moderation verdict.

Routing:

```text
OpenAI clean   -> ad.injection
OpenAI flagged -> requests.fraud
```

## OpenAI Error Policy

OpenAI errors allow the request in v1.

Behavior:

```text
OpenAI error -> ad.injection
```

The event must be clearly marked:

```text
moderation.method = openai_error_allow
moderation.openai_called = true
moderation.openai_error = true
moderation.reasons includes openai_error_allowed
```

OpenAI error-allowed requests must be excluded from Spark RFC model training.

## Moderation Context

Approved or blocked events should include moderation context such as:

```text
verdict
method
similarity_score
similarity_threshold
reference_version
openai_called
openai_reason
openai_error
reasons
```

## Definition Of Done

```text
safe low-similarity prompts usually skip OpenAI
high-similarity prompts call OpenAI
2% low-similarity audit sample calls OpenAI
OpenAI flagged prompts go to requests.fraud
OpenAI errors are allowed but clearly marked
openai_error_allow events are excluded from Spark training
```

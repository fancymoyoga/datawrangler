"""Readmission model features and their plain-English names, shared by training and scoring."""

LABEL = "readmitted_30d"
NUMERIC = ["age_at_admission", "length_of_stay_days", "prior_admissions_12m"]
BINARY = ["has_heart_failure", "has_copd", "has_diabetes", "has_ckd", "missed_followup", "reached_by_outreach"]
CATEGORICAL = ["sex", "insurance_type", "discharge_destination"]
FEATURES = NUMERIC + BINARY + CATEGORICAL

# (name when the value is high or 1, name when it is low or 0)
LABELS = {
    "age_at_admission": ("Older age", "Younger age"),
    "length_of_stay_days": ("Longer hospital stay", "Shorter hospital stay"),
    "prior_admissions_12m": ("More admissions in the past 12 months", "Fewer admissions in the past 12 months"),
    "has_heart_failure": ("Heart failure", "No heart failure"),
    "has_copd": ("COPD", "No COPD"),
    "has_diabetes": ("Diabetes", "No diabetes"),
    "has_ckd": ("Chronic kidney disease", "No chronic kidney disease"),
    "missed_followup": ("Missed follow-up appointment", "Completed follow-up appointment"),
    "reached_by_outreach": ("Reached by outreach within 4 days", "Not reached by outreach within 4 days"),
}
CATEGORY_PREFIX = {"sex": "Sex: ", "insurance_type": "Insurance: ", "discharge_destination": "Discharged to "}


def split_name(transformed):
    """Split a ColumnTransformer output name like 'cat__sex_F' into (kind, feature, category)."""
    kind, rest = transformed.split("__", 1)
    if kind == "cat":
        feature = next(c for c in CATEGORICAL if rest.startswith(c + "_"))
        return kind, feature, rest[len(feature) + 1:]
    return kind, rest, None


def plain_name(transformed):
    """Plain-English name of a model input, as it reads when the value is high, 1 or present."""
    kind, feature, category = split_name(transformed)
    if kind == "cat":
        return CATEGORY_PREFIX[feature] + category
    return LABELS[feature][0]

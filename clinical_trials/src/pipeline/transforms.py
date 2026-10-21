"""DataFrame transforms for the clinical trials pipeline.

Plain functions (DataFrame in, DataFrame out) so they can be unit-tested with local
PySpark. bronze.py / silver.py / gold.py wrap them in Spark Declarative Pipeline datasets.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------


def bronze_from_binary(files: DataFrame) -> DataFrame:
    """Project binaryFile rows (path, modificationTime, length, content) to bronze columns.

    Invalid UTF-8 bytes become U+FFFD instead of failing, so no file is ever dropped.
    """
    return files.select(
        F.expr("make_valid_utf8(CAST(content AS STRING))").alias("raw_json"),
        F.sha2(F.col("content"), 256).alias("file_sha256"),
        F.col("path").alias("source_file"),
        F.col("length").alias("file_bytes"),
        F.col("modificationTime").alias("file_modified_at"),
        F.current_timestamp().alias("bronze_loaded_at"),
    )


# ---------------------------------------------------------------------------
# Silver: parsing
# ---------------------------------------------------------------------------

KNOWN_OVERALL_STATUSES = (
    "ACTIVE_NOT_RECRUITING", "APPROVED_FOR_MARKETING", "AVAILABLE", "COMPLETED",
    "ENROLLING_BY_INVITATION", "NO_LONGER_AVAILABLE", "NOT_YET_RECRUITING", "RECRUITING",
    "SUSPENDED", "TEMPORARILY_NOT_AVAILABLE", "TERMINATED", "UNKNOWN", "WITHDRAWN", "WITHHELD",
)


def _null_is_failure(rules):
    """Wrap each rule so a NULL result counts as a failure."""
    return {name: f"coalesce(({expr}), false)" for name, expr in rules.items()}


# Critical rules: failing rows are dropped from validated and land in quarantine.
CRITICAL_RULES = _null_is_failure({
    "valid_json": "is_valid_json",
    "http_status_200": "http_status = 200",
    "nct_id_present": "nct_id IS NOT NULL",
    "nct_id_format": "nct_id RLIKE '^NCT[0-9]{8}$'",
    "nct_id_matches_request": "nct_id = requested_nct_id",
    "title_present": "trim(brief_title) <> ''",
    "overall_status_present": "overall_status IS NOT NULL",
    "last_update_date_present": "last_update_posted_date IS NOT NULL",
})

# Warning rules: rows are kept; failures are tracked in DQ metrics and dq_warnings.
WARNING_RULES = _null_is_failure({
    "known_overall_status": "overall_status IN ("
    + ", ".join(f"'{s}'" for s in KNOWN_OVERALL_STATUSES) + ")",
    "enrollment_non_negative": "enrollment_count >= 0",
    "completion_after_start": "completion_date >= start_date",
    "primary_completion_after_start": "primary_completion_date >= start_date",
    "start_date_parses": "start_date IS NOT NULL",
    "min_age_not_above_max_age": "minimum_age_years <= maximum_age_years",
})


def _failed(rules) -> Column:
    """Array of the names of the rules a row fails."""
    return F.array_compact(F.array(*[F.when(~F.expr(e), F.lit(n)) for n, e in rules.items()]))


def _get(path: str, type_: str) -> Column:
    """Read one field of the API response; a missing or mistyped field gives NULL."""
    return F.try_variant_get(F.col("payload"), f"$.{path}", type_)


def _date(path: str) -> Column:
    """ClinicalTrials.gov dates are yyyy-MM-dd or yyyy-MM (month precision → first of month)."""
    raw = _get(path, "string")
    return F.coalesce(
        F.try_to_timestamp(raw, F.lit("yyyy-MM-dd")),
        F.try_to_timestamp(raw, F.lit("yyyy-MM")),
    ).cast("date")


_UNIT_TO_YEARS = {"year": 1.0, "month": 12.0, "week": 52.1775, "day": 365.25,
                  "hour": 8766.0, "minute": 525960.0}


def _age_in_years(raw: Column) -> Column:
    """'18 Years' → 18.0, '6 Months' → 0.5; 'N/A' or unknown units → NULL."""
    number = F.regexp_extract(raw, r"^\s*(\d+(?:\.\d+)?)", 1).try_cast("double")
    unit = F.lower(F.regexp_extract(raw, r"([A-Za-z]+)\s*$", 1))
    divisor = None
    for name, value in _UNIT_TO_YEARS.items():
        cond = unit.startswith(name)
        divisor = F.when(cond, value) if divisor is None else divisor.when(cond, value)
    return F.round(number / divisor, 3)


PROTO = "protocolSection"
INTERVENTIONS_TYPE = "array<struct<type:string,name:string>>"
OUTCOMES_TYPE = "array<struct<measure:string,timeFrame:string>>"


def parse_studies(bronze: DataFrame) -> DataFrame:
    """Parse bronze envelopes into typed study columns plus failed_rules / dq_warnings.

    Every field is read with try_parse_json / try_variant_get, so schema drift yields
    NULLs (and rule failures) rather than errors. Used by both validated and quarantine.
    """
    env = bronze.withColumn("_env", F.try_parse_json("raw_json"))
    meta = env.select(
        "*",
        F.col("_env").isNotNull().alias("is_valid_json"),
        # Fall back to the landing folder name ({NCT_ID}/...) when the file isn't valid JSON.
        F.coalesce(
            F.try_variant_get("_env", "$.requested_nct_id", "string"),
            F.nullif(F.regexp_extract("source_file", r"([^/\\]+)[/\\][^/\\]+$", 1), F.lit("")),
        ).alias("requested_nct_id"),
        F.try_variant_get("_env", "$.source_url", "string").alias("source_url"),
        F.try_variant_get("_env", "$.http_status", "int").alias("http_status"),
        F.try_variant_get("_env", "$.ingested_at", "string").try_cast("timestamp").alias("ingested_at"),
        F.try_variant_get("_env", "$.ingest_id", "string").alias("ingest_id"),
        F.try_variant_get("_env", "$.payload", "variant").alias("payload"),
    ).drop("_env")

    interventions = _get(f"{PROTO}.armsInterventionsModule.interventions", INTERVENTIONS_TYPE)
    primary_outcomes = _get(f"{PROTO}.outcomesModule.primaryOutcomes", OUTCOMES_TYPE)
    secondary_outcomes = _get(f"{PROTO}.outcomesModule.secondaryOutcomes", OUTCOMES_TYPE)
    minimum_age = _get(f"{PROTO}.eligibilityModule.minimumAge", "string")
    maximum_age = _get(f"{PROTO}.eligibilityModule.maximumAge", "string")
    party = f"{PROTO}.sponsorCollaboratorsModule.responsibleParty"

    parsed = meta.select(
        _get(f"{PROTO}.identificationModule.nctId", "string").alias("nct_id"),
        "requested_nct_id",
        "http_status",
        "is_valid_json",
        _get(f"{PROTO}.identificationModule.briefTitle", "string").alias("brief_title"),
        _get(f"{PROTO}.identificationModule.officialTitle", "string").alias("official_title"),
        _get(f"{PROTO}.identificationModule.acronym", "string").alias("acronym"),
        _get(f"{PROTO}.identificationModule.organization.fullName", "string").alias("organization_name"),
        _get(f"{PROTO}.identificationModule.organization.class", "string").alias("organization_class"),
        _get(f"{PROTO}.statusModule.overallStatus", "string").alias("overall_status"),
        _date(f"{PROTO}.statusModule.startDateStruct.date").alias("start_date"),
        _date(f"{PROTO}.statusModule.primaryCompletionDateStruct.date").alias("primary_completion_date"),
        _date(f"{PROTO}.statusModule.completionDateStruct.date").alias("completion_date"),
        _date(f"{PROTO}.statusModule.studyFirstPostDateStruct.date").alias("study_first_posted_date"),
        _date(f"{PROTO}.statusModule.lastUpdatePostDateStruct.date").alias("last_update_posted_date"),
        _get(f"{PROTO}.sponsorCollaboratorsModule.leadSponsor.name", "string").alias("lead_sponsor_name"),
        _get(f"{PROTO}.sponsorCollaboratorsModule.leadSponsor.class", "string").alias("lead_sponsor_class"),
        _get(f"{party}.type", "string").alias("responsible_party_type"),
        _get(f"{party}.investigatorFullName", "string").alias("responsible_party_investigator_name"),
        _get(f"{party}.investigatorTitle", "string").alias("responsible_party_investigator_title"),
        _get(f"{party}.investigatorAffiliation", "string").alias("responsible_party_investigator_affiliation"),
        _get(f"{PROTO}.designModule.studyType", "string").alias("study_type"),
        _get(f"{PROTO}.designModule.phases", "array<string>").alias("phases"),
        _get(f"{PROTO}.designModule.enrollmentInfo.count", "int").alias("enrollment_count"),
        _get(f"{PROTO}.designModule.enrollmentInfo.type", "string").alias("enrollment_type"),
        _get(f"{PROTO}.conditionsModule.conditions", "array<string>").alias("conditions"),
        _get(f"{PROTO}.conditionsModule.keywords", "array<string>").alias("keywords"),
        F.transform(interventions, lambda i: i["name"]).alias("intervention_names"),
        F.array_sort(F.array_distinct(F.transform(interventions, lambda i: i["type"]))).alias("intervention_types"),
        F.transform(primary_outcomes, lambda o: o["measure"]).alias("primary_outcome_measures"),
        F.transform(secondary_outcomes, lambda o: o["measure"]).alias("secondary_outcome_measures"),
        _get(f"{PROTO}.descriptionModule.briefSummary", "string").alias("brief_summary"),
        _get(f"{PROTO}.descriptionModule.detailedDescription", "string").alias("detailed_description"),
        _get(f"{PROTO}.eligibilityModule.eligibilityCriteria", "string").alias("eligibility_criteria"),
        _get(f"{PROTO}.eligibilityModule.sex", "string").alias("sex"),
        minimum_age.alias("minimum_age"),
        _age_in_years(minimum_age).alias("minimum_age_years"),
        maximum_age.alias("maximum_age"),
        _age_in_years(maximum_age).alias("maximum_age_years"),
        _get(f"{PROTO}.eligibilityModule.healthyVolunteers", "boolean").alias("healthy_volunteers"),
        _get("hasResults", "boolean").alias("has_results"),
        F.sha2(F.to_json("payload"), 256).alias("payload_hash"),
        "ingest_id",
        "ingested_at",
        "source_url",
        "source_file",
        "file_sha256",
        "file_modified_at",
        "bronze_loaded_at",
        "raw_json",
        "payload",
    )
    return parsed.withColumns({
        "failed_rules": _failed(CRITICAL_RULES),
        "dq_warnings": _failed(WARNING_RULES),
    })


def validated(parsed: DataFrame) -> DataFrame:
    """Columns of silver_studies_validated (the critical-rule drop is applied by expectations)."""
    return parsed.drop("failed_rules", "raw_json")


def quarantine(parsed: DataFrame) -> DataFrame:
    """Records failing at least one critical rule, with a raw JSON snippet for triage."""
    return parsed.where(F.size("failed_rules") > 0).select(
        "failed_rules",
        "dq_warnings",
        "requested_nct_id",
        "nct_id",
        "http_status",
        "brief_title",
        "ingest_id",
        "ingested_at",
        "source_url",
        "source_file",
        "file_sha256",
        "bronze_loaded_at",
        F.substring("raw_json", 1, 2000).alias("raw_json_snippet"),
        F.current_timestamp().alias("quarantined_at"),
    )


def apply_critical_rules(parsed: DataFrame) -> DataFrame:
    """What expect_all_or_drop(CRITICAL_RULES) does in the pipeline; used by local tests."""
    return parsed.where(F.size("failed_rules") == 0)


# ---------------------------------------------------------------------------
# Silver: current version per study, contacts, locations
# ---------------------------------------------------------------------------


# silver_studies is an Auto CDC (SCD Type 1) target fed by silver_studies_validated: one row per
# nct_id, the newest last_update_posted_date winning, then the newest ingested_at.
CDC_KEYS = ["nct_id"]
CDC_SEQUENCE = ["last_update_posted_date", "ingested_at"]


def apply_scd1(changes: DataFrame) -> DataFrame:
    """What the SCD Type 1 Auto CDC flow keeps (newest row per key); used by local tests."""
    newest_first = Window.partitionBy(*CDC_KEYS).orderBy(
        *[F.col(c).desc_nulls_last() for c in CDC_SEQUENCE])
    return changes.withColumn("_rank", F.row_number().over(newest_first)).where("_rank = 1").drop("_rank")


PERSON_TYPE = "array<struct<name:string,role:string,phone:string,phoneExt:string,email:string,affiliation:string>>"
LOCATIONS_TYPE = (
    "array<struct<facility:string,status:string,city:string,state:string,zip:string,country:string,"
    "geoPoint:struct<lat:double,lon:double>,"
    "contacts:array<struct<name:string,role:string,phone:string,phoneExt:string,email:string>>>>"
)
CONTACTS = f"{PROTO}.contactsLocationsModule"


def _blank_to_null(c: Column) -> Column:
    return F.when(F.trim(c) != "", F.trim(c))


def _people(latest: DataFrame, source: str, path: str) -> DataFrame:
    person = F.explode(_get(path, PERSON_TYPE)).alias("p")
    return latest.select("nct_id", person).select(
        "nct_id",
        F.lit(source).alias("source"),
        "p.role", "p.name", "p.email", "p.phone", "p.phoneExt", "p.affiliation",
        F.lit(None).cast("string").alias("facility"),
    )


def contacts(latest: DataFrame) -> DataFrame:
    """One row per person per study from every place the API lists people."""
    central = _people(latest, "central_contact", f"{CONTACTS}.centralContacts")
    officials = _people(latest, "overall_official", f"{CONTACTS}.overallOfficials")
    site_contacts = (
        latest.select("nct_id", F.explode(_get(f"{CONTACTS}.locations", LOCATIONS_TYPE)).alias("l"))
        .select("nct_id", "l.facility", F.explode("l.contacts").alias("p"))
        .select(
            "nct_id",
            F.lit("location_contact").alias("source"),
            "p.role", "p.name", "p.email", "p.phone", "p.phoneExt",
            F.lit(None).cast("string").alias("affiliation"),
            "facility",
        )
    )
    responsible = latest.where(F.col("responsible_party_investigator_name").isNotNull()).select(
        "nct_id",
        F.lit("responsible_party").alias("source"),
        F.col("responsible_party_investigator_title").alias("role"),
        F.col("responsible_party_investigator_name").alias("name"),
        F.lit(None).cast("string").alias("email"),
        F.lit(None).cast("string").alias("phone"),
        F.lit(None).cast("string").alias("phoneExt"),
        F.col("responsible_party_investigator_affiliation").alias("affiliation"),
        F.lit(None).cast("string").alias("facility"),
    )

    people = (
        central.unionByName(officials).unionByName(site_contacts).unionByName(responsible)
        .select(
            "nct_id",
            "source",
            _blank_to_null(F.col("role")).alias("role"),
            _blank_to_null(F.regexp_replace("name", r"\s+", " ")).alias("name"),
            _blank_to_null(F.lower("email")).alias("email"),
            _blank_to_null(F.regexp_replace("phone", r"[^0-9+]", "")).alias("phone"),
            _blank_to_null(F.regexp_replace("phoneExt", r"[^0-9]", "")).alias("phone_ext"),
            _blank_to_null(F.col("affiliation")).alias("affiliation"),
            _blank_to_null(F.col("facility")).alias("facility"),
        )
        .withColumn("person_key", F.coalesce(F.lower("name"), "email", "phone"))
        .where(F.col("person_key").isNotNull())
    )
    return (
        people.groupBy("nct_id", "person_key")
        .agg(
            F.max("name").alias("name"),
            F.max("email").alias("email"),
            F.max("phone").alias("phone"),
            F.max("phone_ext").alias("phone_ext"),
            F.max("affiliation").alias("affiliation"),
            F.array_sort(F.collect_set("role")).alias("roles"),
            F.array_sort(F.collect_set("source")).alias("sources"),
            F.array_sort(F.collect_set("facility")).alias("facilities"),
        )
        .select(
            F.sha2(F.concat_ws("|", "nct_id", "person_key"), 256).alias("contact_id"),
            "nct_id", "name", "email", "phone", "phone_ext", "affiliation", "roles", "sources", "facilities",
        )
    )


LOCATION_RULES = _null_is_failure({
    "country_present": "trim(country) <> ''",
    "valid_coordinates": "lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180",
})


def locations(latest: DataFrame) -> DataFrame:
    """One row per study site (site contacts live in contacts, not here)."""
    sites = latest.select(
        "nct_id", F.explode(_get(f"{CONTACTS}.locations", LOCATIONS_TYPE)).alias("l")
    ).select(
        "nct_id",
        _blank_to_null(F.col("l.facility")).alias("facility"),
        _blank_to_null(F.col("l.status")).alias("status"),
        _blank_to_null(F.col("l.city")).alias("city"),
        _blank_to_null(F.col("l.state")).alias("state"),
        _blank_to_null(F.col("l.zip")).alias("zip"),
        _blank_to_null(F.col("l.country")).alias("country"),
        F.col("l.geoPoint.lat").alias("lat"),
        F.col("l.geoPoint.lon").alias("lon"),
    )
    key = F.concat_ws("|", *[F.lower(F.coalesce(F.col(c), F.lit(""))) for c in
                             ("nct_id", "facility", "city", "state", "zip", "country")])
    return (
        sites.withColumn("location_id", F.sha2(key, 256))
        .dropDuplicates(["location_id"])
        .select("location_id", *sites.columns)
    )


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

EMAIL_PATTERN = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
# 10+ digit North-American-style or international numbers. Dates (2018-05-15, 2018-05,
# 05/15/2018) never have the 3-3-4 digit grouping, and the look-arounds stop matches inside
# longer tokens such as NCT IDs or hashes.
PHONE_PATTERN = r"(?<![\w-])(?:\+?\d{1,3}[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}(?![\w-])"
MIN_DOCUMENT_CHARS = 200

# Explicit allow-list: no contacts, no responsible-party fields, no raw payload.
GOLD_COLUMNS = (
    "nct_id", "brief_title", "official_title", "acronym", "organization_name", "organization_class",
    "overall_status", "start_date", "primary_completion_date", "completion_date",
    "study_first_posted_date", "last_update_posted_date", "lead_sponsor_name", "lead_sponsor_class",
    "study_type", "phases", "enrollment_count", "enrollment_type", "conditions", "keywords",
    "intervention_names", "intervention_types", "primary_outcome_measures",
    "secondary_outcome_measures", "brief_summary", "detailed_description", "eligibility_criteria",
    "sex", "minimum_age", "minimum_age_years", "maximum_age", "maximum_age_years",
    "healthy_volunteers", "has_results", "payload_hash",
)
FREE_TEXT_COLUMNS = ("brief_title", "official_title", "brief_summary", "detailed_description",
                     "eligibility_criteria")
FREE_TEXT_ARRAY_COLUMNS = ("primary_outcome_measures", "secondary_outcome_measures")


def scrub(text: Column) -> Column:
    """Replace emails and phone-shaped numbers; dates are left intact."""
    return F.regexp_replace(
        F.regexp_replace(text, EMAIL_PATTERN, "[email removed]"), PHONE_PATTERN, "[phone removed]"
    )


def _line(label: str, value: Column) -> Column:
    """'Label: value', or NULL (skipped by concat_ws) when the value is missing."""
    return F.concat(F.lit(f"{label}: "), value)


def _joined(c: str) -> Column:
    return F.when(F.size(c) > 0, F.array_join(c, "; "))


def documents(silver_studies: DataFrame) -> DataFrame:
    """PII-free study documents for RAG: allow-listed columns + labelled document_text."""
    docs = silver_studies.select(*GOLD_COLUMNS)
    docs = docs.withColumns({c: scrub(F.col(c)) for c in FREE_TEXT_COLUMNS})
    docs = docs.withColumns({c: F.transform(c, scrub) for c in FREE_TEXT_ARRAY_COLUMNS})

    dates = F.concat_ws(
        "; ",
        _line("start", F.col("start_date").cast("string")),
        _line("primary completion", F.col("primary_completion_date").cast("string")),
        _line("completion", F.col("completion_date").cast("string")),
        _line("first posted", F.col("study_first_posted_date").cast("string")),
        _line("last updated", F.col("last_update_posted_date").cast("string")),
    )
    enrollment = F.concat_ws(
        " ", F.col("enrollment_count").cast("string"), F.lower("enrollment_type")
    )
    eligibility = F.concat_ws(
        "; ",
        _line("sex", F.col("sex")),
        _line("minimum age", F.col("minimum_age")),
        _line("maximum age", F.col("maximum_age")),
        _line("healthy volunteers", F.col("healthy_volunteers").cast("string")),
    )
    text = F.concat_ws(
        "\n",
        _line("Study", F.concat_ws(" - ", "nct_id", "brief_title")),
        _line("Official title", F.col("official_title")),
        _line("Status", F.col("overall_status")),
        _line("Study type", F.col("study_type")),
        _line("Phase", _joined("phases")),
        _line("Conditions", _joined("conditions")),
        _line("Interventions", _joined("intervention_names")),
        _line("Sponsor", F.col("lead_sponsor_name")),
        _line("Enrollment", F.when(enrollment != "", enrollment)),
        _line("Dates", F.when(dates != "", dates)),
        _line("Primary outcomes", _joined("primary_outcome_measures")),
        _line("Summary", F.col("brief_summary")),
        _line("Description", F.col("detailed_description")),
        _line("Eligibility", F.when(eligibility != "", eligibility)),
        _line("Eligibility criteria", F.col("eligibility_criteria")),
    )
    return docs.withColumn("document_text", scrub(text)).withColumn(
        "document_chars", F.length("document_text")
    )


GOLD_RULES = _null_is_failure({"document_long_enough": f"document_chars >= {MIN_DOCUMENT_CHARS}"})

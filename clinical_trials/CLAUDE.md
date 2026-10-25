# CLAUDE.md: clinical_trials

This folder holds ingestion code for the **ClinicalTrials.gov REST API v2**. The general
Python, SQL, and git rules are in the root `../CLAUDE.md`. Follow those too.

## Source of truth

- Full OpenAPI spec: `../docs/ctg-oas-v2.yaml` (OpenAPI `3.0.3`, API version `2.0.5`).
  The YAML below is a condensed copy. Its patterns, enums, defaults, and limits are copied
  exactly from the spec. If you need a description, example, or result-section schema that
  isn't shown here, read the spec file. Don't guess.
- Before you write ingestion code, check `/version`. If `apiVersion` isn't `2.0.5`, stop
  and tell the user, because the spec may have drifted.

## Commands

```bash
# API smoke tests (no auth needed)
curl -s "https://clinicaltrials.gov/api/v2/version"
curl -s "https://clinicaltrials.gov/api/v2/studies?query.cond=lung+cancer&fields=NCTId|BriefTitle|OverallStatus&pageSize=5"

# Live enum values and field names (use these instead of hard-coding)
curl -s "https://clinicaltrials.gov/api/v2/studies/enums"
curl -s "https://clinicaltrials.gov/api/v2/studies/metadata"

# Python syntax check before you claim a script works
python -m py_compile <script>.py

# Fetch one study as a FHIR Bundle (raw/ is git-ignored)
python fetch_study_fhir.py NCT04280705 --out raw/NCT04280705.fhir.json
# ...and one row per study (titles, eligibility, locations as JSON) as CSV (needs pandas)
python fetch_study_fhir.py NCT04280705 --csv raw/NCT04280705_study.csv

# Databricks Asset Bundle: job `study_contacts` (resources/study_contacts.job.yml) runs
# load_study_contacts.py and writes one row per study to
# ${fhir_catalog}.${fhir_schema}.${fhir_table} (clinicaltrials.fhir_studies.studies).
# The lakehouse jobs are in "Clinical trials lakehouse" below. Needs the Databricks CLI.
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev study_contacts

# study_dataframe/: one study -> pandas DataFrames (own venv, raw/ git-ignored)
cd study_dataframe && python -m venv .venv && .venv/Scripts/python -m pip install -r requirements.txt
.venv/Scripts/python study_dataframe.py NCT04280705 --csv-dir raw
```

The only test suite is `tests/test_transforms.py` (local PySpark test of the pipeline
transforms; see "Clinical trials lakehouse" below). There's no requirements file for the
bundle. Don't invent commands. If you add one, record it here.

## Development rules

1. **Validate before you send.** Check every parameter against the patterns, enums,
   `minItems`/`maxItems`, and `minimum` values in the YAML below before you build a request.
   Use `re.fullmatch` with the pattern string as written. Then reject the request locally
   instead of relying on a 400 from the API.
2. **Pagination (`/studies`):**
   - Keep requesting pages until the response has no `nextPageToken`. Send that value
     back as `pageToken`, and don't send `pageToken` on the first page.
   - Every later request must send the first request's parameters **exactly**. Only
     `countTotal`, `pageSize`, and `pageToken` may change.
   - A page can have an empty `studies` array. That doesn't mean you've reached the end.
   - `pageSize`: unset or `0` gives the default of 10. Values above 1000 are reduced to 1000.
     Use `1000` for bulk pulls.
   - `countTotal=true` returns `totalCount` on the first page only. In CSV it comes back in
     the `x-total-count` header. The CSV next-page token is in `x-next-page-token`.
3. **Query vs. filter:**
   - `query.*` uses Essie expression syntax and affects relevance ranking.
     `filter.*` and `postFilter.*` don't affect ranking.
   - If any `query.*` parameter contains only NCT IDs, the API ignores **all** filters.
     When you need to filter by IDs, use `filter.ids`.
   - If you send no query and no filter, the API returns every study. Never do that by
     accident.
4. **Sorting:** results are **unsorted by default**. Set `sort` whenever order matters.
   It accepts at most 2 items, and only date or numeric fields or `@relevance`. Studies
   that lack the sort field always come last. Default directions are desc for dates, asc
   for numbers, and desc for `@relevance`.
5. **Payload size:** always set `fields` for bulk pulls, and never combine an unbounded
   `fields` with a large `pageSize`. `fields` can't be used with `format=fhir.json`.
6. **Array parameters** use `pipeDelimited` style with `explode: false`, so send
   `a|b|c` as one value. Commas are also accepted.
7. **Single study (`/studies/{nctId}`):** an alias NCT ID returns `301`, and you should
   follow the redirect. `404` means the ID wasn't found.
8. **Errors:** `400` and `404` bodies are `text/plain`, not JSON. Log the body text.
9. **Enums:** don't hard-code enum values in pipeline code. Load them from
   `/studies/enums` once per run and validate against that. Use the enums embedded
   below for review and offline checks. If a live value isn't in them, flag spec drift.
   Get field and piece names from `/studies/metadata`.
10. **Response shape:** `PagedStudies` has `additionalProperties: false`, and `studies` is
    its only required key. Every `Study` property is optional, so access nested keys with
    `.get()` instead of assuming they exist.
11. Don't commit raw API dumps. Write them to a git-ignored location or the scratchpad.

## Exact YAML rules (condensed from `../docs/ctg-oas-v2.yaml`)

```yaml
servers:
- url: "https://clinicaltrials.gov/api/v2"

# ===================== GET /studies  (operationId: listStudies) =====================
parameters:
- name: "format"
  schema: {type: "string", enum: ["csv", "json"], default: "json"}
- name: "markupFormat"            # applicable only to json
  schema: {type: "string", enum: ["markdown", "legacy"], default: "markdown"}
# query.* — all {type: "string"}, Essie expression syntax:
#   query.cond, query.term, query.locn, query.titles, query.intr,
#   query.outc, query.spons, query.lead, query.id, query.patient
- name: "filter.overallStatus"    # identical rules for postFilter.overallStatus
  style: "pipeDelimited"
  explode: false
  schema: {type: "array", items: {$ref: "#/components/schemas/Status"}}
- name: "filter.geo"              # identical rules for postFilter.geo
  schema:
    type: "string"
    pattern: "^distance\\(-?\\d+(\\.\\d+)?,-?\\d+(\\.\\d+)?,\\d+(\\.\\d+)?(km|mi)?\\)$"
  # example: "distance(39.0035707,-77.1013313,50mi)"
- name: "filter.ids"              # identical rules for postFilter.ids
  style: "pipeDelimited"
  explode: false
  schema:
    type: "array"
    items: {type: "string", pattern: "^[Nn][Cc][Tt]0*[1-9]\\d{0,7}$"}
  # searched in both NCTId and NCTIdAlias
- name: "filter.advanced"         # identical rules for postFilter.advanced
  schema: {type: "string"}        # Essie syntax, e.g. "AREA[StartDate]2022"
- name: "filter.synonyms"         # identical rules for postFilter.synonyms
  style: "pipeDelimited"
  explode: false
  schema: {type: "array", items: {type: "string"}}   # "area:synonym_id", e.g. "ConditionSearch:1651367"
- name: "aggFilters"
  schema: {type: "string"}        # "filter_id:space-separated option keys", e.g. "results:with,status:com"
- name: "geoDecay"                # ignored unless filter.geo is set and result <= 10,000 studies
  schema:
    type: "string"
    pattern: "^func:(gauss|exp|linear),scale:(\\d+(\\.\\d+)?(km|mi)),offset:(\\d+(\\.\\d+)?(km|mi)),decay:(\\d+(\\.\\d+)?)$"
    default: "func:exp,scale:300mi,offset:0mi,decay:0.5"
- name: "fields"                  # unspecified = all fields
  style: "pipeDelimited"
  explode: false
  schema:
    type: "array"
    minItems: 1
    items: {type: "string", pattern: "^([a-zA-Z][a-zA-Z0-9\\-. ]*)|(@query)$"}
- name: "sort"
  style: "pipeDelimited"
  explode: false
  schema:
    type: "array"
    maxItems: 2
    default: []
    items: {type: "string", pattern: "^(([a-zA-Z][a-zA-Z0-9\\-. ]*)|(@relevance))(:(asc|desc))?$"}
- name: "countTotal"
  schema: {type: "boolean", default: false}
- name: "pageSize"
  schema: {type: "integer", format: "int32", minimum: 0, default: 10}   # coerced down to 1000
- name: "pageToken"
  schema: {type: "string"}        # omit on first page
responses:
  "200": {application/json: {$ref: "#/components/schemas/PagedStudies"}}
  "400": {text/plain: {$ref: "#/components/schemas/errorMessage"}}

# ================= GET /studies/{nctId}  (operationId: fetchStudy) =================
parameters:
- name: "nctId"
  in: "path"
  required: true
  schema: {type: "string", pattern: "^[Nn][Cc][Tt]0*[1-9]\\d{0,7}$"}
- name: "format"
  schema: {type: "string", enum: ["csv", "json", "json.zip", "fhir.json", "ris"], default: "json"}
- name: "markupFormat"
  schema: {type: "string", enum: ["markdown", "legacy"], default: "markdown"}
- name: "fields"                  # must be unspecified for fhir.json; RIS tags for ris
  style: "pipeDelimited"
  explode: false
  schema:
    type: "array"
    minItems: 1
    items: {type: "string", pattern: "^[a-zA-Z][a-zA-Z0-9\\-. ]*$"}
responses:
  "200": {text/csv: StudiesCsv, application/json: Study, application/zip: StudiesZip, application/fhir+json: StudyFhir}
  "301": "Moved Permanently"      # alias NCT ID -> follow redirect
  "400": {text/plain: errorMessage}
  "404": {text/plain: errorMessage}

# ============================ metadata / stats / version ============================
/studies/metadata:
  params: {includeIndexedOnly: {type: boolean, default: false}, includeHistoricOnly: {type: boolean, default: false}}
  returns: FieldNodeList
/studies/search-areas: {returns: SearchDocumentList}
/studies/enums:        {returns: EnumInfoList}
/stats/size:           {returns: GzipStats}
/stats/field/values:
  params:
    types:  {style: pipeDelimited, explode: false, type: array, default: [], items: FieldStatsType}
    fields: {style: pipeDelimited, explode: false, type: array, minItems: 1,
             items: {type: string, pattern: "^[a-zA-Z][a-zA-Z0-9\\-. ]*$"}}
  returns: FieldValuesStatsList   # 400, 404 -> text/plain
/stats/field/sizes:
  params:
    fields: {style: pipeDelimited, explode: false, type: array,
             items: {type: string, pattern: "^[a-zA-Z][a-zA-Z0-9\\-. ]*$", minLength: 1}}
  returns: ListSizesList          # 400, 404 -> text/plain
/version:              {returns: Version}

# ==================================== components ====================================
PagedStudies:
  type: "object"
  properties:
    nextPageToken: {type: "string"}
    studies: {$ref: "#/components/schemas/StudyList"}   # array of Study
    totalCount: {type: "integer", format: "int32"}
  additionalProperties: false
  required: ["studies"]
Version:
  properties: {apiVersion: {type: "string"}, dataTimestamp: {type: "string"}}  # dataTimestamp: yyyy-MM-dd'T'HH:mm:ss UTC
  additionalProperties: false
  required: ["apiVersion"]
EnumInfo:
  properties: {pieces: string[], type: string, values: EnumItem[]}
  required: ["pieces", "type", "values"]
EnumItem:
  properties: {exceptions: object, legacyValue: string, value: string}
  required: ["legacyValue", "value"]
FieldNode:
  properties: {altPieceNames: string[], children: FieldNode[], dedLink: WebLink, description: string,
               historicOnly: boolean, indexedOnly: boolean, isEnum: boolean, maxChars: int32,
               name: string, nested: boolean, piece: string, rules: string, sourceType: string,
               synonyms: boolean, title: string, type: string}
  required: ["name", "piece", "sourceType", "type"]
SearchDocument: {properties: {areas: SearchArea[], name: string}, required: ["areas", "name"]}
SearchArea:     {properties: {name: string, param: string, parts: SearchPart[], uiLabel: string}, required: ["name", "parts"]}
SearchPart:     {properties: {isEnum: boolean, isSynonyms: boolean, pieces: string[], type: string, weight: float},
                 required: ["isEnum", "isSynonyms", "pieces", "type", "weight"]}

# --- scalar types ---
nct:             {type: "string", minLength: 11, maxLength: 11}
PartialDate:     {type: "string"}   # "yyyy", "yyyy-MM", or "yyyy-MM-dd"
DateTimeMinutes: {type: "string"}   # "yyyy-MM-dd'T'HH:mm"
GeoName:         {type: "string"}
GeoPoint:        {properties: {lat: number, lon: number}, required: ["lat", "lon"]}
DateStruct:        {date: date, type: DateType}
PartialDateStruct: {date: PartialDate, type: DateType}
# age / duration pattern (EligibilityModule.minimumAge, maximumAge; DesignModule.targetDuration):
#   "^\\d+ (Year|Years|Month|Months|Week|Weeks|Day|Days|Hour|Hours|Minute|Minutes)$"

# --- enums (exact, spec 2.0.5) ---
FieldStatsType:            ["ENUM", "STRING", "DATE", "INTEGER", "NUMBER", "BOOLEAN"]
Status:                    ["ACTIVE_NOT_RECRUITING", "COMPLETED", "ENROLLING_BY_INVITATION", "NOT_YET_RECRUITING",
                            "RECRUITING", "SUSPENDED", "TERMINATED", "WITHDRAWN", "AVAILABLE", "NO_LONGER_AVAILABLE",
                            "TEMPORARILY_NOT_AVAILABLE", "APPROVED_FOR_MARKETING", "WITHHELD", "UNKNOWN"]
RecruitmentStatus:         ["ACTIVE_NOT_RECRUITING", "COMPLETED", "ENROLLING_BY_INVITATION", "NOT_YET_RECRUITING",
                            "RECRUITING", "SUSPENDED", "TERMINATED", "WITHDRAWN", "AVAILABLE"]
ExpandedAccessStatus:      ["AVAILABLE", "NO_LONGER_AVAILABLE", "TEMPORARILY_NOT_AVAILABLE", "APPROVED_FOR_MARKETING"]
StudyType:                 ["EXPANDED_ACCESS", "INTERVENTIONAL", "OBSERVATIONAL"]
Phase:                     ["NA", "EARLY_PHASE1", "PHASE1", "PHASE2", "PHASE3", "PHASE4"]
Sex:                       ["FEMALE", "MALE", "ALL"]
StandardAge:               ["CHILD", "ADULT", "OLDER_ADULT"]
SamplingMethod:            ["PROBABILITY_SAMPLE", "NON_PROBABILITY_SAMPLE"]
IpdSharing:                ["YES", "NO", "UNDECIDED"]
IpdSharingInfoType:        ["STUDY_PROTOCOL", "SAP", "ICF", "CSR", "ANALYTIC_CODE"]
OrgStudyIdType:            ["NIH", "FDA", "VA", "CDC", "AHRQ", "SAMHSA"]
SecondaryIdType:           ["NIH", "FDA", "VA", "CDC", "AHRQ", "SAMHSA", "OTHER_GRANT", "EUDRACT_NUMBER", "CTIS",
                            "REGISTRY", "OTHER"]
AgencyClass:               ["NIH", "FED", "OTHER_GOV", "INDIV", "INDUSTRY", "NETWORK", "AMBIG", "OTHER", "UNKNOWN"]
DateType:                  ["ACTUAL", "ESTIMATED"]
EnrollmentType:            ["ACTUAL", "ESTIMATED"]
ResponsiblePartyType:      ["SPONSOR", "PRINCIPAL_INVESTIGATOR", "SPONSOR_INVESTIGATOR"]
DesignAllocation:          ["RANDOMIZED", "NON_RANDOMIZED", "NA"]
InterventionalAssignment:  ["SINGLE_GROUP", "PARALLEL", "CROSSOVER", "FACTORIAL", "SEQUENTIAL"]
PrimaryPurpose:            ["TREATMENT", "PREVENTION", "DIAGNOSTIC", "ECT", "SUPPORTIVE_CARE", "SCREENING",
                            "HEALTH_SERVICES_RESEARCH", "BASIC_SCIENCE", "DEVICE_FEASIBILITY", "OTHER"]
ObservationalModel:        ["COHORT", "CASE_CONTROL", "CASE_ONLY", "CASE_CROSSOVER", "ECOLOGIC_OR_COMMUNITY",
                            "FAMILY_BASED", "DEFINED_POPULATION", "NATURAL_HISTORY", "OTHER"]
DesignTimePerspective:     ["RETROSPECTIVE", "PROSPECTIVE", "CROSS_SECTIONAL", "OTHER"]
BioSpecRetention:          ["NONE_RETAINED", "SAMPLES_WITH_DNA", "SAMPLES_WITHOUT_DNA"]
ArmGroupType:              ["EXPERIMENTAL", "ACTIVE_COMPARATOR", "PLACEBO_COMPARATOR", "SHAM_COMPARATOR",
                            "NO_INTERVENTION", "OTHER"]
InterventionType:          ["BEHAVIORAL", "BIOLOGICAL", "COMBINATION_PRODUCT", "DEVICE", "DIAGNOSTIC_TEST",
                            "DIETARY_SUPPLEMENT", "DRUG", "GENETIC", "PROCEDURE", "RADIATION", "OTHER"]
ContactRole:               ["STUDY_CHAIR", "STUDY_DIRECTOR", "PRINCIPAL_INVESTIGATOR", "SUB_INVESTIGATOR", "CONTACT"]
OfficialRole:              ["STUDY_CHAIR", "STUDY_DIRECTOR", "PRINCIPAL_INVESTIGATOR", "SUB_INVESTIGATOR"]
ReferenceType:             ["BACKGROUND", "RESULT", "DERIVED"]
MeasureParam:              ["GEOMETRIC_MEAN", "GEOMETRIC_LEAST_SQUARES_MEAN", "LEAST_SQUARES_MEAN", "LOG_MEAN", "MEAN",
                            "MEDIAN", "NUMBER", "COUNT_OF_PARTICIPANTS", "COUNT_OF_UNITS"]
MeasureDispersionType:     ["NA", "STANDARD_DEVIATION", "STANDARD_ERROR", "INTER_QUARTILE_RANGE", "FULL_RANGE",
                            "CONFIDENCE_80", "CONFIDENCE_90", "CONFIDENCE_95", "CONFIDENCE_975", "CONFIDENCE_99",
                            "CONFIDENCE_OTHER", "GEOMETRIC_COEFFICIENT"]
OutcomeMeasureType:        ["PRIMARY", "SECONDARY", "OTHER_PRE_SPECIFIED", "POST_HOC"]
ReportingStatus:           ["NOT_POSTED", "POSTED"]
EventAssessment:           ["NON_SYSTEMATIC_ASSESSMENT", "SYSTEMATIC_ASSESSMENT"]
AgreementRestrictionType:  ["LTE60", "GT60", "OTHER"]
BrowseLeafRelevance:       ["LOW", "HIGH"]
DesignMasking:             ["NONE", "SINGLE", "DOUBLE", "TRIPLE", "QUADRUPLE"]
WhoMasked:                 ["PARTICIPANT", "CARE_PROVIDER", "INVESTIGATOR", "OUTCOMES_ASSESSOR"]
AnalysisDispersionType:    ["STANDARD_DEVIATION", "STANDARD_ERROR_OF_MEAN"]
ConfidenceIntervalNumSides: ["ONE_SIDED", "TWO_SIDED"]
NonInferiorityType:        ["SUPERIORITY", "NON_INFERIORITY", "EQUIVALENCE", "OTHER", "NON_INFERIORITY_OR_EQUIVALENCE",
                            "SUPERIORITY_OR_OTHER", "NON_INFERIORITY_OR_EQUIVALENCE_LEGACY", "SUPERIORITY_OR_OTHER_LEGACY"]
UnpostedEventType:         ["RESET", "RELEASE", "UNRELEASE"]
ViolationEventType:        ["VIOLATION_IDENTIFIED", "CORRECTION_CONFIRMED", "PENALTY_IMPOSED",
                            "ISSUES_IN_LETTER_ADDRESSED_CONFIRMED"]

# --- Study data model (JSON paths; all properties optional) ---
Study: {protocolSection, resultsSection, annotationSection, documentSection, derivedSection, hasResults: boolean}
protocolSection:
  identificationModule:   {nctId: nct, nctIdAliases: nct[], orgStudyIdInfo: {id, type: OrgStudyIdType, link},
                           secondaryIdInfos: [{id, type: SecondaryIdType, domain, link}], briefTitle, officialTitle,
                           acronym, organization: {fullName, class: AgencyClass}}
  statusModule:           {statusVerifiedDate: PartialDate, overallStatus: Status, lastKnownStatus: Status,
                           delayedPosting: boolean, whyStopped,
                           expandedAccessInfo: {hasExpandedAccess: boolean, nctId: nct, statusForNctId: ExpandedAccessStatus},
                           startDateStruct: PartialDateStruct, primaryCompletionDateStruct: PartialDateStruct,
                           completionDateStruct: PartialDateStruct, studyFirstSubmitDate: date,
                           studyFirstSubmitQcDate: date, studyFirstPostDateStruct: DateStruct, resultsWaived: boolean,
                           resultsFirstSubmitDate: date, resultsFirstSubmitQcDate: date,
                           resultsFirstPostDateStruct: DateStruct, dispFirstSubmitDate: date,
                           dispFirstSubmitQcDate: date, dispFirstPostDateStruct: DateStruct,
                           lastUpdateSubmitDate: date, lastUpdatePostDateStruct: DateStruct}
  sponsorCollaboratorsModule: {responsibleParty: {type: ResponsiblePartyType, investigatorFullName, investigatorTitle,
                               investigatorAffiliation, oldNameTitle, oldOrganization},
                               leadSponsor: {name, class: AgencyClass}, collaborators: [{name, class: AgencyClass}]}
  oversightModule:        {oversightHasDmc, isFdaRegulatedDrug, isFdaRegulatedDevice, isUnapprovedDevice, isPpsd,
                           isUsExport, fdaaa801Violation}          # all boolean
  descriptionModule:      {briefSummary, detailedDescription}
  conditionsModule:       {conditions: string[], keywords: string[]}
  designModule:           {studyType: StudyType, nPtrsToThisExpAccNctId: integer,
                           expandedAccessTypes: {individual, intermediate, treatment},   # boolean
                           patientRegistry: boolean, targetDuration: age/duration pattern, phases: Phase[],
                           designInfo: {allocation: DesignAllocation, interventionModel: InterventionalAssignment,
                                        interventionModelDescription, primaryPurpose: PrimaryPurpose,
                                        observationalModel: ObservationalModel, timePerspective: DesignTimePerspective,
                                        maskingInfo: {masking: DesignMasking, maskingDescription, whoMasked: WhoMasked[]}},
                           bioSpec: {retention: BioSpecRetention, description},
                           enrollmentInfo: {count: integer, type: EnrollmentType}}
  armsInterventionsModule: {armGroups: [{label, type: ArmGroupType, description, interventionNames: string[]}],
                            interventions: [{type: InterventionType, name, description, armGroupLabels: string[],
                                             otherNames: string[]}]}
  outcomesModule:         {primaryOutcomes, secondaryOutcomes, otherOutcomes}   # each [{measure, description, timeFrame}]
  eligibilityModule:      {eligibilityCriteria, healthyVolunteers: boolean, sex: Sex, genderBased: boolean,
                           genderDescription, minimumAge: age pattern, maximumAge: age pattern,
                           stdAges: StandardAge[], studyPopulation, samplingMethod: SamplingMethod}
  contactsLocationsModule: {centralContacts: Contact[], overallOfficials: [{name, affiliation, role: OfficialRole}],
                            locations: [{facility, status: RecruitmentStatus, city: GeoName, state: GeoName, zip,
                                         country, contacts: Contact[], geoPoint: GeoPoint}]}
                          # Contact: {name, role: ContactRole, phone, phoneExt, email}
  referencesModule:       {references: [{pmid, type: ReferenceType, citation, retractions: [{pmid, source}]}],
                           seeAlsoLinks: [{label, url}], availIpds: [{id, type, url, comment}]}
  ipdSharingStatementModule: {ipdSharing: IpdSharing, description, infoTypes: IpdSharingInfoType[], timeFrame,
                              accessCriteria, url}
resultsSection:           # full schemas in ../docs/ctg-oas-v2.yaml
  [participantFlowModule, baselineCharacteristicsModule, outcomeMeasuresModule, adverseEventsModule, moreInfoModule]
annotationSection: {annotationModule: {unpostedAnnotation, violationAnnotation}}
documentSection:   {largeDocumentModule: {noSap: boolean, largeDocs: LargeDoc[]}}
derivedSection:
  miscInfoModule: {versionHolder: date, removedCountries: string[], submissionTracking}
  conditionBrowseModule / interventionBrowseModule:   # BrowseModule
    {meshes: [{id, term}], ancestors: [{id, term}],
     browseLeaves: [{id, name, asFound, relevance: BrowseLeafRelevance}], browseBranches: [{abbrev, name}]}
```

Note: as written, the `/studies` `fields` pattern doesn't group its alternation:
`^(A)|(@query)$`. Validate each item with `re.fullmatch` against
`[a-zA-Z][a-zA-Z0-9\-. ]*|@query` so the anchors apply to both alternatives.

---

# Clinical trials lakehouse

## Goal

Ingest clinical trial records from the ClinicalTrials.gov v2 API into a medallion lakehouse on
Databricks. The gold table feeds an AI model.

- Source: `GET /studies?query.cond=<condition>` (default `lung cancer`), paged, capped at
  `max_studies` per run (default 100)
- Layers: bronze (raw) → silver (validated, deduplicated, PII-tagged) → gold (PII-free, AI-ready)

## Constraints

- **Databricks Free Edition**: serverless compute only. Never add cluster configs
  (`new_cluster`, `job_clusters`, `clusters`, instance pools, `node_type_id`).
  Pipelines use `serverless: true`; job tasks use serverless environments.
- **Catalog**: `workspace`.
- **Packaging**: everything is a Databricks Asset Bundle (`databricks.yml` + `resources/`).
  The `dev` target uses `mode: development`. `databricks.yml` must keep
  `include: resources/*.yml`: without it a deploy deletes every resource defined there.
- **Bundle-owned storage**: the schema `clinical_trials` and the managed volume `landing` are
  bundle resources. Always reference them by resource interpolation, never by literal name, so
  dev-mode prefixes (`dev_<user>_clinical_trials`) flow through:
  - schema: `${resources.schemas.clinical_trials.name}`
  - volume: `${resources.volumes.landing.name}`
  - landing path: `/Volumes/${var.catalog}/${resources.schemas.clinical_trials.name}/${resources.volumes.landing.name}/studies`
- **Transformations**: Lakeflow Spark Declarative Pipelines (SDP), modern API
  (`from pyspark import pipelines as dp`), not legacy `dlt`.
- Workspace profile: `monicaenders` (`https://dbc-2831e5fc-ccb1.cloud.databricks.com`).
  Pass `--profile monicaenders` on every CLI call.

## As built

- **Landing** (`src/ctgov_client.py`, run by `src/01_ingest_api.py`): searches `/studies` with
  `query.cond`, `fields=protocolSection|hasResults`, `sort=@relevance`, `pageSize=1000`, follows
  `nextPageToken` with the first page's parameters unchanged, and checks `/version` each run.
  Each page lands as one envelope (about 12 MB for 1,000 studies) at
  `<volume>/studies/search/{yyyyMMddTHHmmssZ}_{ingest_id[:8]}_page{nnnn}.json`:
  `source_url, http_status, ingested_at, ingest_id, query, page, payload`, where `payload` is the
  page (`studies`, `nextPageToken`, `totalCount`). A failed page is landed with its error body and
  stops paging; empty pages aren't landed. Files are written as `_*.tmp` and renamed, so Auto
  Loader never sees partial files. Retries 429/5xx with backoff; if the API is unreachable,
  `scripts/fetch_and_upload.py` does the same search locally and uploads with `databricks fs cp`.
  Older single-study envelopes (`{NCT_ID}/...json`, with `requested_nct_id`) still parse.
- **Incremental pulls**: `01_ingest_api.py` records every run in `ingest_runs` (`complete` = the
  search reached its last page with no error or cap). With `incremental=true`, a condition that has
  a complete run is searched with `filter.advanced=AREA[LastUpdatePostDate]RANGE[<that run's UTC
  date - 1 day>,MAX]`; the day of overlap covers US-dated updates posted after the run. Blank
  `max_studies` lands every match; a capped run is never complete. `incremental=false` re-fetches all.
- **Bronze** `bronze_studies_raw`: Auto Loader `binaryFile`, so nothing is parsed or dropped.
  One row per landed file (a page or a single study).
- **Silver**: page envelopes are split into one row per study. `silver_studies_validated` (critical rules via `expect_all_or_drop`, warnings via
  `expect_all`), `silver_studies_quarantine` (same parse, `failed_rules` array),
  `silver_studies` (Auto CDC SCD Type 1 streaming table from validated, keyed on `nct_id`,
  sequenced by `struct(last_update_posted_date, ingested_at)`; keeps `payload` for the child
  tables), `silver_study_contacts` (PII), `silver_study_locations`. Rules live in
  `src/pipeline/transforms.py`; NULL rule results count as failures.
  Known gap: the `known_overall_status` rule hard-codes the status list instead of loading
  `/studies/enums` (development rule 9).
- **PII tagging** (`src/02_tag_pii.py`): UC column tags `pii` / `pii_type` / `pii_detection`,
  table tag `contains_pii`, audit in `pii_tag_audit`. Removes tags a run no longer confirms and
  skips pipeline-internal `__materialization_*` tables. Fails the run on direct identifiers in gold.
- **Gold** `gold_study_documents`: allow-listed columns only; free text and `document_text`
  scrubbed of emails/phones; rows under 200 chars dropped; CDF on.
- **Vector Search** (`src/03_vector_index.py`): Free Edition can't index materialized views, so gold
  is merged into the Delta table `gold_study_documents_vs_source`, and the Delta Sync index
  `gold_study_documents_index` (endpoint `clinical-trials-vs`, `databricks-gte-large-en`) syncs from it.
- **Agent** (`src/agent/agent.py`, MLflow `ResponsesAgent`): retrieves from the gold index, answers
  with `databricks-meta-llama-3-3-70b-instruct`, registered as `<schema>.clinical_trials_agent`.
  Declared resources are the gold index and the two model endpoints only, never silver.
  `src/04_log_agent.py` logs and tests it; `src/05_test_agent.py` asks it questions.

## Layout

```
clinical_trials/
├── databricks.yml                     # bundle, variables, sync excludes, dev target
├── resources/
│   ├── clinical_trials.schema.yml     # schema clinical_trials
│   ├── landing.volume.yml             # managed volume landing
│   ├── clinical_trials.pipeline.yml   # SDP pipeline (serverless, triggered)
│   ├── clinical_trials.job.yml        # ingest → pipeline → tag PII → sync vector index
│   ├── clinical_trials_agent.job.yml  # log, register and test the agent
│   └── study_contacts.job.yml         # FHIR loader (load_study_contacts.py)
├── src/
│   ├── ctgov_client.py                # stdlib search client + envelope writer (shared)
│   ├── 01_ingest_api.py … 05_test_agent.py
│   ├── agent/agent.py                 # ResponsesAgent
│   └── pipeline/                      # bronze.py, silver.py, gold.py, transforms.py
├── scripts/fetch_and_upload.py        # local fallback if compute can't reach the API
├── tests/test_transforms.py           # local PySpark test of transforms
├── load_study_contacts.py, fetch_study_fhir.py, study_dataframe/, explore_study_fhir.ipynb
```

## Lakehouse commands

```
databricks bundle validate --strict -t dev --profile monicaenders
databricks bundle plan -t dev --profile monicaenders     # check for unexpected deletes first
databricks bundle deploy -t dev --profile monicaenders
databricks bundle run clinical_trials_job -t dev --profile monicaenders
databricks bundle run clinical_trials_agent_job -t dev --profile monicaenders

# Local transform test: needs PySpark 4.0+, Java 17+, Python <= 3.13
python tests/test_transforms.py
```

Deploying deletes any resource in this bundle's state that isn't in the config. Run
`databricks bundle plan` before every deploy and stop if it shows a delete you didn't intend.

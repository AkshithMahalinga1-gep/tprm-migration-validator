import pandas as pd
from pymongo import MongoClient
import os
from datetime import datetime
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from mongoCalls import (
    apiCall, fetch_forms_responses,
    fetch_aggregated_rsa_documents, fetch_relationships,
    fetch_relationship_forms, fetch_manage_recurrences,
)
from validationHelper import validate_response_data



# --- Configuration ---
MONGO_URI = "mongodb+srv://prodleoustenantdocteamsro:XFrA8ZH4NuJAripp@prod-us-leo-tenant.sppsx.mongodb.net/?ssl=true&authSource=admin&retryWrites=true&readPreference=secondaryPreferred&w=majority&wtimeoutMS=5000&readConcernLevel=majority&retryReads=true&appName=tprm"
DB_NAME = "proddomainmodeladobe"
SRSA_COLLECTION = "riskAssessment_1664901704"
FORM_COLLECTION = "form_1663277990"
RESPONSE_COLLECTION = "documentQuestionnaire_1695405087"
RECURRENCE_COLLECTION = "manageRecurrence_1671208142"
RELATIONSHIP_COLLECTION = "relationship_1667773947"

# --- MongoDB Connection ---
def connect_to_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

# --- File Selection ---
INPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Input Files")

def select_files():
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    files = [
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if f.endswith(".xlsx")
    ]
    if not files:
        print(f"No .xlsx files found in '{INPUT_FOLDER}'. Please drop input files there and re-run.")
        exit()
    print(f"Found {len(files)} file(s) in Input Files: {[os.path.basename(f) for f in files]}")
    return files

# --- Load Excel Sheets ---
def load_excel_sheets(file_path):
    sheets = pd.read_excel(file_path, sheet_name=None)
    return sheets

# --- Fetch SRSA Documents ---
def fetch_srsa_documents(db, contract_ids):
    print(f"Fetching Post Contract SRSA documents for contract IDs...")
    srsa_docs = list(db[SRSA_COLLECTION].find(
        {"revisedContractNumber": {"$in": contract_ids}, "dueDiligencePhase": "Post Contract", "isDeleted": False},
        {
            "internalDocumentId": 1,
            "documentNumber": 1,
            "revisedContractNumber": 1,
            "supplierId": 1,
            "dueDiligencePhase": 1,
            "basicDetail.category": 1,
            "basicDetail.businessUnit": 1,
            "basicDetail.region": 1,
            "riskProfile": 1,
            "residualRiskProfile.riskScoreLevel.riskScoreRating": 1,
            "riskAttributeFields": 1,
        }
    ))
    print(f"Fetched {len(srsa_docs)} Post-Contract SRSA documents")
    return srsa_docs

# --- Fetch Pre-Contract SRSA Documents ---
def fetch_pre_contract_srsa_documents(db, document_numbers):
    print(f"Fetching Pre Contract SRSA documents for document numbers...")
    pre_contract_srsa_docs = list(db[SRSA_COLLECTION].find(
        {"documentNumber": {"$in": document_numbers}, "dueDiligencePhase": "Pre Contract"},
        {"internalDocumentId": 1, "documentNumber": 1}
    ))
    print(f"Fetched {len(pre_contract_srsa_docs)} Pre-Contract SRSA documents")
    return pre_contract_srsa_docs

# --- Fetch Control Forms ---
def fetch_control_forms(db, pre_contract_srsa_ids):
    print(f"Fetching Control Forms for Pre Contract SRSAs...")
    control_forms = list(db[FORM_COLLECTION].find(
        {
            "supplierRSAId": {"$in": pre_contract_srsa_ids},
            "isDeleted": False,
            "formType": {"$in": [6, 8]},
        },
        {"sourceFormDocumentNumber": 1, "supplierRSAId": 1, "internalDocumentId": 1}
    ))
    print(f"Fetched {len(control_forms)} Control Forms")
    return control_forms

from concurrent.futures import ThreadPoolExecutor

def validate_data(srsa_df, risk_char_df, relationship_df, kra_df,
                  form_df, response_df_excel, applicable_forms_df,
                  srsa_doc_map, pre_contract_srsa_doc_map,
                  control_form_map, aggregated_rsa_map,
                  existing_relationship_supplier_ids, relationship_form_map,
                  recurrence_map, form_responses_mongo,
                  duplicate_post_contract_ids, duplicate_pre_contract_doc_numbers,
                  duplicate_agg_rsa_supplier_ids, api_available):
    validation_logs = {
        "Supplier Risk Assessment Header": [],
        "Risk Characteristics": [],
        "Relationship": [],
        "Key Risk Attributes": [],
        "Form Details": [],
        "Form Response": [],
        "Applicable Control Forms": [],
        "Duplicate Documents": [],
    }

    def validate_srsa_row(srsa):
        reference_id = srsa["Reference ID*"]
        contract_id = str(srsa["Contract ID*"]) if not pd.isna(srsa["Contract ID*"]) else None
        srsa_doc = srsa_doc_map.get(contract_id)
        preContractSrsa_internalDocumentId = ""
        documentNumber = None
        srsa_logs = []

        if srsa_doc:
            documentNumber = srsa_doc.get("documentNumber")
            srsa_pre_contract_doc = pre_contract_srsa_doc_map.get(documentNumber)
            if srsa_pre_contract_doc:
                preContractSrsa_internalDocumentId = srsa_pre_contract_doc["internalDocumentId"]

        # --- Duplicate document checks ---
        duplicate_logs = []

        if contract_id and contract_id in duplicate_post_contract_ids:
            duplicate_logs.append({
                "ReferenceID": reference_id,
                "Issue": "Duplicate Post-Contract SRSA found in DB",
                "ContractID": contract_id,
                "Detail": f"Multiple Post-Contract SRSA documents exist for revisedContractNumber={contract_id}",
            })

        if documentNumber and documentNumber in duplicate_pre_contract_doc_numbers:
            duplicate_logs.append({
                "ReferenceID": reference_id,
                "Issue": "Duplicate Pre-Contract SRSA found in DB",
                "DocumentNumber": documentNumber,
                "Detail": f"Multiple Pre-Contract SRSA documents exist for documentNumber={documentNumber}",
            })


        if preContractSrsa_internalDocumentId == "":
            return {
                "srsa_logs": [{
                    "ReferenceID": reference_id,
                    "Issue": "Pre Contract SRSA document missing",
                    "ContractID": contract_id,
                }],
                "risk_char_logs": [],
                "relationship_logs": [],
                "kra_logs": [],
                "form_logs": [],
                "applicable_form_logs": [],
                "duplicate_logs": duplicate_logs,
            }

        # --- SRSA Header new field validation ---
        if srsa_doc:
            def _check(field_label, excel_val, mongo_val):
                if pd.isna(excel_val) if not isinstance(excel_val, str) else False:
                    return
                if str(excel_val) != str(mongo_val) if mongo_val is not None else True:
                    srsa_logs.append({
                        "ReferenceID": reference_id,
                        "Field": field_label,
                        "ExcelValue": excel_val,
                        "MongoValue": mongo_val,
                        "Issue": "Field mismatch",
                    })

            _check("Due Diligence Phase*", srsa.get("Due Diligence Phase*"), srsa_doc.get("dueDiligencePhase"))

            def _check_set(field_label, excel_val, mongo_values):
                """Check that all comma-separated Excel values are present in the MongoDB set."""
                if pd.isna(excel_val) if not isinstance(excel_val, str) else False:
                    return
                excel_set = {str(v).strip() for v in str(excel_val).split(",")}
                mongo_set = {str(v) for v in mongo_values if v is not None}
                missing = excel_set - mongo_set
                if missing:
                    srsa_logs.append({
                        "ReferenceID": reference_id,
                        "Field": field_label,
                        "ExcelValue": excel_val,
                        "MongoValue": ", ".join(sorted(mongo_set)),
                        "Issue": "Field mismatch",
                    })

            basic = srsa_doc.get("basicDetail", {})

            categories = basic.get("category", [])
            _check_set("Category*",
                       srsa.get("Category*"),
                       [str(int(c["clientCode"])) for c in categories if c.get("clientCode") is not None])
            _check_set("Category Level*",
                       srsa.get("Category Level*"),
                       [f"L{c['level']}" for c in categories if c.get("level") is not None])

            bus = basic.get("businessUnit", [])
            _check_set("Business Unit*",
                       srsa.get("Business Unit*"),
                       [str(b.get("entityDetailCode")) for b in bus if b.get("entityDetailCode") is not None])
            _check_set("Business Unit Level*",
                       srsa.get("Business Unit Level*"),
                       [f"L{b['level']}" for b in bus if b.get("level") is not None])

            regions = basic.get("region", [])
            _check_set("Region*",
                       srsa.get("Region*"),
                       [r.get("name") for r in regions if r.get("name") is not None])
            _check_set("Region Level*",
                       srsa.get("Region Level*"),
                       [f"L{r['level']}" for r in regions if r.get("level") is not None])

            risk_profile = srsa_doc.get("riskProfile", {})
            _check("Overall Inherent Risk Rating", srsa.get("Overall Inherent Risk Rating"),
                   risk_profile.get("riskScoreLevel", {}).get("riskScoreRating"))
            _check("Risk Score", srsa.get("Risk Score"), risk_profile.get("overallScore"))
            _check("Residual Risk Rating", srsa.get("Residual Risk Rating"),
                   srsa_doc.get("residualRiskProfile", {}).get("riskScoreLevel", {}).get("riskScoreRating"))

        # --- Risk Characteristics validation ---
        risk_char_logs = []
        characteristics_score = (srsa_doc or {}).get("riskProfile", {}).get("characteristicsScore", [])
        ref_risk_char_rows = risk_char_df[risk_char_df["Reference ID*"] == reference_id]
        for _, rc_row in ref_risk_char_rows.iterrows():
            excel_name = rc_row.get("Risk Characteristic*")
            excel_rating = rc_row.get("Risk Characteristic Rating*")
            excel_score = rc_row.get("Risk Characteristic Score")
            found = False
            for cs in characteristics_score:
                rc = cs.get("riskCharacteristics", {})
                if rc.get("name") == excel_name and rc.get("riskCharacteristicsRating", {}).get("scoreLevel") == excel_rating:
                    if pd.isna(excel_score) or cs.get("characteristicScore") == excel_score:
                        found = True
                        break
            if not found:
                risk_char_logs.append({
                    "ReferenceID": reference_id,
                    "RiskCharacteristic": excel_name,
                    "Rating": excel_rating,
                    "Score": excel_score,
                    "Issue": "Risk Characteristic not found in DB",
                })

        # --- Key Risk Attributes validation ---
        kra_logs = []
        risk_attribute_fields = (srsa_doc or {}).get("riskAttributeFields", {})
        ref_kra_rows = kra_df[kra_df["Reference ID*"] == reference_id]
        for _, kra_row in ref_kra_rows.iterrows():
            attribute_key = kra_row.get("Key Risk Attribute*")
            excel_value = kra_row.get("Key Risk Value")
            if attribute_key not in risk_attribute_fields:
                kra_logs.append({
                    "ReferenceID": reference_id,
                    "KeyRiskAttribute": attribute_key,
                    "ExcelValue": excel_value,
                    "Issue": "Key Risk Attribute not found in DB",
                })
            elif str(risk_attribute_fields[attribute_key]) != str(excel_value):
                kra_logs.append({
                    "ReferenceID": reference_id,
                    "KeyRiskAttribute": attribute_key,
                    "ExcelValue": excel_value,
                    "MongoValue": risk_attribute_fields[attribute_key],
                    "Issue": "Key Risk Attribute value mismatch",
                })

        # --- Relationship validation ---
        relationship_logs = []
        supplier_id = (srsa_doc or {}).get("supplierId")

        ref_relationship_rows = relationship_df[relationship_df["Reference ID*"] == reference_id]
        for _, rel_row in ref_relationship_rows.iterrows():
            # Duplicate Aggregated RSA check
            if supplier_id and supplier_id in duplicate_agg_rsa_supplier_ids:
                agg_docs_dup = aggregated_rsa_map.get(supplier_id, [])
                relationship_logs.append({
                    "ReferenceID": reference_id,
                    "SupplierId": supplier_id,
                    "Issue": "Duplicate Aggregated Risk Assessment found in DB",
                    "AggregatedRSACount": len(agg_docs_dup),
                    "AggregatedRSAIds": ", ".join(d.get("internalDocumentId", "") for d in agg_docs_dup),
                })

            # Check 1: Relationship exists
            if not supplier_id or supplier_id not in existing_relationship_supplier_ids:
                relationship_logs.append({
                    "ReferenceID": reference_id,
                    "SupplierId": supplier_id,
                    "Issue": "Relationship not found in DB",
                })

            # Check 2: Relationship Risk Form exists
            form_id = rel_row.get("Relationship Risk Form ID*")
            agg_docs = aggregated_rsa_map.get(supplier_id, []) if supplier_id else []
            found_rel_form_ids = set()
            for agg_doc in agg_docs:
                found_rel_form_ids.update(relationship_form_map.get(agg_doc["internalDocumentId"], []))

            if form_id not in found_rel_form_ids:
                relationship_logs.append({
                    "ReferenceID": reference_id,
                    "RelationshipRiskFormID": form_id,
                    "SupplierId": supplier_id,
                    "AggregatedRSACount": len(agg_docs),
                    "AggregatedRSAIds": ", ".join(d.get("internalDocumentId", "") for d in agg_docs),
                    "FoundRelFormIds": ", ".join(sorted(found_rel_form_ids)),
                    "Issue": "Relationship Risk Form not found in DB",
                })

            # Check 3: Overall Inherent Risk Rating / Risk Score from Aggregated RSA
            excel_rating = rel_row.get("Overall Inherent Risk Rating")
            excel_score = rel_row.get("Risk Score")
            for agg_doc in agg_docs:
                agg_rp = agg_doc.get("riskProfile", {})
                mongo_rating = agg_rp.get("riskScoreLevel", {}).get("riskScoreRating")
                mongo_score = agg_rp.get("overallScore")
                if not pd.isna(excel_rating) and str(excel_rating) != str(mongo_rating):
                    relationship_logs.append({
                        "ReferenceID": reference_id,
                        "Field": "Overall Inherent Risk Rating",
                        "ExcelValue": excel_rating,
                        "MongoValue": mongo_rating,
                        "Issue": "Field mismatch",
                    })
                if not pd.isna(excel_score) and str(excel_score) != str(mongo_score):
                    relationship_logs.append({
                        "ReferenceID": reference_id,
                        "Field": "Risk Score",
                        "ExcelValue": excel_score,
                        "MongoValue": mongo_score,
                        "Issue": "Field mismatch",
                    })

        # --- Form Details validation ---
        linked_forms = control_form_map.get(preContractSrsa_internalDocumentId, [])
        found_form_ids = {form["sourceFormDocumentNumber"] for form in linked_forms}
        expected_forms = form_df[form_df["Reference ID*"] == reference_id]

        form_logs = []
        for _, form in expected_forms.iterrows():
            # Duplicate Pre-Contract SRSA check
            if documentNumber and documentNumber in duplicate_pre_contract_doc_numbers:
                form_logs.append({
                    "ReferenceID": reference_id,
                    "Issue": "Duplicate Pre-Contract SRSA found in DB",
                    "DocumentNumber": documentNumber,
                    "Detail": f"Multiple Pre-Contract SRSA documents exist for documentNumber={documentNumber}",
                })

            masterFormId = form.get("Master Form ID*")
            if masterFormId not in found_form_ids:
                form_logs.append({
                    "ReferenceID": reference_id,
                    "FormID": masterFormId,
                    "Issue": "Form missing in DB",
                    "SRSAID": preContractSrsa_internalDocumentId,
                    "SRSDocumentNumber": documentNumber,
                })
            else:
                matching_form_mongo = next(
                    (lf for lf in linked_forms if form["Master Form ID*"] == lf["sourceFormDocumentNumber"]), None
                )
                if matching_form_mongo and api_available:
                    validate_response_data(validation_logs, form, matching_form_mongo, response_df_excel, form_responses_mongo)

        # --- Applicable Control Forms validation ---
        applicable_form_logs = []
        srsa_internal_id = (srsa_doc or {}).get("internalDocumentId")
        recurrences = recurrence_map.get(srsa_internal_id, []) if srsa_internal_id else []
        ref_applicable_rows = applicable_forms_df[applicable_forms_df["Reference ID*"] == reference_id]
        for _, af_row in ref_applicable_rows.iterrows():
            # Duplicate Post-Contract SRSA check
            if contract_id and contract_id in duplicate_post_contract_ids:
                applicable_form_logs.append({
                    "ReferenceID": reference_id,
                    "Issue": "Duplicate Post-Contract SRSA found in DB",
                    "ContractID": contract_id,
                    "Detail": f"Multiple Post-Contract SRSA documents exist for revisedContractNumber={contract_id}",
                })

            master_form_id = af_row.get("Master Form ID*")
            matching_rec = next((r for r in recurrences if r.get("masterFormNumber") == master_form_id), None)
            if not matching_rec:
                applicable_form_logs.append({
                    "ReferenceID": reference_id,
                    "MasterFormID": master_form_id,
                    "Issue": "Applicable Control Form not found in DB",
                })
            else:
                rec_details = matching_rec.get("recurrenceDetails", {})
                cycle_details = matching_rec.get("recurrenceCycleDetails", {})

                excel_recurrence_every = af_row.get("Recurrence Every")
                mongo_recurrence_every = rec_details.get("recurrenceFromOcurrence")
                if not pd.isna(excel_recurrence_every) and str(int(excel_recurrence_every)) != str(mongo_recurrence_every):
                    applicable_form_logs.append({
                        "ReferenceID": reference_id,
                        "MasterFormID": master_form_id,
                        "Field": "Recurrence Every",
                        "ExcelValue": excel_recurrence_every,
                        "MongoValue": mongo_recurrence_every,
                        "Issue": "Field mismatch",
                    })

                excel_recurrence_type = af_row.get("Recurrence Type")
                mongo_recurrence_type = rec_details.get("recurrenceType")
                if not pd.isna(excel_recurrence_type) and str(excel_recurrence_type) != str(mongo_recurrence_type):
                    applicable_form_logs.append({
                        "ReferenceID": reference_id,
                        "MasterFormID": master_form_id,
                        "Field": "Recurrence Type",
                        "ExcelValue": excel_recurrence_type,
                        "MongoValue": mongo_recurrence_type,
                        "Issue": "Field mismatch",
                    })

                excel_next_launch = af_row.get("Next Launch Date")
                mongo_next_launch = cycle_details.get("nextLaunchDate")
                if not pd.isna(excel_next_launch) and mongo_next_launch is not None:
                    if isinstance(mongo_next_launch, datetime):
                        mongo_date_str = mongo_next_launch.strftime("%m/%d/%Y")
                    else:
                        mongo_date_str = str(mongo_next_launch)
                    excel_date_str = pd.to_datetime(excel_next_launch).strftime("%m/%d/%Y") if not isinstance(excel_next_launch, str) else excel_next_launch
                    if excel_date_str != mongo_date_str:
                        applicable_form_logs.append({
                            "ReferenceID": reference_id,
                            "MasterFormID": master_form_id,
                            "Field": "Next Launch Date",
                            "ExcelValue": excel_date_str,
                            "MongoValue": mongo_date_str,
                            "Issue": "Field mismatch",
                        })

        return {
            "srsa_logs": srsa_logs,
            "risk_char_logs": risk_char_logs,
            "relationship_logs": relationship_logs,
            "kra_logs": kra_logs,
            "form_logs": form_logs,
            "applicable_form_logs": applicable_form_logs,
            "duplicate_logs": duplicate_logs,
        }

    # Parallelize validation of rows
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(validate_srsa_row, srsa_df.to_dict(orient="records")))

    # Aggregate results
    for result in results:
        validation_logs["Supplier Risk Assessment Header"].extend(result["srsa_logs"])
        validation_logs["Risk Characteristics"].extend(result["risk_char_logs"])
        validation_logs["Relationship"].extend(result["relationship_logs"])
        validation_logs["Key Risk Attributes"].extend(result["kra_logs"])
        validation_logs["Form Details"].extend(result["form_logs"])
        validation_logs["Applicable Control Forms"].extend(result["applicable_form_logs"])
        validation_logs["Duplicate Documents"].extend(result["duplicate_logs"])

    if not api_available:
        validation_logs["Form Response"].append({
            "Issue": "Form Response validation was skipped — API call failed (check token.txt and re-run)",
        })

    return validation_logs

# --- Save Validation Results ---
def save_validation_results(output_file_name, sheets, validation_logs):
    if any(validation_logs[sheet_name] for sheet_name in validation_logs):
        try:
            with pd.ExcelWriter(output_file_name, engine='xlsxwriter') as writer:
                # Write validation logs to corresponding sheets
                for sheet_name, logs in validation_logs.items():
                    if logs:
                        validation_df = pd.DataFrame(logs)
                        validation_df.to_excel(writer, sheet_name=f"{sheet_name}", index=False)
                print(f"Validation results saved to {output_file_name}")
        except Exception as e:
            print(f"ERROR WHILE SAVING RESULT IN EXCEL {output_file_name}: {e}")
    else:
        print(f"No validation issues found for {output_file_name}. No file created.")

def load_file_data(file_path):
    """Helper function to load data from a single file."""
    sheets = load_excel_sheets(file_path)
    srsa_df = sheets["Supplier Risk Assessment Header"]
    return [str(x) for x in srsa_df["Contract ID*"].dropna().tolist()]

def load_questionNumber(file_path):
    """Helper function to load data from a single file."""
    sheets = load_excel_sheets(file_path)
    formResponse_df = sheets["Form Response"]
    return formResponse_df["Question Number*"].drop_duplicates().tolist()


def fetch_all_data(db, files):
    TOTAL_STEPS = 9

    def _step(n, label):
        print(f"\n[{n}/{TOTAL_STEPS}] {label}...", flush=True)
        return time.time()

    def _done(t0):
        print(f"  Done in {time.time() - t0:.1f}s", flush=True)

    # Step 1 — load contract IDs from Excel
    t0 = _step(1, "Loading contract IDs from input files")
    all_contract_ids = set()
    with ThreadPoolExecutor(max_workers=10) as executor:
        for contract_ids in executor.map(load_file_data, files):
            all_contract_ids.update(contract_ids)
    all_contract_ids = list(all_contract_ids)
    print(f"  -> Found {len(all_contract_ids)} unique contract IDs across {len(files)} file(s)")
    _done(t0)

    # Step 2 — Post-Contract SRSAs
    t0 = _step(2, "Fetching Post-Contract SRSA documents")
    srsa_docs = fetch_srsa_documents(db, all_contract_ids)
    srsa_doc_map = {doc["revisedContractNumber"]: doc for doc in srsa_docs}
    post_contract_counter = Counter(doc["revisedContractNumber"] for doc in srsa_docs)
    duplicate_post_contract_ids = {k for k, v in post_contract_counter.items() if v > 1}
    if duplicate_post_contract_ids:
        print(f"  -> WARNING: {len(duplicate_post_contract_ids)} contract ID(s) have duplicate Post-Contract SRSAs")
    _done(t0)

    # Step 3 — Pre-Contract SRSAs
    t0 = _step(3, "Fetching Pre-Contract SRSA documents")
    all_document_numbers = [doc["documentNumber"] for doc in srsa_docs if "documentNumber" in doc]
    pre_contract_srsa_docs = fetch_pre_contract_srsa_documents(db, all_document_numbers)
    pre_contract_srsa_doc_map = {doc["documentNumber"]: doc for doc in pre_contract_srsa_docs}
    pre_contract_counter = Counter(doc["documentNumber"] for doc in pre_contract_srsa_docs)
    duplicate_pre_contract_doc_numbers = {k for k, v in pre_contract_counter.items() if v > 1}
    pre_contract_srsa_ids = [doc["internalDocumentId"] for doc in pre_contract_srsa_docs]
    if duplicate_pre_contract_doc_numbers:
        print(f"  -> WARNING: {len(duplicate_pre_contract_doc_numbers)} document number(s) have duplicate Pre-Contract SRSAs")
    _done(t0)

    # Step 4 — Control Forms
    t0 = _step(4, "Fetching Control Forms")
    control_forms = fetch_control_forms(db, pre_contract_srsa_ids)
    _done(t0)

    # Step 5 — Form Responses
    t0 = _step(5, "Fetching Control Form Responses")
    form_responses = fetch_forms_responses(db, control_forms, RESPONSE_COLLECTION)
    _done(t0)

    # Step 6 — Question number mappings via API
    t0 = _step(6, "Fetching question number mappings from API")
    with ThreadPoolExecutor(max_workers=10) as executor:
        distinct_questionNumbers = set()
        for qn_list in executor.map(load_questionNumber, files):
            distinct_questionNumbers.update(qn_list)
    print(f"  -> {len(distinct_questionNumbers)} distinct question numbers to resolve", flush=True)
    questionMappings = apiCall(list(distinct_questionNumbers))
    api_available = questionMappings is not None
    if not api_available:
        print("  -> WARNING: API call failed — Form Response validation will be skipped")
    _done(t0)

    control_form_map = {}
    for form in control_forms:
        control_form_map.setdefault(form["supplierRSAId"], []).append(form)

    for form_response in form_responses:
        for questionnaire_detail in form_response.get("questionnaireDetails", []):
            for question in questionnaire_detail.get("questions", []):
                question_library_id = question.get("questionLibraryQuestionId")
                if questionMappings and question_library_id in questionMappings:
                    question["mappedQuestionId"] = questionMappings[question_library_id]

    # Step 7 — Aggregated RSAs
    t0 = _step(7, "Fetching Aggregated Risk Assessment documents")
    supplier_ids = list({doc["supplierId"] for doc in srsa_docs if "supplierId" in doc})
    aggregated_rsa_docs = fetch_aggregated_rsa_documents(db, SRSA_COLLECTION, supplier_ids)
    agg_rsa_counter = Counter(doc["supplierId"] for doc in aggregated_rsa_docs)
    duplicate_agg_rsa_supplier_ids = {k for k, v in agg_rsa_counter.items() if v > 1}
    aggregated_rsa_map = {}
    for doc in aggregated_rsa_docs:
        aggregated_rsa_map.setdefault(doc["supplierId"], []).append(doc)
    if duplicate_agg_rsa_supplier_ids:
        print(f"  -> WARNING: {len(duplicate_agg_rsa_supplier_ids)} supplier(s) have duplicate Aggregated RSAs")
    _done(t0)

    # Step 8 — Relationships + Relationship Forms
    t0 = _step(8, "Fetching Relationship documents and Relationship Risk Forms")
    relationships = fetch_relationships(db, RELATIONSHIP_COLLECTION, supplier_ids)
    existing_relationship_supplier_ids = {rel["supplierId"] for rel in relationships}
    aggregated_rsa_ids = [doc["internalDocumentId"] for doc in aggregated_rsa_docs]
    relationship_forms = fetch_relationship_forms(db, FORM_COLLECTION, aggregated_rsa_ids)
    relationship_form_map = {}
    for form in relationship_forms:
        relationship_form_map.setdefault(form["supplierRSAId"], []).append(form["sourceFormDocumentNumber"])
    _done(t0)

    # Step 9 — Manage Recurrences
    t0 = _step(9, "Fetching Manage Recurrence documents")
    post_contract_srsa_ids = [doc["internalDocumentId"] for doc in srsa_docs]
    recurrence_docs = fetch_manage_recurrences(db, RECURRENCE_COLLECTION, post_contract_srsa_ids)
    recurrence_map = {}
    for rec in recurrence_docs:
        recurrence_map.setdefault(rec["rsaInternalDocumentId"], []).append(rec)
    _done(t0)

    return (
        srsa_doc_map, pre_contract_srsa_doc_map, control_form_map,
        aggregated_rsa_map, existing_relationship_supplier_ids,
        relationship_form_map, recurrence_map, form_responses,
        duplicate_post_contract_ids, duplicate_pre_contract_doc_numbers,
        duplicate_agg_rsa_supplier_ids, api_available,
    )

def process_file(excel_file, srsa_doc_map, pre_contract_srsa_doc_map, control_form_map,
                 aggregated_rsa_map, existing_relationship_supplier_ids,
                 relationship_form_map, recurrence_map, form_responses,
                 duplicate_post_contract_ids, duplicate_pre_contract_doc_numbers,
                 duplicate_agg_rsa_supplier_ids, api_available):
    """Process a single Excel file."""
    print(f"Processing file: {excel_file}")
    sheets = load_excel_sheets(excel_file)
    srsa_df             = sheets["Supplier Risk Assessment Header"]
    risk_char_df        = sheets["Risk Characteristics"]
    relationship_df     = sheets["Relationship"]
    kra_df              = sheets["Key Risk Attributes"]
    form_df             = sheets["Form Details"]
    response_df         = sheets["Form Response"]
    applicable_forms_df = sheets["Applicable Control Forms"]

    # Validate data for the current file
    validation_logs = validate_data(
        srsa_df, risk_char_df, relationship_df, kra_df,
        form_df, response_df, applicable_forms_df,
        srsa_doc_map, pre_contract_srsa_doc_map,
        control_form_map, aggregated_rsa_map,
        existing_relationship_supplier_ids, relationship_form_map,
        recurrence_map, form_responses,
        duplicate_post_contract_ids, duplicate_pre_contract_doc_numbers,
        duplicate_agg_rsa_supplier_ids, api_available,
    )
    
    output_folder = "Validation Result"
    os.makedirs(output_folder, exist_ok=True)

    # Save validation results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_file_name = os.path.splitext(os.path.basename(excel_file))[0]
    output_file_name = os.path.join(output_folder, f"{input_file_name}_ValidationResult_{timestamp}.xlsx")
    save_validation_results(output_file_name, sheets, validation_logs)

    print(f"Finished processing file: {excel_file}")

# --- Main Script ---
def main():
    start_time = time.time()  # Record the start time
    db = connect_to_db()
    files = select_files()

    print("Fetching all data...")
    (srsa_doc_map, pre_contract_srsa_doc_map, control_form_map,
     aggregated_rsa_map, existing_relationship_supplier_ids,
     relationship_form_map, recurrence_map, form_responses,
     duplicate_post_contract_ids, duplicate_pre_contract_doc_numbers,
     duplicate_agg_rsa_supplier_ids, api_available) = fetch_all_data(db, files)
    print(f"Data fetching completed in {time.time() - start_time:.2f} seconds.")

    batch_size = 5  # Number of files to process in each batch
    file_batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

    print("Processing files...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        for batch in file_batches:
            executor.map(
                process_file, batch,
                [srsa_doc_map] * len(batch),
                [pre_contract_srsa_doc_map] * len(batch),
                [control_form_map] * len(batch),
                [aggregated_rsa_map] * len(batch),
                [existing_relationship_supplier_ids] * len(batch),
                [relationship_form_map] * len(batch),
                [recurrence_map] * len(batch),
                [form_responses] * len(batch),
                [duplicate_post_contract_ids] * len(batch),
                [duplicate_pre_contract_doc_numbers] * len(batch),
                [duplicate_agg_rsa_supplier_ids] * len(batch),
                [api_available] * len(batch),
            )
    print(f"File processing completed in {time.time() - start_time:.2f} seconds.")

    end_time = time.time()  # Record the end time
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
   

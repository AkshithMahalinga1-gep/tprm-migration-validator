import requests
from API_Details import neo4j_api
from pymongo import MongoClient

BATCH_SIZE = 200

def _batched(ids, batch_size=BATCH_SIZE, label=None):
    """Split a list into chunks of batch_size, printing batch progress if label is given."""
    total = max(1, (len(ids) + batch_size - 1) // batch_size)
    for batch_num, i in enumerate(range(0, len(ids), batch_size), 1):
        if label:
            print(f"  {label}: batch {batch_num}/{total}", end="\r", flush=True)
        yield ids[i:i + batch_size]
    if label and total > 1:
        print()  # clear the \r line

# --- Fetch Control Forms Responses ---
def fetch_forms_responses(db, forms, RESPONSE_COLLECTION):
    form_ids = [doc["internalDocumentId"] for doc in forms]
    control_forms_responses = []
    for batch in _batched(form_ids, label="Control Form Responses"):
        batch_results = list(db[RESPONSE_COLLECTION].find(
            {
                "internalDocumentId": {"$in": batch},
                "isLive": True,
            },
            {"internalDocumentId": 1, "documentNumber": 1, "questionnaireDetails.questions.questionId": 1, "questionnaireDetails.questions.questionLibraryQuestionId": 1, "questionnaireDetails.questions.responseValue": 1, "questionnaireDetails.questions.responseAttachment": 1},
        ).batch_size(100))
        control_forms_responses.extend(batch_results)
    print(f"  -> Fetched {len(control_forms_responses)} Control Forms Responses")
    return control_forms_responses

def fetch_aggregated_rsa_documents(db, SRSA_COLLECTION, supplier_ids):
    """Fetch Aggregated Risk Assessments (riskAssessmentType.code='4') for the given supplier IDs."""
    docs = []
    for batch in _batched(supplier_ids, label="Aggregated RSA documents"):
        docs.extend(list(db[SRSA_COLLECTION].find(
            {
                "supplierId": {"$in": batch},
                "riskAssessmentType.code": "4",
                "isDeleted": False,
            },
            {
                "internalDocumentId": 1,
                "supplierId": 1,
                "riskProfile.riskScoreLevel.riskScoreRating": 1,
                "riskProfile.overallScore": 1,
            }
        ).batch_size(100)))
    print(f"  -> Fetched {len(docs)} Aggregated RSA documents")
    return docs

def fetch_relationships(db, RELATIONSHIP_COLLECTION, supplier_ids):
    """Fetch Relationship documents from relationship_1667773947 for the given supplier IDs."""
    docs = []
    for batch in _batched(supplier_ids, label="Relationship documents"):
        docs.extend(list(db[RELATIONSHIP_COLLECTION].find(
            {
                "supplierId": {"$in": batch},
                "relationshipType.code": "5",
            },
            {"supplierId": 1}
        ).batch_size(100)))
    print(f"  -> Fetched {len(docs)} Relationship documents")
    return docs

def fetch_relationship_forms(db, FORM_COLLECTION, aggregated_rsa_ids):
    """Fetch Relationship Risk Forms (isVLRA=true) linked to Aggregated RSAs via supplierRsaId."""
    forms = []
    for batch in _batched(aggregated_rsa_ids, label="Relationship Forms"):
        forms.extend(list(db[FORM_COLLECTION].find(
            {
                "supplierRSAId": {"$in": batch},
                "formType": 8,
                "isDeleted": False,
            },
            {"sourceFormDocumentNumber": 1, "supplierRSAId": 1, "internalDocumentId": 1}
        ).batch_size(100)))
    print(f"  -> Fetched {len(forms)} Relationship Forms")
    return forms

def fetch_manage_recurrences(db, RECURRENCE_COLLECTION, srsa_internal_ids):
    """Fetch Manage Recurrence documents for Applicable Control Forms validation."""
    docs = []
    for batch in _batched(srsa_internal_ids, label="Manage Recurrence documents"):
        docs.extend(list(db[RECURRENCE_COLLECTION].find(
            {
                "rsaInternalDocumentId": {"$in": batch},
                "isDeleted": False,
            },
            {
                "masterFormNumber": 1,
                "rsaInternalDocumentId": 1,
                "recurrenceDetails.recurrenceFromOcurrence": 1,
                "recurrenceDetails.recurrenceType": 1,
                "recurrenceCycleDetails.nextLaunchDate": 1,
            }
        ).batch_size(100)))
    print(f"  -> Fetched {len(docs)} Manage Recurrence documents")
    return docs

def apiCall(distinct_questionNumbers):
    url = neo4j_api["url"]
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json-patch+json",
        "Authorization": neo4j_api["Authorization"],
        }
    payload = {
        "ClientId": "70022785",
        "AppId": "1090",
        "PluginId": "1",
        "PluginVersion": "1",
        "OperationName": "GetMasterFormQuestionsAndResponses",
        "Version": "1",
        "Variables": {
            "documentNumber": distinct_questionNumbers
        },
        "TransactionId": "955806cc-e968-44af-a766-ca58c79ab538",
        "IsRetry": True,
        "DacThumbprint": None,
        "QueryResolverSettings": {
            "BaseUrl": None,
            "AcsAppClientId": None,
            "JWToken": None,
            "TransactionScopeId": None
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        print(f"  -> API Error ({response.status_code}): token may be expired — update token.txt and re-run")
        return None
    response_data = response.json()
    question_mapping = {item['questionId']: item['questionNumber'] for item in response_data.get('ouputData', [])}
    print(f"  -> API returned {len(question_mapping)} question mappings")
    return question_mapping

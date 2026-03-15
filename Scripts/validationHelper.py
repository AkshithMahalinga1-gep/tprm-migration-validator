import json

# --- Fetch Control Forms ---
def validate_response_data(validation_logs, formExcel, matching_form_mongo, response_df_excel, form_responses_mongo):
    # Filter rows from response_df_excel based on the criteria
    
    filtered_formResponse_Excel_rows = response_df_excel[
        (response_df_excel["Reference ID*"] == formExcel["Reference ID*"]) &
        (response_df_excel["Form Recurrence ID"] == formExcel["Form Recurrence ID"])
    ]

    # print("Form Mongo", matching_form_mongo)
    # print("Filtered Excel Rows", filtered_formResponse_Excel_rows)

    if not filtered_formResponse_Excel_rows.empty and filtered_formResponse_Excel_rows["Master Form ID*"].iloc[0] == matching_form_mongo["sourceFormDocumentNumber"]:
        filtered_formResponse_Excel_rows["internalDocumentId"] = matching_form_mongo["internalDocumentId"]

    # print("Updated Filtered Excel Rows:")
    # print(filtered_formResponse_Excel_rows)


    for _, row in filtered_formResponse_Excel_rows.iterrows():
        # Get the internalDocumentId from the row
        internal_document_id = row.get("internalDocumentId")

        # Skip if internalDocumentId is None
        if internal_document_id is None:
            continue

        # Loop through form_responses_mongo to find a match
        for form_response in form_responses_mongo:
            if form_response["internalDocumentId"] == internal_document_id:
                # Check questionnaireDetails and questions for matching conditions
                for questionnaire_detail in form_response.get("questionnaireDetails", []):
                    for question in questionnaire_detail.get("questions", []):
                        if (question.get("mappedQuestionId") == row["Question Number*"]):
                            if row["Response*"] not in question.get("responseValue", []):
                                validation_logs["Form Response"].append({
                                    "formRecurrenceId": row["Form Recurrence ID"],
                                    "referenceId": row["Reference ID*"],
                                    "masterFormId": row["Master Form ID*"],
                                    "response": row["Response*"],
                                    "internalDocumentId": internal_document_id,
                                    "mongoDetail": question,
                                })

  
    # Save validation_logs to a new JSON file
    # with open("validation_logs_output.json", "w", encoding="utf-8") as f:
    #     json.dump(validation_logs, f, ensure_ascii=False, indent=4)
    return validation_logs
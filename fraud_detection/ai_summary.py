import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

genai.configure(api_key="YOUR_API_KEY")

def generate_advanced_audit_report(results):
    # 1. Define the System Instruction to set the AI's "persona"
    system_prompt = (
        "You are a Senior Forensic Auditor. Your task is to analyze anomaly data "
        "and provide highly structured, professional risk assessments. "
        "Focus on financial integrity, internal control weaknesses, and fraud detection."
    )

    # 2. Initialize the model with the system instruction
    model = genai.GenerativeModel(
        model_name="gemini-1.5-pro", # Using the latest 1.5 Pro for better reasoning
        system_instruction=system_prompt
    )

    # 3. Create a more detailed prompt with specific context
    # We pass actual counts and perhaps a snippet of the data if available
    user_prompt = f"""
    AUDIT DATASET SUMMARY:
    - Total Employee Anomalies: {len(results.get("employee", []))}
    - Total Departmental Discrepancies: {len(results.get("department", []))}
    - High-Risk Goods/Inventory Alerts: {len(results.get("goods", []))}

    TASK:
    Analyze these figures against standard internal control benchmarks. 
    Provide a report with the following Markdown sections:
    ## 1. Executive Summary
    ## 2. Statistical Risk Profile (Low/Medium/High)
    ## 3. Potential Fraud Vectors
    ## 4. Immediate Remediation Steps
    """

    # 4. Add Safety Settings to prevent the model from refusing sensitive analysis
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
    }

    # 5. Generation Config for more focused output
    config = genai.GenerationConfig(
        temperature=0.2,  # Lower temperature = more precise and less 'creative'
        top_p=0.8,
        max_output_tokens=1000,
    )

    try:
        response = model.generate_content(
            user_prompt,
            generation_config=config,
            safety_settings=safety_settings
        )
        return response.text
    except Exception as e:
        return f"Error generating audit report: {str(e)}"

# Example Usage
# results = {"employee": [1, 2], "department": [], "goods": [1, 2, 3, 4, 5]}
# print(generate_advanced_audit_report(results))
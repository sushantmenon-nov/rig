import streamlit as st
import os
import pandas as pd
from openai import AzureOpenAI
from oracle_utils import get_bom_structure, get_latest_purchase_orders

# --- Setup ---
def setup_azure_openai_client():
    try:
        return AzureOpenAI(
            azure_endpoint="https://askdaviddemo4082360630.openai.azure.com/openai/deployments/gpt-4o/chat/completions?api-version=2024-08-01-preview",
            api_key=os.getenv("AZURE_API_KEY_NEW"),
            api_version="2024-10-01-preview",
        )
    except Exception as e:
        print("Failed to setup Azure OpenAI client:", e)
        return None

# --- Helper Functions ---
def normalize_component(comp: str) -> str:
    return comp.strip().replace("-  ", "").lstrip("-").strip()

def estimate_percentage(row):
    return "8%" if row["B/M"] == "Make" else "15%" if row["B/M"] == "Buy" else ""

def highlight_extended_cost_level_1(row):
    if row["Level"] == 1:
        return ['background-color: lightblue' if col == "Extended Cost" else '' for col in row.index]
    elif row["Level"] == 0:
        return ['background-color: yellow' if col == "Extended Cost" else '' for col in row.index]
    else:
        return ['' for _ in row]

# --- BOM Hierarchy Construction ---
def build_bom_hierarchy(df):
    df = df[(df["Unit Cost"] > 0) & (df["Component Quantity"] > 0)].copy()
    df["Extended Cost"] = df["Component Quantity"] * df["Unit Cost"]
    df["B/M"] = df["B/M"].map({1: "Make", 2: "Buy"}).fillna("Unknown")
    top_items = df[df["Level"] == 1]["Item"].unique()
    hierarchy_rows = []

    def recurse(parent, level):
        children = df[df["Item"] == parent]
        for _, row in children.iterrows():
            row_data = row.copy()
            row_data["Display Component"] = f"{'    ' * level}-  {row['Component']}"
            hierarchy_rows.append(row_data)
            if level <= 2:
                recurse(row["Component"], level + 1)

    for top in top_items:
        pseudo_row = pd.Series({
            "Level": 0, "Item": None, "Component": top, "Component Description": "Top-Level Assembly",
            "Component Quantity": None, "Unit Cost": df[df["Item"] == top]["Unit Cost"].sum(),
            "Extended Cost": df[df["Item"] == top]["Extended Cost"].sum(), "Parent": None,
            "Display Component": top
        })
        hierarchy_rows.append(pseudo_row)
        recurse(top, 1)

    return pd.DataFrame(hierarchy_rows)

# --- Prompt Building ---
def build_prompt_from_group(group):
    item_id = group.iloc[0]['Component']
    item_desc = group.iloc[0]['Component Description']
    records = [
        "- " + " | ".join(f"{k}: {v}" for k, v in row.dropna().items())
        for _, row in group.iterrows()
    ]
    details = "\n".join(records)
    return f"""
    You are a pricing analyst AI. Your task is to analyze component-level estimates and calculate a reasonable total estimated cost for an assembly item.

    The Level 0 item is: {item_id}
    Description: {item_desc}

    Below are the subcomponents with all available columns:
    {details}

    Notes:
    - The cost estimation is based on summing: Extended Cost + Ext Delta for each row.
    - The cost is already calculated hierarchically. To estimate the total cost for Level 0, sum all Level 1 items only.
    - Provide a range estimate, concise reasoning, and any assumptions.
    - Adjust the price based on time inflation given the column \"Percentage Est\".
    - Use equation: "Last PO Price" * (1 + "Year Past")^"Percentage Est".
    - If PO is missing, fallback to Unit Cost.
    """

# --- LLM Call ---
def query_llm(prompt):
    client = setup_azure_openai_client()
    response = client.chat.completions.create(
        temperature=0,
        model="gpt-4o-pib",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content.replace("$", "\\$")

# --- Streamlit UI ---
st.set_page_config(page_title="BOM Hierarchy Viewer", layout="wide")
st.title("🔍 BOM Hierarchical Viewer with Cost Estimation")

item_number = st.text_input("Enter Assembly Item Number (e.g., 10985296-001):")

if st.button("Search BOM"):
    if not item_number:
        st.warning("Please enter a valid item number.")
    else:
        try:
            with st.spinner("Fetching BOM and PO data..."):
                df = get_bom_structure(item_number)
                for i in range(1, 4):
                    df[f"PO {i}"] = df[f"PO Date {i}"] = df[f"Unit Price {i}"] = ""

                components = df["Component"].dropna().astype(str).str.strip().unique()
                po_info_dict = {}
                for component in components:
                    clean = normalize_component(component)
                    if clean not in po_info_dict:
                        po_info_dict[clean] = get_latest_purchase_orders(clean)

                for i, row in df.iterrows():
                    comp = normalize_component(str(row["Component"]))
                    po_data = po_info_dict.get(comp)
                    if po_data is not None and not po_data.empty:
                        for idx, (_, po_row) in enumerate(po_data.iterrows()):
                            idx1 = idx + 1
                            df.at[i, f"PO {idx1}"] = po_row.get("po_number", "")
                            df.at[i, f"PO Date {idx1}"] = po_row.get("last_receipt_date", "")
                            df.at[i, f"Unit Price {idx1}"] = po_row.get("last_unit_price", "")

                if df.empty:
                    st.info("No data found.")
                else:
                    st.success(f"{len(df)} rows retrieved.")
                    df_result = build_bom_hierarchy(df).reset_index(drop=True)
                    df_result["Percentage Est"] = df_result.apply(estimate_percentage, axis=1)
                    df_result = df_result[df_result["Level"].isin([0, 1])].copy()
                    df_result["Component Quantity"] = df_result["Component Quantity"].fillna(0).astype(int)

                    for col in ["Unit Cost", "Extended Cost", "Unit Price 1", "Unit Price 2", "Unit Price 3"]:
                        df_result[col] = pd.to_numeric(df_result[col], errors="coerce").round(2)

                    styled_df = (
                        df_result[[
                            "Level", "Display Component", "Component Description", "Component Quantity", "B/M",
                            "Unit Cost", "Extended Cost", "PO 1", "PO Date 1", "Unit Price 1",
                            "PO 2", "PO Date 2", "Unit Price 2", "PO 3", "PO Date 3", "Unit Price 3", "Percentage Est"
                        ]]
                        .style
                        .apply(highlight_extended_cost_level_1, axis=1)
                        .format({
                            "Unit Cost": "{:.2f}", "Extended Cost": "{:.2f}",
                            "Unit Price 1": "{:.2f}", "Unit Price 2": "{:.2f}", "Unit Price 3": "{:.2f}"
                        })
                    )

                    st.dataframe(styled_df, use_container_width=True)
                    st.subheader("📊 LLM-Based Cost Analysis")

                    level_0_indices = df_result[df_result["Level"] == 0].index.tolist() + [len(df_result)]
                    level_0_groups = [df_result.iloc[level_0_indices[i]:level_0_indices[i+1]] for i in range(len(level_0_indices) - 1)]

                    for group in level_0_groups:
                        prompt = build_prompt_from_group(group)
                        with st.spinner(f"Analyzing: {group.iloc[0]['Component']}..."):
                            response = query_llm(prompt)
                        with st.expander(f"Cost Analysis for {group.iloc[0]['Component']}"):
                            st.markdown(response)

        except Exception as e:
            st.error(f"Error: {e}")

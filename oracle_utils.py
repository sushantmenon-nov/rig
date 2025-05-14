import pandas as pd
import oracledb
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Oracle client
oracle_client_path = "instantclient-basic-linux.x64-23.8.0.25.04/instantclient_23_8"
oracledb.init_oracle_client(lib_dir=oracle_client_path)

def create_connection():
    """Establish Oracle DB connection using environment variables."""
    dsn = f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE_NAME')}"
    return oracledb.connect(
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        dsn=dsn
    )

def get_bom_structure(item_number: str) -> pd.DataFrame:
    """
    Retrieves the hierarchical Bill of Materials for a given assembly item number.
    Filters for effective components and includes cost and description data.
    """
    query = f"""
    SELECT
        LEVEL AS "Level",
        xbev.assembly_item AS "Item",
        xbev.to_level_item_desc AS "Description",
        xbev.component_item AS "Component",
        xbev.make_buy_code AS "B/M",
        xbev.component_description AS "Component Description",
        ROUND(xbev.component_quantity) AS "Component Quantity",
        ROUND(xbev.item_cost, 2) AS "Unit Cost"
    FROM (
        SELECT DISTINCT
            bom.assembly_item_id,
            msib.segment1 AS assembly_item,
            ood.organization_code AS organization_code_assembly,
            ood2.organization_code AS organization_code_component,
            bic.component_item_id,
            bic.component_quantity,
            msib.description AS to_level_item_desc,
            msib2.description AS component_description,
            msib2.planning_make_buy_code AS make_buy_code,
            TO_CHAR(msib.creation_date, 'DD-MON-YYYY') AS to_level_item_creation_date,
            TO_CHAR(msib2.creation_date, 'DD-MON-YYYY') AS item_creation_date,
            msib2.segment1 AS component_item,
            cic.item_cost
        FROM
            apps.bom_components_b bic
            JOIN apps.bom_structures_b bom ON bic.bill_sequence_id = bom.bill_sequence_id AND bic.bill_sequence_id = source_bill_sequence_id
            JOIN apps.mtl_system_items_b msib ON bom.assembly_item_id = msib.inventory_item_id AND bom.organization_id = msib.organization_id
            JOIN apps.mtl_system_items_b msib2 ON bic.component_item_id = msib2.inventory_item_id AND bom.organization_id = msib2.organization_id
            JOIN apps.cst_item_costs cic ON cic.inventory_item_id = msib2.inventory_item_id AND cic.organization_id = msib2.organization_id AND cic.cost_type_id = 1
            JOIN apps.org_organization_definitions ood ON ood.organization_id = msib.organization_id
            JOIN apps.org_organization_definitions ood2 ON ood2.organization_id = msib2.organization_id
        WHERE
            SYSDATE BETWEEN bic.effectivity_date AND NVL(bic.disable_date, SYSDATE)
    ) xbev
    START WITH xbev.assembly_item = '{item_number}'
    CONNECT BY NOCYCLE PRIOR xbev.component_item = xbev.assembly_item
    ORDER BY LEVEL
    """
    with create_connection().cursor() as cursor:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns).drop_duplicates()

def get_latest_purchase_orders(item_number: str) -> pd.DataFrame:
    """
    Retrieves the latest 3 purchase orders for a given item_number.
    Includes PO number, vendor, receipt date, and unit price.
    """
    query = f"""
    SELECT * FROM (
        SELECT
            msi.segment1 AS item_number,
            ph.segment1 AS po_number,
            MAX(rt.transaction_date) AS last_receipt_date,
            MAX(rt.po_unit_price) AS last_unit_price,
            sup.vendor_name,
            DENSE_RANK() OVER (ORDER BY MAX(rt.transaction_date) DESC) AS rank_by_po
        FROM
            INV.mtl_system_items_b msi
            JOIN PO.po_lines_all pol ON msi.inventory_item_id = pol.item_id
            JOIN PO.rcv_transactions rt ON rt.po_line_id = pol.po_line_id
            JOIN PO.po_headers_all ph ON rt.po_header_id = ph.po_header_id
            JOIN AP.ap_suppliers sup ON ph.vendor_id = sup.vendor_id
        WHERE
            msi.segment1 = '{item_number}'
            AND rt.transaction_type = 'RECEIVE'
            AND rt.po_unit_price IS NOT NULL
        GROUP BY
            msi.segment1, ph.segment1, sup.vendor_name
    ) WHERE rank_by_po <= 3
    ORDER BY last_receipt_date DESC
    """
    with create_connection().cursor() as cursor:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns).rename(columns=str.lower)

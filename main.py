import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

# ==================== CONFIGURATION ====================

PORT = int(os.getenv("PORT", 8000))
HOST = "0.0.0.0"

# ==================== GLOBAL DATA ====================

df_global = pd.DataFrame()
df_landed_cost = pd.DataFrame()
df_billable_type = pd.DataFrame()
df_model_group = pd.DataFrame()
ro_remarks_list = []  # Standard RO Remarks


# ==================== LOADERS ====================

def load_ro_remarks_dynamically():
    """Load RO remarks dynamically from Excel file - returns latest remarks"""
    try:
        remark_file = None
        for fn in ["RO Remark.xlsx", "RO_Remark.xlsx", "ro_remark.xlsx"]:
            if os.path.exists(fn):
                remark_file = fn
                break

        if remark_file:
            df_remarks = pd.read_excel(remark_file)
            remarks_col = df_remarks.columns[0]
            remarks = [str(x).strip() for x in df_remarks[remarks_col].dropna().unique()]
            return remarks
        return []
    except Exception as e:
        print(f"⚠ Error loading RO remarks dynamically: {e}")
        return []


def map_ro_remark(remark):
    """
    Map RO Remarks to standard codes (case-insensitive)
    Searches for any standard remark code within the remarks text
    Returns the first match found, or "Not Assigned" if no match
    """
    global ro_remarks_list

    if pd.isna(remark) or str(remark).strip() in ["", "-"]:
        return "Not Assigned"

    # Load latest remarks from file
    remarks_list = load_ro_remarks_dynamically()
    if not remarks_list:
        remarks_list = ro_remarks_list  # fallback

    remark_str = str(remark).strip().upper()

    for standard_remark in remarks_list:
        if str(standard_remark).strip() == "":
            continue
        standard_upper = str(standard_remark).strip().upper()
        if standard_upper in remark_str:
            return str(standard_remark).strip()

    return "Not Assigned"


def get_all_ro_remarks_for_dropdown():
    """Get all RO remarks for dropdown - both standard and those found in data"""
    try:
        standard_remarks = load_ro_remarks_dynamically()

        if df_global is not None and not df_global.empty and "ro_remark_mapped" in df_global.columns:
            mapped_remarks = [
                str(x) for x in df_global["ro_remark_mapped"].dropna().unique()
                if str(x).strip() != "" and str(x) != "Not Assigned"
            ]
            all_remarks = list(set(standard_remarks + mapped_remarks))
            return sorted(all_remarks)
        return sorted(standard_remarks) if standard_remarks else ["All"]
    except Exception as e:
        print(f"⚠ Error getting all RO remarks: {e}")
        return ["All"]


def load_data():
    """Load Excel files and merge data"""
    global df_global, df_landed_cost, df_billable_type, df_model_group, ro_remarks_list

    try:
        # ==================== Load Standard RO Remarks ====================
        print("[OK] Loading standard RO Remarks list...")
        remark_file = None
        for fn in ["RO Remark.xlsx", "RO_Remark.xlsx", "ro_remark.xlsx"]:
            if os.path.exists(fn):
                remark_file = fn
                break

        if remark_file:
            print(f"[OK] Loading: {remark_file}")
            df_remarks = pd.read_excel(remark_file)
            remarks_col = df_remarks.columns[0]
            ro_remarks_list = [str(x).strip() for x in df_remarks[remarks_col].dropna().unique()]
            print(f"[OK] Loaded {len(ro_remarks_list)} standard RO Remarks")
        else:
            print("⚠ RO Remark file not found - RO Remark mapping will not be available")
            ro_remarks_list = []

        # ==================== Load Model Group mapping ====================
        model_file = None
        for fn in ["Model Group.xlsx", "Model_Group.xlsx", "model_group.xlsx"]:
            if os.path.exists(fn):
                model_file = fn
                break

        if model_file:
            print(f"[OK] Loading: {model_file}")
            df_model_group = pd.read_excel(model_file)
            print(f"[OK] Loaded {len(df_model_group)} model group records")
        else:
            print("⚠ Model Group Excel file not found - Model Group and Segment will not be enriched")
            df_model_group = pd.DataFrame()

        # ==================== Load Open RO data ====================
        excel_file = None
        for fn in ["Open RO.xlsx", "Open_RO.xlsx", "open_ro.xlsx"]:
            if os.path.exists(fn):
                excel_file = fn
                break

        if excel_file is None:
            print("⚠ Open RO Excel file not found")
            df_global = pd.DataFrame()
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
            return

        print(f"[OK] Loading: {excel_file}")
        df_global = pd.read_excel(excel_file)
        print(f"[OK] Loaded {len(df_global)} rows, {len(df_global.columns)} cols")

        # Ensure essential columns exist to avoid crashes
        for col in ["RO Date", "RO ID", "Branch", "RO Status", "Age Bucket"]:
            if col not in df_global.columns:
                df_global[col] = None

        # ==================== Add RO Remark Mapping Column ====================
        if "RO Remarks" not in df_global.columns:
            df_global["RO Remarks"] = "-"
        print("[OK] Mapping RO Remarks...")
        df_global["ro_remark_mapped"] = df_global["RO Remarks"].apply(map_ro_remark)
        print("[OK] RO Remark mapping complete")

        # ==================== Merge Model Group data if available ====================
        if not df_model_group.empty and "Model Group" in df_global.columns:
            try:
                if "Model Code" in df_model_group.columns and "Segment" in df_model_group.columns and "Model Group" in df_model_group.columns:
                    segment_mapping = dict(zip(df_model_group["Model Code"], df_model_group["Segment"]))
                    mg_mapping = dict(zip(df_model_group["Model Code"], df_model_group["Model Group"]))

                    df_global["segment"] = df_global["Model Group"].map(segment_mapping)
                    df_global["model_group_mapped"] = df_global["Model Group"].map(mg_mapping)

                    df_global["segment"] = df_global["segment"].fillna("Unknown")
                    df_global["model_group_mapped"] = df_global["model_group_mapped"].fillna(df_global["Model Group"])
                    df_global["Model Group"] = df_global["model_group_mapped"]
                else:
                    df_global["segment"] = "Unknown"
            except Exception as e:
                print(f"⚠ Error during model mapping: {e}")
                df_global["segment"] = "Unknown"
        else:
            if "segment" not in df_global.columns:
                df_global["segment"] = "Unknown"

        # ==================== Load and aggregate Landed Cost data ====================
        parts_file = None
        for fn in ["Part Issue But Not Bill.xlsx", "Part_Issue_But_Not_Bill.xlsx", "part_issue_but_not_bill.xlsx"]:
            if os.path.exists(fn):
                parts_file = fn
                break

        if parts_file is None:
            print("⚠ Part Issue file not found - Landed Cost and Billable Type will not be available")
            df_global["total_landed_cost"] = 0.0
            df_global["billable_type"] = "Not Billed"
            return

        print(f"[OK] Loading: {parts_file}")
        df_parts = pd.read_excel(parts_file)
        print(f"[OK] Loaded {len(df_parts)} part records")

        # Aggregate Landed Cost by RO Number
        if "RO Number" in df_parts.columns and "Landed Cost (Total)" in df_parts.columns:
            df_landed_cost = df_parts.groupby("RO Number")["Landed Cost (Total)"].sum().reset_index()
            df_landed_cost.columns = ["RO ID", "total_landed_cost"]
        else:
            df_landed_cost = pd.DataFrame(columns=["RO ID", "total_landed_cost"])

        # Extract Billable Type by RO Number
        if "RO Number" in df_parts.columns and "Billable Type" in df_parts.columns:
            df_billable_type = df_parts.groupby("RO Number")["Billable Type"].first().reset_index()
            df_billable_type.columns = ["RO ID", "billable_type"]
        else:
            df_billable_type = pd.DataFrame(columns=["RO ID", "billable_type"])

        # Merge with main dataframe
        df_global = df_global.merge(df_landed_cost, on="RO ID", how="left")
        df_global["total_landed_cost"] = df_global["total_landed_cost"].fillna(0)

        df_global = df_global.merge(df_billable_type, on="RO ID", how="left")
        df_global["billable_type"] = df_global["billable_type"].fillna("Not Billed")

        print("[OK] Merged landed cost and billable type data into main dataframe")

    except Exception as e:
        print(f"[ERROR] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        df_global = pd.DataFrame()
        df_landed_cost = pd.DataFrame()
        df_billable_type = pd.DataFrame()


# ==================== HELPERS ====================

def parse_date(date_str):
    """Parse date string in various formats (YYYY-MM-DD, DD/MM/YYYY, etc.)"""
    if not date_str or pd.isna(date_str):
        return None

    date_str = str(date_str).strip()

    try:
        return pd.to_datetime(date_str, format="%Y-%m-%d", errors="raise")
    except:
        pass

    try:
        return pd.to_datetime(date_str, format="%d/%m/%Y", errors="raise")
    except:
        pass

    try:
        return pd.to_datetime(date_str, errors="coerce")
    except:
        return None


def extract_mjobs(remark):
    """Extract MJob codes from remarks"""
    if pd.isna(remark) or str(remark).strip() in ["-", ""]:
        return None
    remark_str = str(remark).strip()
    matches = re.findall(r"\bM/?[1-4]\b", remark_str)
    return matches if matches else None


def sort_by_ro_date(df: pd.DataFrame, ascending: bool):
    """
    Correct RO Date sorting:
    - Descending => latest (current) date first (top)
    - Ascending  => oldest date first (top)
    """
    if df is None or df.empty:
        return df

    if "RO Date" not in df.columns:
        return df

    tmp = df.copy()
    tmp["_ro_date_dt"] = pd.to_datetime(tmp["RO Date"], errors="coerce", dayfirst=True)
    # push NaT always last
    tmp["_ro_date_nat"] = tmp["_ro_date_dt"].isna().astype(int)
    tmp = tmp.sort_values(by=["_ro_date_nat", "_ro_date_dt"], ascending=[True, ascending])
    tmp = tmp.drop(columns=["_ro_date_dt", "_ro_date_nat"], errors="ignore")
    return tmp


def convert_row(row) -> Dict[str, Any]:
    """Convert pandas row to JSON-safe dict"""
    def safe_date_parse(date_value):
        if pd.notna(date_value) and str(date_value).strip() not in ["-", "", "nan", "NaT"]:
            try:
                # Keep YYYY-MM-DD for JS sorting
                return pd.to_datetime(date_value, errors="coerce").strftime("%Y-%m-%d")
            except:
                return "-"
        return "-"

    return {
        "ro_id": str(row.get("RO ID", "-")).strip() if pd.notna(row.get("RO ID", None)) else "-",
        "branch": str(row.get("Branch", "-")).strip() if pd.notna(row.get("Branch", None)) else "-",
        "ro_status": str(row.get("RO Status", "-")).strip() if pd.notna(row.get("RO Status", None)) else "-",
        "age_bucket": str(row.get("Age Bucket", "-")).strip() if pd.notna(row.get("Age Bucket", None)) else "-",
        "service_category": str(row.get("SERVC_CATGRY_DESC", "-")).strip() if pd.notna(row.get("SERVC_CATGRY_DESC", None)) else "-",
        "service_type": str(row.get("SERVC_TYPE_DESC", "-")).strip() if pd.notna(row.get("SERVC_TYPE_DESC", None)) else "-",
        "vehicle_model": str(row.get("Family", "-")).strip() if pd.notna(row.get("Family", None)) else "-",
        "model_group": str(row.get("Model Group", "-")).strip() if pd.notna(row.get("Model Group", None)) else "-",
        "segment": str(row.get("segment", "Unknown")).strip() if pd.notna(row.get("segment", None)) else "Unknown",
        "reg_number": str(row.get("Reg. Number", "-")).strip() if pd.notna(row.get("Reg. Number", None)) else "-",
        "ro_date": safe_date_parse(row.get("RO Date", None)),
        "vehicle_ready_date": safe_date_parse(row.get("Vehicle  Ready Date", None)),
        "ro_remarks": str(row.get("RO Remarks", "-")).strip() if pd.notna(row.get("RO Remarks", None)) else "-",
        "ro_remark_mapped": str(row.get("ro_remark_mapped", "Not Assigned")).strip()
        if pd.notna(row.get("ro_remark_mapped", None)) else "Not Assigned",
        "km": int(row.get("KM", 0)) if pd.notna(row.get("KM", None)) else 0,
        "days": int(row.get("Days", 0)) if pd.notna(row.get("Days", None)) else 0,
        "days_open": int(row.get("[No of Visits (In last 90 days)]", 0)) if pd.notna(row.get("[No of Visits (In last 90 days)]", None)) else 0,
        "service_adviser": str(row.get("Service Adviser Name", "-")).strip() if pd.notna(row.get("Service Adviser Name", None)) else "-",
        "vin": str(row.get("VIN", "-")).strip() if pd.notna(row.get("VIN", None)) else "-",
        "pendncy_resn_desc": str(row.get("PENDNCY_RESN_DESC", "-")).strip() if pd.notna(row.get("PENDNCY_RESN_DESC", None)) else "-",
        "total_landed_cost": round(float(row.get("total_landed_cost", 0.0)), 2) if pd.notna(row.get("total_landed_cost", None)) else 0.0,
        "billable_type": str(row.get("billable_type", "Not Billed")).strip() if pd.notna(row.get("billable_type", None)) else "Not Billed",
    }


def apply_filters(
    df,
    branch,
    ro_status,
    age_bucket,
    mjob=None,
    billable_type=None,
    reg_number=None,
    service_type=None,
    sa_name=None,
    segment=None,
    ro_remark=None,
    pending_reason=None,
    from_date=None,
    to_date=None,
):
    """Apply filters to dataframe"""
    result = df.copy()

    if branch and branch != "All":
        result = result[result["Branch"] == branch]

    if ro_status and ro_status != "All":
        result = result[result["RO Status"] == ro_status]

    if age_bucket and age_bucket != "All":
        result = result[result["Age Bucket"] == age_bucket]

    if billable_type and billable_type != "All":
        result = result[result["billable_type"] == billable_type]

    if service_type and service_type != "All":
        result = result[result["SERVC_TYPE_DESC"] == service_type]

    if sa_name and sa_name != "All":
        result = result[result["Service Adviser Name"] == sa_name]

    if segment and segment != "All":
        result = result[result["segment"] == segment]

    if ro_remark and ro_remark != "All":
        result = result[result["ro_remark_mapped"] == ro_remark]

    if pending_reason and pending_reason != "All" and "PENDNCY_RESN_DESC" in result.columns:
        result = result[result["PENDNCY_RESN_DESC"] == pending_reason]

    # Date range filtering
    if from_date:
        from_date_parsed = parse_date(from_date)
        if from_date_parsed is not None:
            result = result[pd.to_datetime(result["RO Date"], errors="coerce", dayfirst=True) >= from_date_parsed]

    if to_date:
        to_date_parsed = parse_date(to_date)
        if to_date_parsed is not None:
            result = result[pd.to_datetime(result["RO Date"], errors="coerce", dayfirst=True) <= to_date_parsed]

    # MJob filter (Bodyshop only)
    if mjob and mjob != "All":
        if mjob == "Not Categorized":
            result = result[result["RO Remarks"].apply(lambda x: extract_mjobs(x) is None)]
        else:
            search_mjob = mjob.upper()
            result = result[result["RO Remarks"].apply(
                lambda x: any(m.upper() in [search_mjob, search_mjob.replace("/", "")]
                              for m in (extract_mjobs(x) or []))
            )]

    if reg_number and reg_number.strip() != "":
        search_reg = reg_number.strip().upper()
        result = result[result["Reg. Number"].astype(str).str.upper().str.contains(search_reg, na=False)]

    return result


def apply_sort(df: pd.DataFrame, sort_by: str, sort_dir: str) -> pd.DataFrame:
    """
    Supports table header sorting from UI.
    - sort_by: "ro_date" OR "landed_cost" OR "ro_id" etc.
    - sort_dir: "asc" / "desc"
    """
    if df is None or df.empty:
        return df

    direction = (str(sort_dir).lower() == "asc")

    if sort_by == "ro_date":
        return sort_by_ro_date(df, ascending=direction)

    if sort_by == "landed_cost":
        if "total_landed_cost" in df.columns:
            return df.sort_values(by=["total_landed_cost"], ascending=direction)
        return df

    if sort_by == "ro_id":
        if "RO ID" in df.columns:
            return df.sort_values(by=["RO ID"], ascending=direction)
        return df

    # fallback: no change
    return df


# ==================== APP ====================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load data at startup
load_data()

# ==================== API ENDPOINTS ====================

@app.get("/api/dashboard/statistics")
async def statistics():
    """Dashboard statistics - total counts"""
    try:
        if df_global is None or df_global.empty:
            return {
                "total_vehicles": 0,
                "mechanical_count": 0,
                "bodyshop_count": 0,
                "accessories_count": 0,
                "presale_count": 0,
                "total_landed_cost": 0.0,
            }

        return {
            "total_vehicles": int(len(df_global)),
            "mechanical_count": int(len(df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])])),
            "bodyshop_count": int(len(df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"])),
            "accessories_count": int(len(df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"])),
            "presale_count": int(len(df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"])),
            "total_landed_cost": float(df_global["total_landed_cost"].sum()) if "total_landed_cost" in df_global.columns else 0.0,
        }
    except Exception as e:
        print(f"Error in statistics: {str(e)}")
        return {
            "total_vehicles": 0,
            "mechanical_count": 0,
            "bodyshop_count": 0,
            "accessories_count": 0,
            "presale_count": 0,
            "total_landed_cost": 0.0,
        }


@app.get("/api/filter-options/mechanical")
async def mech_filters(branch: Optional[str] = Query("All")):
    """Mechanical filters"""
    try:
        if df_global is None or df_global.empty:
            return {
                "branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"],
                "billable_types": ["All"], "service_types": ["All"], "sa_names": ["All"],
                "segments": ["All"], "ro_remarks": ["All"], "pending_reasons": ["All"]
            }

        df = df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])].copy()

        billable_types = ["All"] + sorted([str(x) for x in df["billable_type"].dropna().unique().tolist() if str(x) != "Not Billed"])
        if (df["billable_type"] == "Not Billed").any():
            billable_types.append("Not Billed")

        exclude_service_types = ["ACCIDENTAL", "PDI SERVICE", "PRESALE", "Sales Accessories", "Service Accessories"]
        service_types_raw = df["SERVC_TYPE_DESC"].dropna().unique().tolist() if "SERVC_TYPE_DESC" in df.columns else []
        service_types_filtered = [str(x) for x in service_types_raw if str(x) not in exclude_service_types]
        service_types = ["All"] + sorted(service_types_filtered)

        if branch and branch != "All":
            df_branch = df[df["Branch"] == branch]
            sa_names = ["All"] + sorted([str(x) for x in df_branch["Service Adviser Name"].dropna().unique().tolist()])
        else:
            sa_names = ["All"] + sorted([str(x) for x in df["Service Adviser Name"].dropna().unique().tolist()])

        segments = ["All"] + sorted([str(x) for x in df["segment"].dropna().unique().tolist() if str(x) != "Unknown"])
        if (df["segment"] == "Unknown").any():
            segments.append("Unknown")

        ro_remarks = ["All"] + get_all_ro_remarks_for_dropdown()

        pending_reasons_raw = df["PENDNCY_RESN_DESC"].dropna().unique().tolist() if "PENDNCY_RESN_DESC" in df.columns else []
        pending_reasons = ["All"] + sorted([str(x) for x in pending_reasons_raw if str(x).strip() not in ["-", "", "nan"]])

        return {
            "branches": ["All"] + sorted([str(x) for x in df["Branch"].dropna().unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df["RO Status"].dropna().unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df["Age Bucket"].dropna().unique().tolist()]),
            "billable_types": billable_types,
            "service_types": service_types,
            "sa_names": sa_names,
            "segments": segments,
            "ro_remarks": ro_remarks,
            "pending_reasons": pending_reasons,
        }
    except Exception as e:
        print(f"Error in mech_filters: {str(e)}")
        return {
            "branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"],
            "billable_types": ["All"], "service_types": ["All"], "sa_names": ["All"],
            "segments": ["All"], "ro_remarks": ["All"], "pending_reasons": ["All"]
        }


@app.get("/api/filter-options/bodyshop")
async def bs_filters(branch: Optional[str] = Query("All")):
    """Bodyshop filters"""
    try:
        if df_global is None or df_global.empty:
            return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"], "mjobs": ["All"],
                    "billable_types": ["All"], "sa_names": ["All"], "segments": ["All"], "ro_remarks": ["All"]}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"].copy()

        mjobs_set = set(["Not Categorized"])
        for remark in df["RO Remarks"].dropna():
            extracted = extract_mjobs(remark)
            if extracted:
                mjobs_set.update(extracted)

        mjobs_sorted = ["All", "Not Categorized"]
        for m in ["M1", "M2", "M3", "M4", "M/4"]:
            if m in mjobs_set:
                mjobs_sorted.append(m)

        billable_types = ["All"] + sorted([str(x) for x in df["billable_type"].dropna().unique().tolist() if str(x) != "Not Billed"])
        if (df["billable_type"] == "Not Billed").any():
            billable_types.append("Not Billed")

        if branch and branch != "All":
            df_branch = df[df["Branch"] == branch]
            sa_names = ["All"] + sorted([str(x) for x in df_branch["Service Adviser Name"].dropna().unique().tolist()])
        else:
            sa_names = ["All"] + sorted([str(x) for x in df["Service Adviser Name"].dropna().unique().tolist()])

        segments = ["All"] + sorted([str(x) for x in df["segment"].dropna().unique().tolist() if str(x) != "Unknown"])
        if (df["segment"] == "Unknown").any():
            segments.append("Unknown")

        ro_remarks = ["All"] + get_all_ro_remarks_for_dropdown()

        return {
            "branches": ["All"] + sorted([str(x) for x in df["Branch"].dropna().unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df["RO Status"].dropna().unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df["Age Bucket"].dropna().unique().tolist()]),
            "mjobs": mjobs_sorted,
            "billable_types": billable_types,
            "sa_names": sa_names,
            "segments": segments,
            "ro_remarks": ro_remarks,
        }
    except Exception as e:
        print(f"Error in bs_filters: {str(e)}")
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"], "mjobs": ["All"],
                "billable_types": ["All"], "sa_names": ["All"], "segments": ["All"], "ro_remarks": ["All"]}


@app.get("/api/filter-options/accessories")
async def acc_filters(branch: Optional[str] = Query("All")):
    """Accessories filters"""
    try:
        if df_global is None or df_global.empty:
            return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"],
                    "billable_types": ["All"], "sa_names": ["All"], "segments": ["All"], "ro_remarks": ["All"]}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"].copy()

        billable_types = ["All"] + sorted([str(x) for x in df["billable_type"].dropna().unique().tolist() if str(x) != "Not Billed"])
        if (df["billable_type"] == "Not Billed").any():
            billable_types.append("Not Billed")

        if branch and branch != "All":
            df_branch = df[df["Branch"] == branch]
            sa_names = ["All"] + sorted([str(x) for x in df_branch["Service Adviser Name"].dropna().unique().tolist()])
        else:
            sa_names = ["All"] + sorted([str(x) for x in df["Service Adviser Name"].dropna().unique().tolist()])

        segments = ["All"] + sorted([str(x) for x in df["segment"].dropna().unique().tolist() if str(x) != "Unknown"])
        if (df["segment"] == "Unknown").any():
            segments.append("Unknown")

        ro_remarks = ["All"] + get_all_ro_remarks_for_dropdown()

        return {
            "branches": ["All"] + sorted([str(x) for x in df["Branch"].dropna().unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df["RO Status"].dropna().unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df["Age Bucket"].dropna().unique().tolist()]),
            "billable_types": billable_types,
            "sa_names": sa_names,
            "segments": segments,
            "ro_remarks": ro_remarks,
        }
    except Exception as e:
        print(f"Error in acc_filters: {str(e)}")
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"],
                "billable_types": ["All"], "sa_names": ["All"], "segments": ["All"], "ro_remarks": ["All"]}


@app.get("/api/filter-options/presale")
async def presale_filters(branch: Optional[str] = Query("All")):
    """Pre-Sale/PDI filters"""
    try:
        if df_global is None or df_global.empty:
            return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"],
                    "billable_types": ["All"], "sa_names": ["All"], "segments": ["All"], "ro_remarks": ["All"]}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"].copy()

        billable_types = ["All"] + sorted([str(x) for x in df["billable_type"].dropna().unique().tolist() if str(x) != "Not Billed"])
        if (df["billable_type"] == "Not Billed").any():
            billable_types.append("Not Billed")

        if branch and branch != "All":
            df_branch = df[df["Branch"] == branch]
            sa_names = ["All"] + sorted([str(x) for x in df_branch["Service Adviser Name"].dropna().unique().tolist()])
        else:
            sa_names = ["All"] + sorted([str(x) for x in df["Service Adviser Name"].dropna().unique().tolist()])

        segments = ["All"] + sorted([str(x) for x in df["segment"].dropna().unique().tolist() if str(x) != "Unknown"])
        if (df["segment"] == "Unknown").any():
            segments.append("Unknown")

        ro_remarks = ["All"] + get_all_ro_remarks_for_dropdown()

        return {
            "branches": ["All"] + sorted([str(x) for x in df["Branch"].dropna().unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df["RO Status"].dropna().unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df["Age Bucket"].dropna().unique().tolist()]),
            "billable_types": billable_types,
            "sa_names": sa_names,
            "segments": segments,
            "ro_remarks": ro_remarks,
        }
    except Exception as e:
        print(f"Error in presale_filters: {str(e)}")
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"],
                "billable_types": ["All"], "sa_names": ["All"], "segments": ["All"], "ro_remarks": ["All"]}


# ==================== VEHICLES (limit=0 means ALL rows) ====================

def _vehicles_response(df: pd.DataFrame, skip: int, limit: int):
    total = int(len(df))
    if limit and limit > 0:
        df2 = df.iloc[skip: skip + limit]
    else:
        df2 = df  # ALL rows
    vehicles = [convert_row(row) for _, row in df2.iterrows()]
    return {"total_count": total, "filtered_count": total, "vehicles": vehicles}


@app.get("/api/vehicles/mechanical")
async def get_mechanical(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    service_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    pending_reason: Optional[str] = Query("All"),
    reg_number: Optional[str] = Query(""),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    sort_by: Optional[str] = Query("ro_date"),   # ro_date / landed_cost / ro_id
    sort_dir: Optional[str] = Query("desc"),     # desc / asc
    skip: int = Query(0),
    limit: int = Query(50),                      # UI dropdown can send 0 for ALL
):
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])].copy()
        df = apply_filters(df, branch, ro_status, age_bucket, None, billable_type, reg_number, service_type, sa_name, segment, ro_remark, pending_reason, from_date, to_date)
        df = apply_sort(df, sort_by, sort_dir)

        filtered = int(len(df))
        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]

        return {"total_count": int(len(df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])])),
                "filtered_count": filtered,
                "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_mechanical: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}


@app.get("/api/vehicles/bodyshop")
async def get_bodyshop(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    reg_number: Optional[str] = Query(""),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(50),
):
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"].copy()
        df = apply_filters(df, branch, ro_status, age_bucket, mjob, billable_type, reg_number, None, sa_name, segment, ro_remark, None, from_date, to_date)
        df = apply_sort(df, sort_by, sort_dir)

        filtered = int(len(df))
        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]

        return {"total_count": int(len(df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"])),
                "filtered_count": filtered,
                "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_bodyshop: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}


@app.get("/api/vehicles/accessories")
async def get_accessories(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(50),
):
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"].copy()
        df = apply_filters(df, branch, ro_status, age_bucket, None, billable_type, None, None, sa_name, segment, ro_remark, None, from_date, to_date)
        df = apply_sort(df, sort_by, sort_dir)

        filtered = int(len(df))
        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]

        return {"total_count": int(len(df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"])),
                "filtered_count": filtered,
                "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_accessories: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}


@app.get("/api/vehicles/presale")
async def get_presale(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(50),
):
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"].copy()
        df = apply_filters(df, branch, ro_status, age_bucket, None, billable_type, None, None, sa_name, segment, ro_remark, None, from_date, to_date)
        df = apply_sort(df, sort_by, sort_dir)

        filtered = int(len(df))
        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]

        return {"total_count": int(len(df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"])),
                "filtered_count": filtered,
                "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_presale: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}


# ==================== EXPORTS (Clear All => /api/export/all) ====================

@app.get("/api/export/mechanical")
async def export_mech(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    service_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    pending_reason: Optional[str] = Query("All"),
    reg_number: Optional[str] = Query(""),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(0),   # 0 = ALL filtered rows
):
    try:
        if df_global is None or df_global.empty:
            return {"vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])].copy()
        df = apply_filters(df, branch, ro_status, age_bucket, None, billable_type, reg_number, service_type, sa_name, segment, ro_remark, pending_reason, from_date, to_date)
        df = apply_sort(df, sort_by, sort_dir)

        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]

        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        print(f"Error in export_mech: {str(e)}")
        return {"vehicles": []}


@app.get("/api/export/bodyshop")
async def export_bs(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    reg_number: Optional[str] = Query(""),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(0),
):
    try:
        if df_global is None or df_global.empty:
            return {"vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"].copy()
        df = apply_filters(df, branch, ro_status, age_bucket, mjob, billable_type, reg_number, None, sa_name, segment, ro_remark, None, from_date, to_date)
        df = apply_sort(df, sort_by, sort_dir)

        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]

        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        print(f"Error in export_bs: {str(e)}")
        return {"vehicles": []}


@app.get("/api/export/accessories")
async def export_acc(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(0),
):
    try:
        if df_global is None or df_global.empty:
            return {"vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"].copy()
        df = apply_filters(df, branch, ro_status, age_bucket, None, billable_type, None, None, sa_name, segment, ro_remark, None, from_date, to_date)
        df = apply_sort(df, sort_by, sort_dir)

        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]

        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        print(f"Error in export_acc: {str(e)}")
        return {"vehicles": []}


@app.get("/api/export/presale")
async def export_presale(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(0),
):
    try:
        if df_global is None or df_global.empty:
            return {"vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"].copy()
        df = apply_filters(df, branch, ro_status, age_bucket, None, billable_type, None, None, sa_name, segment, ro_remark, None, from_date, to_date)
        df = apply_sort(df, sort_by, sort_dir)

        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]

        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        print(f"Error in export_presale: {str(e)}")
        return {"vehicles": []}


@app.get("/api/export/all")
async def export_all(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    service_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    pending_reason: Optional[str] = Query("All"),
    reg_number: Optional[str] = Query(""),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(0),  # 0 = ALL filtered rows
):
    """
    Clear All case:
    - exports ALL data (with current filters, if any)
    """
    try:
        if df_global is None or df_global.empty:
            return {"vehicles": []}

        df = df_global.copy()
        df = apply_filters(df, branch, ro_status, age_bucket, None, billable_type, reg_number, service_type, sa_name, segment, ro_remark, pending_reason, from_date, to_date)
        df = apply_sort(df, sort_by, sort_dir)

        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]

        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        print(f"Error in export_all: {str(e)}")
        return {"vehicles": []}


# ==================== ROOT / HEALTH ====================

@app.get("/")
async def dashboard():
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return {"error": "dashboard.html not found"}


@app.get("/health")
async def health():
    return {"status": "healthy", "records": int(len(df_global)) if df_global is not None and not df_global.empty else 0}


# ==================== MAIN ====================

if __name__ == "__main__":
    import uvicorn
    print(f"Running on http://{HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)

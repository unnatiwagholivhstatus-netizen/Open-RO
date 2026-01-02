import os
import pandas as pd
import re
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any

# ==================== CONFIGURATION ====================

PORT = int(os.getenv("PORT", 8000))
HOST = "0.0.0.0"

# ==================== GLOBAL DATA ====================

df_global = None
df_landed_cost = None
df_billable_type = None
df_model_group = None
ro_remarks_list = []  # Standard RO Remarks


# ==================== DATA LOADING ====================

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
        else:
            return []
    except Exception as e:
        print(f"Error loading RO remarks dynamically: {e}")
        return []


def get_all_ro_remarks_for_dropdown():
    """Get all RO remarks for dropdown - both standard and those found in data"""
    try:
        standard_remarks = load_ro_remarks_dynamically()

        if df_global is not None and (not df_global.empty) and ("ro_remark_mapped" in df_global.columns):
            mapped_remarks = [
                str(x) for x in df_global["ro_remark_mapped"].dropna().unique()
                if str(x) != "Not Assigned"
            ]
            all_remarks = list(set(standard_remarks + mapped_remarks))
            return sorted(all_remarks)
        else:
            return sorted(standard_remarks) if standard_remarks else ["All"]
    except Exception as e:
        print(f"Error getting all RO remarks: {e}")
        return ["All"]


def map_ro_remark(remark):
    """
    Map RO Remarks to standard codes (case-insensitive)
    Searches for any standard remark code within the remarks text
    Returns the first match found, or "Not Assigned" if no match
    """
    global ro_remarks_list

    if pd.isna(remark) or str(remark).strip() in ["", "-"]:
        return "Not Assigned"

    remarks_list = load_ro_remarks_dynamically()
    if not remarks_list:
        remarks_list = ro_remarks_list  # fallback

    remark_str = str(remark).strip().upper()

    for standard_remark in remarks_list:
        standard_upper = str(standard_remark).upper()
        if standard_upper in remark_str:
            return standard_remark

    return "Not Assigned"


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
            print("[WARN] RO Remark file not found - RO Remark mapping will not be available")
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
            print("[WARN] Model Group Excel file not found - Model Group and Segment will not be enriched")
            df_model_group = pd.DataFrame()

        # ==================== Load Open RO data ====================
        excel_file = None
        for fn in ["Open RO.xlsx", "Open_RO.xlsx", "open_ro.xlsx"]:
            if os.path.exists(fn):
                excel_file = fn
                break

        if excel_file is None:
            print("[WARN] Open RO Excel file not found")
            df_global = pd.DataFrame()
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
            return

        print(f"[OK] Loading: {excel_file}")
        df_global = pd.read_excel(excel_file)
        print(f"[OK] Loaded {len(df_global)} rows, {len(df_global.columns)} cols")
        print(f"[OK] Columns: {list(df_global.columns)}")

        # ==================== RO DATE PARSE (FOR SORTING + DATE FILTERS) ====================
        # This is the only change required for correct RO Date sort behavior
        if "RO Date" in df_global.columns:
            df_global["RO_DATE_DT"] = pd.to_datetime(df_global["RO Date"], errors="coerce", dayfirst=True)
        else:
            df_global["RO_DATE_DT"] = pd.NaT

        # ==================== Add RO Remark Mapping Column ====================
        print("[OK] Mapping RO Remarks...")
        if "RO Remarks" in df_global.columns:
            df_global["ro_remark_mapped"] = df_global["RO Remarks"].apply(map_ro_remark)
        else:
            df_global["ro_remark_mapped"] = "Not Assigned"
        print("[OK] RO Remark mapping complete")

        # ==================== Merge Model Group mapping ====================
        if df_model_group is not None and (not df_model_group.empty):
            try:
                if "Model Group" in df_global.columns:
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
                print(f"[WARN] Error during model mapping: {str(e)}")
                df_global["segment"] = "Unknown"
        else:
            df_global["segment"] = "Unknown"

        # ==================== Load and aggregate Landed Cost data ====================
        parts_file = None
        for fn in ["Part Issue But Not Bill.xlsx", "Part_Issue_But_Not_Bill.xlsx", "part_issue_but_not_bill.xlsx"]:
            if os.path.exists(fn):
                parts_file = fn
                break

        if parts_file is None:
            print("[WARN] Part Issue file not found - Landed Cost and Billable Type will not be available")
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
            df_global["total_landed_cost"] = 0
            df_global["billable_type"] = "Not Billed"
        else:
            print(f"[OK] Loading: {parts_file}")
            df_parts = pd.read_excel(parts_file)
            print(f"[OK] Loaded {len(df_parts)} part records")

            df_landed_cost = df_parts.groupby("RO Number")["Landed Cost (Total)"].sum().reset_index()
            df_landed_cost.columns = ["RO ID", "total_landed_cost"]

            df_billable_type = df_parts.groupby("RO Number")["Billable Type"].first().reset_index()
            df_billable_type.columns = ["RO ID", "billable_type"]

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


load_data()

# ==================== HELPER FUNCTIONS ====================

def parse_date(date_str):
    """Parse date string in various formats (YYYY-MM-DD, DD/MM/YYYY, etc.)"""
    if not date_str or pd.isna(date_str):
        return None

    date_str = str(date_str).strip()

    try:
        return pd.to_datetime(date_str, format="%Y-%m-%d")
    except:
        pass

    try:
        return pd.to_datetime(date_str, format="%d/%m/%Y")
    except:
        pass

    try:
        return pd.to_datetime(date_str)
    except:
        return None


def extract_mjobs(remark):
    """Extract MJob codes from remarks"""
    if pd.isna(remark) or remark in ["-", ""]:
        return None

    remark_str = str(remark).strip()
    matches = re.findall(r"\bM/?[1-4]\b", remark_str)
    return matches if matches else None


def get_mjob_category(remark):
    """Categorize a remark as M1, M2, M3, M4, M/4, or 'Not Categorized' """
    mjobs = extract_mjobs(remark)
    if mjobs:
        return mjobs[0]
    return "Not Categorized"


def convert_row(row) -> Dict[str, Any]:
    """Convert pandas row to JSON-safe dict"""
    def safe_date_parse(date_value):
        if pd.notna(date_value) and str(date_value).strip() not in ["-", "", "nan", "NaT"]:
            try:
                return pd.Timestamp(date_value).strftime("%Y-%m-%d")
            except:
                return "-"
        return "-"

    return {
        "ro_id": str(row["RO ID"]).strip() if pd.notna(row.get("RO ID")) else "-",
        "branch": str(row["Branch"]).strip() if pd.notna(row.get("Branch")) else "-",
        "ro_status": str(row["RO Status"]).strip() if pd.notna(row.get("RO Status")) else "-",
        "age_bucket": str(row["Age Bucket"]).strip() if pd.notna(row.get("Age Bucket")) else "-",
        "service_category": str(row["SERVC_CATGRY_DESC"]).strip() if pd.notna(row.get("SERVC_CATGRY_DESC")) else "-",
        "service_type": str(row["SERVC_TYPE_DESC"]).strip() if pd.notna(row.get("SERVC_TYPE_DESC")) else "-",
        "vehicle_model": str(row["Family"]).strip() if pd.notna(row.get("Family")) else "-",
        "model_group": str(row.get("Model Group", "-")).strip() if pd.notna(row.get("Model Group")) else "-",
        "segment": str(row.get("segment", "Unknown")).strip() if pd.notna(row.get("segment")) else "Unknown",
        "reg_number": str(row["Reg. Number"]).strip() if pd.notna(row.get("Reg. Number")) else "-",
        "ro_date": safe_date_parse(row.get("RO Date")),
        "vehicle_ready_date": safe_date_parse(row.get("Vehicle  Ready Date")),
        "ro_remarks": str(row["RO Remarks"]).strip() if pd.notna(row.get("RO Remarks")) else "-",
        "ro_remark_mapped": str(row.get("ro_remark_mapped", "Not Assigned")).strip()
        if pd.notna(row.get("ro_remark_mapped")) else "Not Assigned",
        "km": int(row["KM"]) if pd.notna(row.get("KM")) else 0,
        "days": int(row["Days"]) if pd.notna(row.get("Days")) else 0,
        "days_open": int(row["[No of Visits (In last 90 days)]"])
        if pd.notna(row.get("[No of Visits (In last 90 days)]")) else 0,
        "service_adviser": str(row["Service Adviser Name"]).strip()
        if pd.notna(row.get("Service Adviser Name")) else "-",
        "vin": str(row["VIN"]).strip() if pd.notna(row.get("VIN")) else "-",
        "pendncy_resn_desc": str(row["PENDNCY_RESN_DESC"]).strip()
        if pd.notna(row.get("PENDNCY_RESN_DESC")) else "-",
        "total_landed_cost": round(float(row.get("total_landed_cost", 0.0)), 2)
        if pd.notna(row.get("total_landed_cost")) else 0.00,
        "billable_type": str(row.get("billable_type", "Not Billed")).strip()
        if pd.notna(row.get("billable_type")) else "Not Billed",
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

    if pending_reason and pending_reason != "All":
        result = result[result["PENDNCY_RESN_DESC"] == pending_reason]

    # Date range filtering (use parsed datetime column for correct results)
    if from_date:
        from_date_parsed = parse_date(from_date)
        if from_date_parsed is not None and "RO_DATE_DT" in result.columns:
            result = result[result["RO_DATE_DT"] >= from_date_parsed]

    if to_date:
        to_date_parsed = parse_date(to_date)
        if to_date_parsed is not None and "RO_DATE_DT" in result.columns:
            result = result[result["RO_DATE_DT"] <= to_date_parsed]

    if mjob and mjob != "All":
        if mjob == "Not Categorized":
            result = result[result["RO Remarks"].apply(lambda x: extract_mjobs(x) is None)]
        else:
            search_mjob = mjob.upper()
            result = result[result["RO Remarks"].apply(
                lambda x: any(
                    m.upper() in [search_mjob, search_mjob.replace("/", "")]
                    for m in (extract_mjobs(x) or [])
                )
            )]

    if reg_number and reg_number.strip() != "":
        search_reg = reg_number.strip().upper()
        result = result[result["Reg. Number"].astype(str).str.upper().str.contains(search_reg, na=False)]

    return result


# ==================== APP ====================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


@app.get("/api/dashboard/division-stats-v2")
async def division_stats_v2(service_category: str = Query("mechanical")):
    """Division-wise stats with both Open and Closed But Not Billed counts"""
    try:
        if df_global is None or df_global.empty:
            return {"divisions": [], "total_open": 0, "total_closed_not_billed": 0}

        if service_category == "mechanical":
            df = df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])]
        elif service_category == "bodyshop":
            df = df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"]
        elif service_category == "accessories":
            df = df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"]
        elif service_category == "presale":
            df = df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"]
        else:
            df = df_global

        branches = df["Branch"].unique()

        divisions = []
        total_open = 0
        total_closed_not_billed = 0

        for branch in sorted(branches):
            df_branch = df[df["Branch"] == branch]

            open_count = len(df_branch[df_branch["RO Status"] == "Open"])
            closed_not_billed_count = len(df_branch[df_branch["RO Status"] == "Closed but not billed"])

            total_open += open_count
            total_closed_not_billed += closed_not_billed_count

            divisions.append({
                "branch": branch,
                "open_count": open_count,
                "closed_not_billed_count": closed_not_billed_count,
                "total": open_count + closed_not_billed_count
            })

        divisions = sorted(divisions, key=lambda x: x["total"], reverse=True)

        return {
            "service_category": service_category,
            "total_open": total_open,
            "total_closed_not_billed": total_closed_not_billed,
            "divisions": divisions
        }

    except Exception as e:
        print(f"Error in division_stats_v2: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"service_category": service_category, "total_open": 0, "total_closed_not_billed": 0, "divisions": []}


@app.get("/api/dashboard/division-stats")
async def division_stats(service_category: str = Query("mechanical")):
    """Division-wise (Branch-wise) open RO count for each service category"""
    try:
        if df_global is None or df_global.empty:
            return {"divisions": []}

        if service_category == "mechanical":
            df = df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])]
        elif service_category == "bodyshop":
            df = df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"]
        elif service_category == "accessories":
            df = df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"]
        elif service_category == "presale":
            df = df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"]
        else:
            df = df_global

        df_open = df[df["RO Status"] == "Open"].copy()

        division_stats_df = df_open.groupby("Branch").size().reset_index(name="count")
        division_stats_df = division_stats_df.sort_values("count", ascending=False)

        divisions = [{"branch": row["Branch"], "open_count": int(row["count"])} for _, row in division_stats_df.iterrows()]
        total_open = int(df_open.shape[0])

        return {"service_category": service_category, "total_open": total_open, "divisions": divisions}

    except Exception as e:
        print(f"Error in division_stats: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"service_category": service_category, "total_open": 0, "divisions": []}


@app.get("/api/dashboard/statistics/filtered")
async def filtered_statistics(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    service_category: Optional[str] = Query("All"),
    service_type: Optional[str] = Query("All"),
    reg_number: Optional[str] = Query(""),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    pending_reason: Optional[str] = Query("All"),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
):
    """Dashboard statistics - with dynamic filtering by service category"""
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

        filtered_df = df_global.copy()

        if service_category and service_category != "All":
            if service_category == "mechanical":
                filtered_df = filtered_df[filtered_df["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])]
            elif service_category == "bodyshop":
                filtered_df = filtered_df[filtered_df["SERVC_CATGRY_DESC"] == "Bodyshop"]
            elif service_category == "accessories":
                filtered_df = filtered_df[filtered_df["SERVC_CATGRY_DESC"] == "Accessories"]
            elif service_category == "presale":
                filtered_df = filtered_df[filtered_df["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"]

        # Apply filters (same logic as your original)
        if branch and branch != "All":
            filtered_df = filtered_df[filtered_df["Branch"] == branch]

        if ro_status and ro_status != "All":
            filtered_df = filtered_df[filtered_df["RO Status"] == ro_status]

        if age_bucket and age_bucket != "All":
            filtered_df = filtered_df[filtered_df["Age Bucket"] == age_bucket]

        if billable_type and billable_type != "All":
            filtered_df = filtered_df[filtered_df["billable_type"] == billable_type]

        if service_type and service_type != "All":
            filtered_df = filtered_df[filtered_df["SERVC_TYPE_DESC"] == service_type]

        if sa_name and sa_name != "All":
            filtered_df = filtered_df[filtered_df["Service Adviser Name"] == sa_name]

        if segment and segment != "All":
            filtered_df = filtered_df[filtered_df["segment"] == segment]

        if ro_remark and ro_remark != "All":
            filtered_df = filtered_df[filtered_df["ro_remark_mapped"] == ro_remark]

        if pending_reason and pending_reason != "All":
            filtered_df = filtered_df[filtered_df["PENDNCY_RESN_DESC"] == pending_reason]

        # Date range filtering using RO_DATE_DT
        if from_date:
            from_date_parsed = parse_date(from_date)
            if from_date_parsed is not None and "RO_DATE_DT" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df["RO_DATE_DT"] >= from_date_parsed]

        if to_date:
            to_date_parsed = parse_date(to_date)
            if to_date_parsed is not None and "RO_DATE_DT" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df["RO_DATE_DT"] <= to_date_parsed]

        if mjob and mjob != "All":
            if mjob == "Not Categorized":
                filtered_df = filtered_df[filtered_df["RO Remarks"].apply(lambda x: extract_mjobs(x) is None)]
            else:
                search_mjob = mjob.upper()
                filtered_df = filtered_df[filtered_df["RO Remarks"].apply(
                    lambda x: any(
                        m.upper() in [search_mjob, search_mjob.replace("/", "")]
                        for m in (extract_mjobs(x) or [])
                    )
                )]

        if reg_number and reg_number.strip() != "":
            search_reg = reg_number.strip().upper()
            filtered_df = filtered_df[filtered_df["Reg. Number"].astype(str).str.upper().str.contains(search_reg, na=False)]

        mechanical = filtered_df[filtered_df["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])]
        bodyshop = filtered_df[filtered_df["SERVC_CATGRY_DESC"] == "Bodyshop"]
        accessories = filtered_df[filtered_df["SERVC_CATGRY_DESC"] == "Accessories"]
        presale = filtered_df[filtered_df["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"]

        total_cost = float(filtered_df["total_landed_cost"].sum()) if "total_landed_cost" in filtered_df.columns else 0.0

        return {
            "total_vehicles": int(len(filtered_df)),
            "mechanical_count": int(len(mechanical)),
            "bodyshop_count": int(len(bodyshop)),
            "accessories_count": int(len(accessories)),
            "presale_count": int(len(presale)),
            "total_landed_cost": float(total_cost),
        }

    except Exception as e:
        print(f"Error in filtered_statistics: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "total_vehicles": 0,
            "mechanical_count": 0,
            "bodyshop_count": 0,
            "accessories_count": 0,
            "presale_count": 0,
            "total_landed_cost": 0.0,
        }


# ==================== FILTER OPTIONS ====================

@app.get("/api/filter-options/mechanical")
async def mech_filters(branch: Optional[str] = Query("All")):
    """Mechanical filters - SA Names and Segments filtered by branch"""
    try:
        if df_global is None or df_global.empty:
            return {
                "branches": ["All"],
                "ro_statuses": ["All"],
                "age_buckets": ["All"],
                "billable_types": ["All"],
                "service_types": ["All"],
                "sa_names": ["All"],
                "segments": ["All"],
                "ro_remarks": ["All"],
                "pending_reasons": ["All"],
            }

        df = df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])]

        billable_types = ["All"] + sorted([str(x) for x in df["billable_type"].dropna().unique().tolist() if str(x) != "Not Billed"])
        if "Not Billed" in df["billable_type"].astype(str).values:
            billable_types.append("Not Billed")

        exclude_service_types = ["ACCIDENTAL", "PDI SERVICE", "PRESALE", "Sales Accessories", "Service Accessories"]
        service_types_raw = df["SERVC_TYPE_DESC"].dropna().unique().tolist()
        service_types_filtered = [str(x) for x in service_types_raw if str(x) not in exclude_service_types]
        service_types = ["All"] + sorted(service_types_filtered)

        if branch and branch != "All":
            df_branch = df[df["Branch"] == branch]
            sa_names = ["All"] + sorted([str(x) for x in df_branch["Service Adviser Name"].dropna().unique().tolist()])
        else:
            sa_names = ["All"] + sorted([str(x) for x in df["Service Adviser Name"].dropna().unique().tolist()])

        if "segment" in df.columns:
            segments = ["All"] + sorted([str(x) for x in df["segment"].dropna().unique().tolist() if str(x) != "Unknown"])
            if "Unknown" in df["segment"].astype(str).values:
                segments.append("Unknown")
        else:
            segments = ["All", "Unknown"]

        ro_remarks = ["All"] + get_all_ro_remarks_for_dropdown()

        pending_reasons_raw = df["PENDNCY_RESN_DESC"].dropna().unique().tolist()
        pending_reasons = ["All"] + sorted([str(x) for x in pending_reasons_raw if str(x).strip() not in ["-", "", "nan"]])

        return {
            "branches": ["All"] + sorted([str(x) for x in df["Branch"].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df["RO Status"].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df["Age Bucket"].unique().tolist()]),
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
            "branches": ["All"],
            "ro_statuses": ["All"],
            "age_buckets": ["All"],
            "billable_types": ["All"],
            "service_types": ["All"],
            "sa_names": ["All"],
            "segments": ["All"],
            "ro_remarks": ["All"],
            "pending_reasons": ["All"],
        }


@app.get("/api/filter-options/bodyshop")
async def bs_filters(branch: Optional[str] = Query("All")):
    """Bodyshop filters - dynamically extracts MJob options, billable types, SA Names, and Segments"""
    try:
        if df_global is None or df_global.empty:
            return {
                "branches": ["All"],
                "ro_statuses": ["All"],
                "age_buckets": ["All"],
                "mjobs": ["All"],
                "billable_types": ["All"],
                "sa_names": ["All"],
                "segments": ["All"],
                "ro_remarks": ["All"],
            }

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"]

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
        if "Not Billed" in df["billable_type"].astype(str).values:
            billable_types.append("Not Billed")

        if branch and branch != "All":
            df_branch = df[df["Branch"] == branch]
            sa_names = ["All"] + sorted([str(x) for x in df_branch["Service Adviser Name"].dropna().unique().tolist()])
        else:
            sa_names = ["All"] + sorted([str(x) for x in df["Service Adviser Name"].dropna().unique().tolist()])

        if "segment" in df.columns:
            segments = ["All"] + sorted([str(x) for x in df["segment"].dropna().unique().tolist() if str(x) != "Unknown"])
            if "Unknown" in df["segment"].astype(str).values:
                segments.append("Unknown")
        else:
            segments = ["All", "Unknown"]

        ro_remarks = ["All"] + get_all_ro_remarks_for_dropdown()

        return {
            "branches": ["All"] + sorted([str(x) for x in df["Branch"].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df["RO Status"].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df["Age Bucket"].unique().tolist()]),
            "mjobs": mjobs_sorted,
            "billable_types": billable_types,
            "sa_names": sa_names,
            "segments": segments,
            "ro_remarks": ro_remarks,
        }

    except Exception as e:
        print(f"Error in bs_filters: {str(e)}")
        return {
            "branches": ["All"],
            "ro_statuses": ["All"],
            "age_buckets": ["All"],
            "mjobs": ["All"],
            "billable_types": ["All"],
            "sa_names": ["All"],
            "segments": ["All"],
            "ro_remarks": ["All"],
        }


@app.get("/api/filter-options/accessories")
async def acc_filters(branch: Optional[str] = Query("All")):
    """Accessories filters with billable type, SA Names, and Segments"""
    try:
        if df_global is None or df_global.empty:
            return {
                "branches": ["All"],
                "ro_statuses": ["All"],
                "age_buckets": ["All"],
                "billable_types": ["All"],
                "sa_names": ["All"],
                "segments": ["All"],
                "ro_remarks": ["All"],
            }

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"]

        billable_types = ["All"] + sorted([str(x) for x in df["billable_type"].dropna().unique().tolist() if str(x) != "Not Billed"])
        if "Not Billed" in df["billable_type"].astype(str).values:
            billable_types.append("Not Billed")

        if branch and branch != "All":
            df_branch = df[df["Branch"] == branch]
            sa_names = ["All"] + sorted([str(x) for x in df_branch["Service Adviser Name"].dropna().unique().tolist()])
        else:
            sa_names = ["All"] + sorted([str(x) for x in df["Service Adviser Name"].dropna().unique().tolist()])

        if "segment" in df.columns:
            segments = ["All"] + sorted([str(x) for x in df["segment"].dropna().unique().tolist() if str(x) != "Unknown"])
            if "Unknown" in df["segment"].astype(str).values:
                segments.append("Unknown")
        else:
            segments = ["All", "Unknown"]

        ro_remarks = ["All"] + get_all_ro_remarks_for_dropdown()

        return {
            "branches": ["All"] + sorted([str(x) for x in df["Branch"].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df["RO Status"].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df["Age Bucket"].unique().tolist()]),
            "billable_types": billable_types,
            "sa_names": sa_names,
            "segments": segments,
            "ro_remarks": ro_remarks,
        }

    except Exception as e:
        print(f"Error in acc_filters: {str(e)}")
        return {
            "branches": ["All"],
            "ro_statuses": ["All"],
            "age_buckets": ["All"],
            "billable_types": ["All"],
            "sa_names": ["All"],
            "segments": ["All"],
            "ro_remarks": ["All"],
        }


@app.get("/api/filter-options/presale")
async def presale_filters(branch: Optional[str] = Query("All")):
    """Pre-Sale/PDI filters with billable type, SA Names, and Segments"""
    try:
        if df_global is None or df_global.empty:
            return {
                "branches": ["All"],
                "ro_statuses": ["All"],
                "age_buckets": ["All"],
                "billable_types": ["All"],
                "sa_names": ["All"],
                "segments": ["All"],
                "ro_remarks": ["All"],
            }

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"]

        billable_types = ["All"] + sorted([str(x) for x in df["billable_type"].dropna().unique().tolist() if str(x) != "Not Billed"])
        if "Not Billed" in df["billable_type"].astype(str).values:
            billable_types.append("Not Billed")

        if branch and branch != "All":
            df_branch = df[df["Branch"] == branch]
            sa_names = ["All"] + sorted([str(x) for x in df_branch["Service Adviser Name"].dropna().unique().tolist()])
        else:
            sa_names = ["All"] + sorted([str(x) for x in df["Service Adviser Name"].dropna().unique().tolist()])

        if "segment" in df.columns:
            segments = ["All"] + sorted([str(x) for x in df["segment"].dropna().unique().tolist() if str(x) != "Unknown"])
            if "Unknown" in df["segment"].astype(str).values:
                segments.append("Unknown")
        else:
            segments = ["All", "Unknown"]

        ro_remarks = ["All"] + get_all_ro_remarks_for_dropdown()

        return {
            "branches": ["All"] + sorted([str(x) for x in df["Branch"].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df["RO Status"].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df["Age Bucket"].unique().tolist()]),
            "billable_types": billable_types,
            "sa_names": sa_names,
            "segments": segments,
            "ro_remarks": ro_remarks,
        }

    except Exception as e:
        print(f"Error in presale_filters: {str(e)}")
        return {
            "branches": ["All"],
            "ro_statuses": ["All"],
            "age_buckets": ["All"],
            "billable_types": ["All"],
            "sa_names": ["All"],
            "segments": ["All"],
            "ro_remarks": ["All"],
        }


# ==================== VEHICLES (WITH RO DATE SORT FIX) ====================

def sort_by_ro_date_desc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Current date on top (Descending).
    This is the only sorting change.
    """
    if df is None or df.empty:
        return df
    if "RO_DATE_DT" in df.columns:
        return df.sort_values("RO_DATE_DT", ascending=False, na_position="last")
    return df


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
    skip: int = Query(0),
    limit: int = Query(50),
):
    """Get mechanical vehicles with date range filtering"""
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])].copy()
        total = len(df)

        df = apply_filters(
            df, branch, ro_status, age_bucket,
            billable_type=billable_type,
            service_type=service_type,
            sa_name=sa_name,
            reg_number=reg_number,
            segment=segment,
            ro_remark=ro_remark,
            pending_reason=pending_reason,
            from_date=from_date,
            to_date=to_date
        )
        filtered = len(df)

        # RO DATE sort before pagination (FIX)
        df = sort_by_ro_date_desc(df)

        df = df.iloc[skip: skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}

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
    skip: int = Query(0),
    limit: int = Query(50),
):
    """Get bodyshop vehicles with date range filtering"""
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"].copy()
        total = len(df)

        df = apply_filters(
            df, branch, ro_status, age_bucket,
            mjob=mjob,
            billable_type=billable_type,
            reg_number=reg_number,
            sa_name=sa_name,
            segment=segment,
            ro_remark=ro_remark,
            from_date=from_date,
            to_date=to_date
        )
        filtered = len(df)

        # RO DATE sort before pagination (FIX)
        df = sort_by_ro_date_desc(df)

        df = df.iloc[skip: skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}

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
    skip: int = Query(0),
    limit: int = Query(50),
):
    """Get accessories vehicles with date range filtering"""
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"].copy()
        total = len(df)

        df = apply_filters(
            df, branch, ro_status, age_bucket,
            billable_type=billable_type,
            sa_name=sa_name,
            segment=segment,
            ro_remark=ro_remark,
            from_date=from_date,
            to_date=to_date
        )
        filtered = len(df)

        # RO DATE sort before pagination (FIX)
        df = sort_by_ro_date_desc(df)

        df = df.iloc[skip: skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}

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
    skip: int = Query(0),
    limit: int = Query(50),
):
    """Get Pre-Sale/PDI vehicles with date range filtering"""
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"].copy()
        total = len(df)

        df = apply_filters(
            df, branch, ro_status, age_bucket,
            billable_type=billable_type,
            sa_name=sa_name,
            segment=segment,
            ro_remark=ro_remark,
            from_date=from_date,
            to_date=to_date
        )
        filtered = len(df)

        # RO DATE sort before pagination (FIX)
        df = sort_by_ro_date_desc(df)

        df = df.iloc[skip: skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}

    except Exception as e:
        print(f"Error in get_presale: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}


# ==================== EXPORT ENDPOINTS (UNCHANGED LOGIC) ====================

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
):
    """Export mechanical vehicles"""
    try:
        if df_global is None or df_global.empty:
            return {"vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])].copy()
        df = apply_filters(
            df, branch, ro_status, age_bucket,
            billable_type=billable_type,
            service_type=service_type,
            sa_name=sa_name,
            reg_number=reg_number,
            segment=segment,
            ro_remark=ro_remark,
            pending_reason=pending_reason,
            from_date=from_date,
            to_date=to_date
        )

        # Keep export consistent with current date on top
        df = sort_by_ro_date_desc(df)

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
):
    """Export bodyshop vehicles with date range filtering"""
    try:
        if df_global is None or df_global.empty:
            return {"vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"].copy()
        df = apply_filters(
            df, branch, ro_status, age_bucket,
            mjob=mjob,
            billable_type=billable_type,
            reg_number=reg_number,
            sa_name=sa_name,
            segment=segment,
            ro_remark=ro_remark,
            from_date=from_date,
            to_date=to_date
        )

        df = sort_by_ro_date_desc(df)

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
):
    """Export accessories vehicles"""
    try:
        if df_global is None or df_global.empty:
            return {"vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"].copy()
        df = apply_filters(
            df, branch, ro_status, age_bucket,
            billable_type=billable_type,
            sa_name=sa_name,
            segment=segment,
            ro_remark=ro_remark,
            from_date=from_date,
            to_date=to_date
        )

        df = sort_by_ro_date_desc(df)

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
):
    """Export Pre-Sale/PDI vehicles"""
    try:
        if df_global is None or df_global.empty:
            return {"vehicles": []}

        df = df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"].copy()
        df = apply_filters(
            df, branch, ro_status, age_bucket,
            billable_type=billable_type,
            sa_name=sa_name,
            segment=segment,
            ro_remark=ro_remark,
            from_date=from_date,
            to_date=to_date
        )

        df = sort_by_ro_date_desc(df)

        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}

    except Exception as e:
        print(f"Error in export_presale: {str(e)}")
        return {"vehicles": []}


# ==================== ROOT + HEALTH ====================

@app.get("/")
async def dashboard():
    """Serve dashboard"""
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return {"error": "dashboard.html not found"}


@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy", "records": len(df_global) if (df_global is not None and not df_global.empty) else 0}


# ==================== MAIN ====================

if __name__ == "__main__":
    import uvicorn
    print(f"Running on http://{HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)

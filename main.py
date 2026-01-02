import os
import pandas as pd
import re
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any

# ==================== CONFIGURATION ====================

PORT = int(os.getenv('PORT', 8000))
HOST = '0.0.0.0'

# ==================== GLOBAL DATA ====================

df_global = None
df_landed_cost = None
df_billable_type = None
df_model_group = None
ro_remarks_list = []  # Standard RO Remarks


# ==================== DATE HELPERS ====================

def parse_date(date_str):
    """Parse date string in various formats (YYYY-MM-DD, DD/MM/YYYY, etc.)"""
    if not date_str or pd.isna(date_str):
        return None

    date_str = str(date_str).strip()
    if date_str in ["-", "", "nan", "NaT"]:
        return None

    # Try ISO first
    try:
        return pd.to_datetime(date_str, format='%Y-%m-%d', errors='raise')
    except:
        pass

    # Try DD/MM/YYYY
    try:
        return pd.to_datetime(date_str, format='%d/%m/%Y', errors='raise')
    except:
        pass

    # Generic (handles mixed)
    try:
        return pd.to_datetime(date_str, errors='coerce', dayfirst=True)
    except:
        return None


def build_datetime_column(series: pd.Series) -> pd.Series:
    """
    Robust datetime conversion for mixed formats.
    Uses dayfirst=True to support DD/MM/YYYY properly.
    """
    dt = pd.to_datetime(series, errors='coerce', dayfirst=True)
    # If almost everything became NaT, try again without dayfirst
    if dt.notna().sum() == 0 and series.notna().sum() > 0:
        dt = pd.to_datetime(series, errors='coerce', dayfirst=False)
    return dt


# ==================== RO REMARK HELPERS ====================

def load_ro_remarks_dynamically():
    """Load RO remarks dynamically from Excel file - returns latest remarks"""
    try:
        remark_file = None
        for fn in ['RO Remark.xlsx', 'RO_Remark.xlsx', 'ro_remark.xlsx']:
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
        print(f"Error loading RO remarks dynamically: {e}")
        return []


def map_ro_remark(remark):
    """
    Map RO Remarks to standard codes (case-insensitive)
    Searches for any standard remark code within the remarks text
    Returns the first match found, or "Not Assigned" if no match
    """
    global ro_remarks_list

    if pd.isna(remark) or str(remark).strip() in ['', '-']:
        return 'Not Assigned'

    # Load latest remarks from file
    remarks_list = load_ro_remarks_dynamically()
    if not remarks_list:
        remarks_list = ro_remarks_list  # fallback

    remark_str = str(remark).strip().upper()

    for standard_remark in remarks_list:
        if str(standard_remark).strip() == "":
            continue
        standard_upper = str(standard_remark).upper()
        if standard_upper in remark_str:
            return standard_remark

    return 'Not Assigned'


def get_all_ro_remarks_for_dropdown():
    """Get all RO remarks for dropdown - both standard and those found in data"""
    try:
        standard_remarks = load_ro_remarks_dynamically()

        if df_global is not None and not df_global.empty and 'ro_remark_mapped' in df_global.columns:
            mapped_remarks = [str(x) for x in df_global['ro_remark_mapped'].dropna().unique() if x != 'Not Assigned']
            all_remarks = list(set(standard_remarks + mapped_remarks))
            all_remarks = [x for x in all_remarks if str(x).strip() != ""]
            return sorted(all_remarks)

        return sorted(standard_remarks) if standard_remarks else ['All']
    except Exception as e:
        print(f"Error getting all RO remarks: {e}")
        return ['All']


# ==================== MJOB HELPERS ====================

def extract_mjobs(remark):
    """Extract MJob codes from remarks"""
    if pd.isna(remark) or remark in ['-', '']:
        return None

    remark_str = str(remark).strip()
    matches = re.findall(r'\bM/?[1-4]\b', remark_str)
    return matches if matches else None


# ==================== LOAD DATA ====================

def load_data():
    """Load Excel files and merge data"""
    global df_global, df_landed_cost, df_billable_type, df_model_group, ro_remarks_list

    try:
        # ==================== Load Standard RO Remarks ====================
        print("[OK] Loading standard RO Remarks list...")
        remark_file = None
        for fn in ['RO Remark.xlsx', 'RO_Remark.xlsx', 'ro_remark.xlsx']:
            if os.path.exists(fn):
                remark_file = fn
                break

        if remark_file:
            print(f"[OK] Loading: {remark_file}")
            df_remarks = pd.read_excel(remark_file)
            remarks_col = df_remarks.columns[0]
            ro_remarks_list = [str(x).strip() for x in df_remarks[remarks_col].dropna().unique()]
            ro_remarks_list = [x for x in ro_remarks_list if x != ""]
            print(f"[OK] Loaded {len(ro_remarks_list)} standard RO Remarks")
        else:
            print("RO Remark file not found - RO Remark mapping will not be available")
            ro_remarks_list = []

        # ==================== Load Model Group mapping ====================
        model_file = None
        for fn in ['Model Group.xlsx', 'Model_Group.xlsx', 'model_group.xlsx']:
            if os.path.exists(fn):
                model_file = fn
                break

        if model_file:
            print(f"[OK] Loading: {model_file}")
            df_model_group = pd.read_excel(model_file)
            print(f"[OK] Loaded {len(df_model_group)} model group records")
        else:
            print("Model Group Excel file not found - Model Group and Segment will not be enriched")
            df_model_group = pd.DataFrame()

        # ==================== Load Open RO data ====================
        excel_file = None
        for fn in ['Open RO.xlsx', 'Open_RO.xlsx', 'open_ro.xlsx']:
            if os.path.exists(fn):
                excel_file = fn
                break

        if excel_file is None:
            print("Open RO Excel file not found")
            df_global = pd.DataFrame()
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
            return

        print(f"[OK] Loading: {excel_file}")
        df_global = pd.read_excel(excel_file)
        print(f"[OK] Loaded {len(df_global)} rows, {len(df_global.columns)} cols")

        # ==================== Fix DATE columns once (IMPORTANT for sorting) ====================
        if 'RO Date' in df_global.columns:
            df_global['RO Date_dt'] = build_datetime_column(df_global['RO Date'])
        else:
            df_global['RO Date_dt'] = pd.NaT

        if 'Vehicle  Ready Date' in df_global.columns:
            df_global['Vehicle Ready Date_dt'] = build_datetime_column(df_global['Vehicle  Ready Date'])
        else:
            df_global['Vehicle Ready Date_dt'] = pd.NaT

        # ==================== Add RO Remark Mapping Column ====================
        if 'RO Remarks' in df_global.columns:
            print("[OK] Mapping RO Remarks...")
            df_global['ro_remark_mapped'] = df_global['RO Remarks'].apply(map_ro_remark)
            print("[OK] RO Remark mapping complete")
        else:
            df_global['ro_remark_mapped'] = 'Not Assigned'

        # ==================== Merge Model Group ====================
        if df_model_group is not None and not df_model_group.empty:
            try:
                if 'Model Group' in df_global.columns:
                    segment_mapping = dict(zip(df_model_group['Model Code'], df_model_group['Segment']))
                    mg_mapping = dict(zip(df_model_group['Model Code'], df_model_group['Model Group']))

                    df_global['segment'] = df_global['Model Group'].map(segment_mapping)
                    df_global['model_group_mapped'] = df_global['Model Group'].map(mg_mapping)

                    df_global['segment'] = df_global['segment'].fillna('Unknown')
                    df_global['model_group_mapped'] = df_global['model_group_mapped'].fillna(df_global['Model Group'])
                    df_global['Model Group'] = df_global['model_group_mapped']
                else:
                    df_global['segment'] = 'Unknown'
            except Exception as e:
                print(f"Error during model mapping: {str(e)}")
                df_global['segment'] = 'Unknown'
        else:
            df_global['segment'] = 'Unknown'

        # ==================== Load and aggregate Landed Cost data ====================
        parts_file = None
        for fn in ['Part Issue But Not Bill.xlsx', 'Part_Issue_But_Not_Bill.xlsx', 'part_issue_but_not_bill.xlsx']:
            if os.path.exists(fn):
                parts_file = fn
                break

        if parts_file is None:
            print("Part Issue file not found - Landed Cost and Billable Type will not be available")
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
            df_global['total_landed_cost'] = 0
            df_global['billable_type'] = 'Not Billed'
        else:
            print(f"[OK] Loading: {parts_file}")
            df_parts = pd.read_excel(parts_file)

            df_landed_cost = df_parts.groupby('RO Number')['Landed Cost (Total)'].sum().reset_index()
            df_landed_cost.columns = ['RO ID', 'total_landed_cost']

            df_billable_type = df_parts.groupby('RO Number')['Billable Type'].first().reset_index()
            df_billable_type.columns = ['RO ID', 'billable_type']

            df_global = df_global.merge(df_landed_cost, on='RO ID', how='left')
            df_global['total_landed_cost'] = df_global['total_landed_cost'].fillna(0)

            df_global = df_global.merge(df_billable_type, on='RO ID', how='left')
            df_global['billable_type'] = df_global['billable_type'].fillna('Not Billed')

        # Final safety
        if 'RO Date_dt' not in df_global.columns:
            df_global['RO Date_dt'] = pd.NaT

    except Exception as e:
        print(f"[ERROR] load_data failed: {str(e)}")
        import traceback
        traceback.print_exc()
        df_global = pd.DataFrame()
        df_landed_cost = pd.DataFrame()
        df_billable_type = pd.DataFrame()


load_data()


# ==================== JSON CONVERTER ====================

def convert_row(row) -> Dict[str, Any]:
    """Convert pandas row to JSON-safe dict"""
    def safe_fmt(dt_value):
        if pd.isna(dt_value):
            return '-'
        try:
            return pd.Timestamp(dt_value).strftime('%Y-%m-%d')
        except:
            return '-'

    return {
        'ro_id': str(row.get('RO ID', '-')).strip() if pd.notna(row.get('RO ID', None)) else '-',
        'branch': str(row.get('Branch', '-')).strip() if pd.notna(row.get('Branch', None)) else '-',
        'ro_status': str(row.get('RO Status', '-')).strip() if pd.notna(row.get('RO Status', None)) else '-',
        'age_bucket': str(row.get('Age Bucket', '-')).strip() if pd.notna(row.get('Age Bucket', None)) else '-',
        'service_category': str(row.get('SERVC_CATGRY_DESC', '-')).strip() if pd.notna(row.get('SERVC_CATGRY_DESC', None)) else '-',
        'service_type': str(row.get('SERVC_TYPE_DESC', '-')).strip() if pd.notna(row.get('SERVC_TYPE_DESC', None)) else '-',
        'vehicle_model': str(row.get('Family', '-')).strip() if pd.notna(row.get('Family', None)) else '-',
        'model_group': str(row.get('Model Group', '-')).strip() if pd.notna(row.get('Model Group', None)) else '-',
        'segment': str(row.get('segment', 'Unknown')).strip() if pd.notna(row.get('segment', None)) else 'Unknown',
        'reg_number': str(row.get('Reg. Number', '-')).strip() if pd.notna(row.get('Reg. Number', None)) else '-',
        'ro_date': safe_fmt(row.get('RO Date_dt', pd.NaT)),
        'vehicle_ready_date': safe_fmt(row.get('Vehicle Ready Date_dt', pd.NaT)),
        'ro_remarks': str(row.get('RO Remarks', '-')).strip() if pd.notna(row.get('RO Remarks', None)) else '-',
        'ro_remark_mapped': str(row.get('ro_remark_mapped', 'Not Assigned')).strip() if pd.notna(row.get('ro_remark_mapped', None)) else 'Not Assigned',
        'km': int(row.get('KM', 0)) if pd.notna(row.get('KM', None)) else 0,
        'days': int(row.get('Days', 0)) if pd.notna(row.get('Days', None)) else 0,
        'days_open': int(row.get('[No of Visits (In last 90 days)]', 0)) if pd.notna(row.get('[No of Visits (In last 90 days)]', None)) else 0,
        'service_adviser': str(row.get('Service Adviser Name', '-')).strip() if pd.notna(row.get('Service Adviser Name', None)) else '-',
        'vin': str(row.get('VIN', '-')).strip() if pd.notna(row.get('VIN', None)) else '-',
        'pendncy_resn_desc': str(row.get('PENDNCY_RESN_DESC', '-')).strip() if pd.notna(row.get('PENDNCY_RESN_DESC', None)) else '-',
        'total_landed_cost': round(float(row.get('total_landed_cost', 0.0)), 2) if pd.notna(row.get('total_landed_cost', None)) else 0.00,
        'billable_type': str(row.get('billable_type', 'Not Billed')).strip() if pd.notna(row.get('billable_type', None)) else 'Not Billed',
    }


# ==================== FILTERS ====================

def apply_filters(df, branch, ro_status, age_bucket, mjob=None, billable_type=None, reg_number=None,
                  service_type=None, sa_name=None, segment=None, ro_remark=None, pending_reason=None,
                  from_date=None, to_date=None):
    """Apply filters to dataframe"""
    result = df.copy()

    if branch and branch != "All":
        result = result[result['Branch'] == branch]

    if ro_status and ro_status != "All":
        result = result[result['RO Status'] == ro_status]

    if age_bucket and age_bucket != "All":
        result = result[result['Age Bucket'] == age_bucket]

    if billable_type and billable_type != "All":
        result = result[result['billable_type'] == billable_type]

    if service_type and service_type != "All":
        result = result[result['SERVC_TYPE_DESC'] == service_type]

    if sa_name and sa_name != "All":
        result = result[result['Service Adviser Name'] == sa_name]

    if segment and segment != "All":
        result = result[result['segment'] == segment]

    if ro_remark and ro_remark != "All":
        result = result[result['ro_remark_mapped'] == ro_remark]

    if pending_reason and pending_reason != "All":
        result = result[result['PENDNCY_RESN_DESC'] == pending_reason]

    # Date range filtering using parsed datetime column
    if from_date:
        from_dt = parse_date(from_date)
        if from_dt is not None:
            result = result[result['RO Date_dt'] >= from_dt]

    if to_date:
        to_dt = parse_date(to_date)
        if to_dt is not None:
            result = result[result['RO Date_dt'] <= to_dt]

    if mjob and mjob != "All":
        if mjob == "Not Categorized":
            result = result[result['RO Remarks'].apply(lambda x: extract_mjobs(x) is None)]
        else:
            search_mjob = mjob.upper()
            result = result[result['RO Remarks'].apply(
                lambda x: any(m.upper() in [search_mjob, search_mjob.replace('/', '')]
                              for m in (extract_mjobs(x) or []))
            )]

    if reg_number and reg_number.strip() != "":
        search_reg = reg_number.strip().upper()
        result = result[result['Reg. Number'].astype(str).str.upper().str.contains(search_reg, na=False)]

    return result


# ==================== SORTING (FIXED) ====================

def apply_sorting(df: pd.DataFrame, sort_by: str, sort_dir: str) -> pd.DataFrame:
    """
    sort_dir:
      - 'desc' => latest date first (current date on top)
      - 'asc'  => oldest date first
    """
    sort_dir = (sort_dir or "desc").lower()
    ascending = True if sort_dir == "asc" else False

    sort_by = (sort_by or "ro_date").lower().strip()

    # Map UI sort keys to dataframe columns
    if sort_by in ["ro_date", "ro date", "date"]:
        col = "RO Date_dt"
        if col in df.columns:
            return df.sort_values(col, ascending=ascending, na_position="last")

    if sort_by in ["vehicle_ready_date", "vehicle ready date", "ready_date"]:
        col = "Vehicle Ready Date_dt"
        if col in df.columns:
            return df.sort_values(col, ascending=ascending, na_position="last")

    if sort_by in ["landed_cost", "total_landed_cost", "landed cost"]:
        col = "total_landed_cost"
        if col in df.columns:
            return df.sort_values(col, ascending=ascending, na_position="last")

    # fallback: no sorting applied
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


# ==================== VEHICLE ENDPOINTS (WITH SORTING) ====================

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
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])].copy()
        total = len(df)

        df = apply_filters(
            df, branch, ro_status, age_bucket,
            billable_type=billable_type, service_type=service_type, sa_name=sa_name,
            reg_number=reg_number, segment=segment, ro_remark=ro_remark,
            pending_reason=pending_reason, from_date=from_date, to_date=to_date
        )
        filtered = len(df)

        df = apply_sorting(df, sort_by, sort_dir)
        df = df.iloc[skip:skip + limit]

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
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'].copy()
        total = len(df)

        df = apply_filters(
            df, branch, ro_status, age_bucket, mjob,
            billable_type=billable_type, reg_number=reg_number, sa_name=sa_name,
            segment=segment, ro_remark=ro_remark, from_date=from_date, to_date=to_date
        )
        filtered = len(df)

        df = apply_sorting(df, sort_by, sort_dir)
        df = df.iloc[skip:skip + limit]

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
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories'].copy()
        total = len(df)

        df = apply_filters(
            df, branch, ro_status, age_bucket,
            billable_type=billable_type, sa_name=sa_name, segment=segment,
            ro_remark=ro_remark, from_date=from_date, to_date=to_date
        )
        filtered = len(df)

        df = apply_sorting(df, sort_by, sort_dir)
        df = df.iloc[skip:skip + limit]

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
    sort_by: Optional[str] = Query("ro_date"),
    sort_dir: Optional[str] = Query("desc"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    try:
        if df_global is None or df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Pre-Sale/PDI'].copy()
        total = len(df)

        df = apply_filters(
            df, branch, ro_status, age_bucket,
            billable_type=billable_type, sa_name=sa_name, segment=segment,
            ro_remark=ro_remark, from_date=from_date, to_date=to_date
        )
        filtered = len(df)

        df = apply_sorting(df, sort_by, sort_dir)
        df = df.iloc[skip:skip + limit]

        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_presale: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}


# ==================== DASHBOARD HTML ====================

@app.get("/")
async def dashboard():
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return {"error": "dashboard.html not found"}


@app.get("/health")
async def health():
    return {"status": "healthy", "records": len(df_global) if df_global is not None and not df_global.empty else 0}


# ==================== MAIN ====================

if __name__ == "__main__":
    import uvicorn
    print(f"Running on http://0.0.0.0:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)

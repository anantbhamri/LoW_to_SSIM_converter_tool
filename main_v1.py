import csv
from datetime import datetime, timedelta

# Handle timezone libraries with fallbacks
try:
    import airportsdata
    AIRPORTSDATA_AVAILABLE = True
except ImportError:
    print("Warning: airportsdata not available. Install with: pip install airportsdata")
    AIRPORTSDATA_AVAILABLE = False

try:
    from zoneinfo import ZoneInfo
    ZONEINFO_AVAILABLE = True
except ImportError:
    try:
        ZONEINFO_AVAILABLE = True
    except ImportError:
        print("Warning: zoneinfo not available. Using static UTC offsets.")
        ZONEINFO_AVAILABLE = False

fileName = 'LOW_1day_73J_test_v0.txt'

# LOW Actual Location
LOW_FILENAME = 'data/' + fileName
SSIM_OUT_FILENAME = 'data/output_' + fileName

def parse_flexible_date(date_string):
    """
    Parse date string that can be in multiple formats:
    - "20Dec25" (%d%b%y format)
    - "10/09/2025" (%m/%d/%Y format)
    - "12/25/25" (%m/%d/%y format) 
    - Add more formats as needed
    
    Args:
        date_string: Date string in various formats
    
    Returns:
        datetime object or None if parsing fails
    """
    if not date_string or date_string.strip() == '':
        return None
        
    date_string = date_string.strip()
    
    # List of possible date formats to try
    date_formats = [
        "%d%b%y",      # 20Dec25
        "%m/%d/%Y",    # 10/09/2025
        "%m/%d/%y",    # 10/09/25
        "%d-%b-%y",    # 20-Dec-25
        "%d/%m/%Y",    # 20/12/2025 (European format)
        "%Y-%m-%d",    # 2025-12-20 (ISO format)
        "%d%b%Y",      # 20Dec2025
        "%m-%d-%Y",    # 10-09-2025
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue  # Try next format
    
    # If no format worked, return None
    print(f"Warning: Could not parse date '{date_string}' with any known format")
    return None

def extract_airport_codes_and_date_range(filename):
    """Extract all unique airport codes from LoW file AND find the first and last flight dates"""
    codes = set()
    all_dates = []
    
    try:
        with open(filename, 'r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                codes.add(row["Dept Sta"].upper())
                codes.add(row["Arvl Sta"].upper())
                
                # Collect all departure and arrival dates using flexible parsing
                if row.get("Dept Date"):
                    dept_date = parse_flexible_date(row["Dept Date"])
                    if dept_date:
                        all_dates.append(dept_date)
                    else:
                        print(f"Warning: Could not parse dept date '{row['Dept Date']}'")
                        
                if row.get("Arvl Date"):
                    arvl_date = parse_flexible_date(row["Arvl Date"])
                    if arvl_date:
                        all_dates.append(arvl_date)
                    else:
                        print(f"Warning: Could not parse arvl date '{row['Arvl Date']}'")
                        
    except FileNotFoundError:
        print(f"Error: File {filename} not found!")
        return set(), None, None
    
    codes.discard('')
    
    # Find first and last dates
    if all_dates:
        first_date = min(all_dates)
        last_date = max(all_dates)
        print(f"Schedule period: {first_date.strftime('%d%b%y')} to {last_date.strftime('%d%b%y')}")
    else:
        # If no valid dates found, use reasonable defaults
        first_date = datetime.now()
        last_date = first_date + timedelta(days=1)
        print(f"Warning: No valid dates found in LoW file, using default range")
    
    return codes, first_date, last_date


def calculate_airport_utc_offsets(airport_codes, reference_date):
    """
    Dynamically calculate UTC offsets for all airports in the LoW file using the actual flight date.
    Uses airportsdata + zoneinfo for accurate timezone calculations including DST.
    Falls back to estimated offsets if libraries unavailable.
    """
    offsets = {}
    
    if AIRPORTSDATA_AVAILABLE and ZONEINFO_AVAILABLE:
        print(f"Calculating dynamic timezone offsets using flight date: {reference_date.strftime('%d%b%y %H:%M UTC')}")
        airports = airportsdata.load('IATA')
        
        for code in airport_codes:
            info = airports.get(code)
            if not info or not info.get('tz'):
                # Fallback estimation based on US geography
                offsets[code] = estimate_us_timezone_offset(code, reference_date)
                print(f"  {code}: Using estimated offset {offsets[code]} hours (no timezone data)")
            else:
                try:
                    tz = ZoneInfo(info['tz'])
                    # Use the actual flight date instead of current time for DST accuracy
                    offset_hours = tz.utcoffset(reference_date).total_seconds() / 3600
                    offsets[code] = offset_hours
                    dst_indicator = "DST" if tz.dst(reference_date) else "STD"
                    print(f"  {code}: {offset_hours:+.1f} hours ({info['tz']} - {dst_indicator})")
                except Exception as e:
                    offsets[code] = estimate_us_timezone_offset(code, reference_date)
                    print(f"  {code}: Using estimated offset {offsets[code]} hours (timezone error: {e})")
    else:
        print(f"Using estimated timezone offsets for date: {reference_date.strftime('%d%b%y')} (airportsdata/zoneinfo unavailable)...")
        for code in airport_codes:
            offsets[code] = estimate_us_timezone_offset(code, reference_date)
            print(f"  {code}: {offsets[code]} hours (estimated)")
    
    return offsets

def estimate_us_timezone_offset(airport_code, reference_date):
    """
    Estimate US timezone offset based on airport code and date (considering DST).
    This is a fallback when timezone libraries are unavailable.
    """
    # Determine if DST is in effect (rough approximation for US)
    # DST typically runs from 2nd Sunday in March to 1st Sunday in November
    year = reference_date.year
    month = reference_date.month
    
    # Simple DST check: March through October (rough approximation)
    is_dst = month >= 3 and month <= 10
    
    # Eastern Time
    eastern_airports = {
        'ATL', 'DTW', 'CHA', 'CLE', 'CVG', 'PIT', 'RIC', 'ORF', 'BWI', 
        'BDL', 'PHL', 'BUF', 'JFK', 'LGA', 'EWR', 'BOS', 'MIA', 'FLL', 
        'TPA', 'MCO', 'TLH', 'CHS', 'GRR', 'RSW', 'SRQ'
    }
    
    # Central Time
    central_airports = {
        'MEM', 'MSP', 'ORD', 'DFW', 'MCI', 'STL', 'MSY', 'IAH', 'HOU',
        'SAT', 'AUS', 'OKC', 'TUL', 'LIT', 'BHM', 'MOB', 'HSV'
    }
    
    # Mountain Time
    mountain_airports = {
        'DEN', 'PHX', 'SLC', 'ABQ', 'BOI', 'BIL', 'GJT', 'COS'
    }
    
    # Pacific Time
    pacific_airports = {
        'LAX', 'SFO', 'SEA', 'LAS', 'SAN', 'PDX', 'SMF', 'OAK', 'SJC'
    }
    
    # Calculate offset with DST consideration
    if airport_code in eastern_airports:
        return -4.0 if is_dst else -5.0  # EDT vs EST
    elif airport_code in central_airports:
        return -5.0 if is_dst else -6.0  # CDT vs CST
    elif airport_code in mountain_airports:
        # Note: Arizona (PHX) doesn't observe DST
        if airport_code == 'PHX':
            return -7.0  # Always MST
        else:
            return -6.0 if is_dst else -7.0  # MDT vs MST
    elif airport_code in pacific_airports:
        return -7.0 if is_dst else -8.0  # PDT vs PST
    else:
        # Default to Eastern time for unknown airports
        return -4.0 if is_dst else -5.0

def convert_date_to_utc_if_needed(local_datetime, timezone_offset, time_zone_mode):
    """
    Convert local datetime to UTC if time_zone_mode is 'U', otherwise keep local.
    
    Args:
        local_datetime: datetime object in local time
        timezone_offset: UTC offset in hours (e.g., -5.0 for EST)
        time_zone_mode: 'L' for Local time, 'U' for UTC time
    
    Returns:
        datetime object (either local or UTC depending on mode)
    """
    if time_zone_mode == 'U' and timezone_offset is not None:
        # Convert local time to UTC by subtracting the offset
        utc_datetime = local_datetime - timedelta(hours=timezone_offset)
        return utc_datetime
    else:
        # Keep local time
        return local_datetime

def find_operation_date_range(all_legs, time_zone_mode='L'):
    """
    Find the first and last date of operation, considering timezone conversion if needed.
    
    Args:
        all_legs: List of flight leg dictionaries
        time_zone_mode: 'L' for Local time, 'U' for UTC time
    
    Returns:
        tuple: (first_operation_date, last_operation_date) as datetime objects
    """
    all_operation_dates = []
    
    for leg in all_legs:
        dept_dt = leg.get("Dept DateTime")
        arvl_dt = leg.get("Arvl DateTime")
        dept_offset = leg.get("Dept UTC Offset")
        arvl_offset = leg.get("Arvl UTC Offset")
        
        if dept_dt:
            operation_date = convert_date_to_utc_if_needed(dept_dt, dept_offset, time_zone_mode)
            all_operation_dates.append(operation_date)
            
        if arvl_dt:
            operation_date = convert_date_to_utc_if_needed(arvl_dt, arvl_offset, time_zone_mode)
            all_operation_dates.append(operation_date)
    
    if all_operation_dates:
        first_op_date = min(all_operation_dates)
        last_op_date = max(all_operation_dates)
        return first_op_date, last_op_date
    else:
        # Fallback to current date
        now = datetime.now()
        return now, now + timedelta(days=1)


def read_low_csv(filename, airport_offsets):
    """Read LoW CSV and organize by aircraft rotations (shells)"""
    all_legs = []
    shells = {}
    
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Clean and pad times
            dept_time = row["Dept Time"].zfill(4)
            arvl_time = row["Arvl Time"].zfill(4)
            
            # Parse datetime objects using flexible date parsing
            dept_dt = None
            arvl_dt = None
            
            # Parse departure datetime
            dept_date_obj = parse_flexible_date(row.get('Dept Date', ''))
            if dept_date_obj:
                try:
                    # Combine date with time
                    dept_dt = dept_date_obj.replace(
                        hour=int(dept_time[:2]), 
                        minute=int(dept_time[2:])
                    )
                except Exception as e:
                    print(f"Warning: Could not combine departure date/time: {row.get('Dept Date', '')} {dept_time}")
            
            # Parse arrival datetime  
            arvl_date_obj = parse_flexible_date(row.get('Arvl Date', ''))
            if arvl_date_obj:
                try:
                    # Combine date with time
                    arvl_dt = arvl_date_obj.replace(
                        hour=int(arvl_time[:2]), 
                        minute=int(arvl_time[2:])
                    )
                except Exception as e:
                    print(f"Warning: Could not combine arrival date/time: {row.get('Arvl Date', '')} {arvl_time}")
            
            # Get calculated UTC offsets
            dept_offset = airport_offsets.get(row["Dept Sta"].upper())
            arvl_offset = airport_offsets.get(row["Arvl Sta"].upper())
            
            # Create leg dictionary with all original data plus calculated fields
            leg = {
                **row,  # Include all original LoW columns
                "Dept DateTime": dept_dt,
                "Arvl DateTime": arvl_dt,
                "Dept UTC Offset": dept_offset,
                "Arvl UTC Offset": arvl_offset,
                "Dept Time": dept_time,
                "Arvl Time": arvl_time,
            }
            
            all_legs.append(leg)
            
            # Group by Line Num (aircraft rotation)
            shell_id = row["Line Num"]
            if shell_id not in shells:
                shells[shell_id] = []
            shells[shell_id].append(leg)
    
    # Sort legs within each shell by Leg Seq num
    for shell in shells.values():
        shell.sort(key=lambda x: int(x.get("Leg Seq num", "1")))
    
    print(f"Read {len(all_legs)} flight legs organized into {len(shells)} aircraft rotations")
    for shell_id, legs in shells.items():
        print(f"  Line {shell_id}: {len(legs)} flights")
    
    return all_legs, shells


def pad_right(s, length):
    return str(s)[:length].ljust(length)

def pad_left(s, length, fill=' '):
    return str(s)[-length:].rjust(length, fill)

def format_ssim_date(dt):
    return dt.strftime('%d%b%y').upper() if dt else ''.ljust(7)

def format_utc_offset(offset):
    """Format UTC offset as SSIM-compatible string (e.g., -0500)"""
    if offset is None:
        return '0000'
    sign = '-' if offset < 0 else '+'
    abs_offset = abs(offset)
    hours = int(abs_offset)
    mins = int(round((abs_offset - hours) * 60))
    return f"{sign}{hours:02d}{mins:02d}"

def ssim_time_field(t):
    return str(t).zfill(4)

def write_ssim_with_segments_v0(all_legs, shells, filename, schedule_first_date, schedule_last_date):
    """Write SSIM file with Type 3 and Type 4 records"""
    
    # Time zone mode: L for local, U for UTC
    time_zone = 'L'
    
    # Find operation date range (first and last actual operation dates)
    first_op_date, last_op_date = find_operation_date_range(all_legs, time_zone)
    
    print(f"Schedule period: {schedule_first_date.strftime('%d%b%y')} to {schedule_last_date.strftime('%d%b%y')}")
    print(f"Operation period ({'Local' if time_zone == 'L' else 'UTC'}): {first_op_date.strftime('%d%b%y')} to {last_op_date.strftime('%d%b%y')}")
    
    with open(filename, 'w') as out:
        # SSIM Header Type 1
        out.write("1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001\n")
        
        # Add 4 lines of 200 characters with all zeros
        for _ in range(4):
            out.write("0" * 200 + "\n")
        
        # SSIM Header Type 2 - with actual date ranges
        today_date = datetime.now()
        header2 = f"2{time_zone}DL  0008    {first_op_date.strftime('%d%b%y').upper()}{last_op_date.strftime('%d%b%y').upper()}{today_date.strftime('%d%b%y').upper()}                                    PCreated by Python LoW-to-SSIM Converter"
        header2 = pad_right(header2, 188) + "EN1140000002"
        out.write(header2 + "\n")
        
        # Add 4 more lines of 200 characters with all zeros
        for _ in range(4):
            out.write("0" * 200 + "\n")

        record_serial = 3
        
        for shell_id, shell_legs in shells.items():
            for idx, leg in enumerate(shell_legs):
                # Find next leg (onward flight) in this shell
                next_leg = shell_legs[idx+1] if idx + 1 < len(shell_legs) else None

                # ===== EXTRACT RAW VALUES (NO PADDING) =====
                
                # Basic flight information
                raw_airline_code = leg.get('Aln', 'DL')
                raw_flight_number = leg.get('Flt Num', '')
                raw_itinerary_var = leg.get('Line Num', '1')
                raw_leg_seq_num = leg.get('Leg Seq num', '1')
                raw_service_type = leg.get('Service Type', 'J')
                
                # Date and time information
                raw_dep_dt = leg["Dept DateTime"]
                raw_arr_dt = leg["Arvl DateTime"]
                raw_dep_date_str = format_ssim_date(raw_dep_dt)
                raw_arr_date_str = format_ssim_date(raw_arr_dt)
                raw_day_of_week = raw_dep_dt.strftime('%u') if raw_dep_dt else '1'
                
                # Airport and time information
                raw_dep_airport = leg.get('Dept Sta', '')
                raw_arr_airport = leg.get('Arvl Sta', '')
                raw_dep_time = leg["Dept Time"]
                raw_arr_time = leg["Arvl Time"]
                
                # UTC offsets
                raw_dep_offset_hours = leg["Dept UTC Offset"]
                raw_arr_offset_hours = leg["Arvl UTC Offset"]
                
                # Aircraft information
                raw_ac_type = leg.get('Equip', '73J')
                raw_ac_config = leg.get('AC Config', '')
                raw_ac_owner = '' #leg.get('A/C Own', '')
                
                # Next flight (onward) information
                raw_onward_airline = next_leg.get('Aln', '') if next_leg else ''
                raw_onward_fltno = next_leg.get('Flt Num', '') if next_leg else ''
                raw_onward_leg_var = '' #next_leg.get('Line Num', '') if next_leg else ''
                raw_onward_leg_seq = '' #next_leg.get('Leg Seq num', '') if next_leg else ''
                
                # Convert raw values to formatted strings
                formatted_dep_time = ssim_time_field(raw_dep_time)
                formatted_arr_time = ssim_time_field(raw_arr_time)
                formatted_dep_offset = format_utc_offset(raw_dep_offset_hours)
                formatted_arr_offset = format_utc_offset(raw_arr_offset_hours)
                formatted_period_of_op = raw_dep_date_str + raw_dep_date_str
                
                # reason code 
                reason_code = leg.get('REASON CODE', ' ')
                if reason_code == 'CRWB':
                    debug_point = 1
                # ===== BUILD TYPE 3 RECORD WITH PROPER PADDING =====
                
                rec = ''
                rec += pad_right('3', 1)                                    # Pos 1: Record Type
                rec += pad_right('', 1)                                     # Pos 2: Operational Suffix
                rec += pad_right(raw_airline_code, 3)                       # Pos 3-5: Airline Code
                rec += pad_left(raw_flight_number, 4)                       # Pos 6-9: Flight Number
                rec += pad_left(raw_itinerary_var, 2, '0')                  # Pos 10-11: Itinerary Variant
                rec += pad_left(raw_leg_seq_num, 2, '0')                    # Pos 12-13: Leg Sequence
                rec += pad_right(raw_service_type, 1)                       # Pos 14: Service Type
                rec += pad_right(formatted_period_of_op, 14)                # Pos 15-28: Period of Operation
                rec += pad_left(raw_day_of_week, 7)                         # Pos 29-35: Days of Operation
                rec += pad_right('', 1)                                     # Pos 36: Frequency Rate
                rec += pad_right(raw_dep_airport, 3)                        # Pos 37-39: Departure Airport
                rec += pad_right(formatted_dep_time, 4)                     # Pos 40-43: Departure Time
                rec += pad_right(formatted_dep_time, 4)                     # Pos 44-47: Departure Time (repeat)
                rec += pad_right(formatted_dep_offset, 5)                   # Pos 48-52: Departure UTC Offset
                rec += pad_right('', 2)                                     # Pos 53-54: Departure Terminal
                rec += pad_right(raw_arr_airport, 3)                        # Pos 55-57: Arrival Airport
                rec += pad_right(formatted_arr_time, 4)                     # Pos 58-61: Arrival Time
                rec += pad_right(formatted_arr_time, 4)                     # Pos 62-65: Arrival Time (repeat)
                rec += pad_right(formatted_arr_offset, 5)                   # Pos 66-70: Arrival UTC Offset
                rec += pad_right('', 2)                                     # Pos 71-72: Arrival Terminal
                rec += pad_right(raw_ac_type, 3)                            # Pos 73-75: Aircraft Type
                rec += pad_right('', 23)                                    # Pos 76-98: Various fields (empty)
                rec += pad_right(raw_ac_owner, 3)                           # Pos 99-101: Aircraft Owner
                rec += pad_right('', 36)                                    # Pos 102-137: More fields (empty)
                rec += pad_right(raw_onward_airline, 3)                     # Pos 138-140: Onward Airline
                rec += pad_left(raw_onward_fltno, 4)                        # Pos 141-144: Onward Flight Number
                rec += pad_left(raw_onward_leg_var, 1)                      # Pos 145-145: Aircraft Rotation Layover
                rec += pad_left(raw_onward_leg_seq, 1)                      # Pos 146-146: Operational Suffix
                rec += pad_right('', 26)                                    # Pos 147-172: More fields (empty)
                rec += pad_right(raw_ac_config + 'VV10', 20)                # Pos 173-192: Aircraft Configuration
                rec += pad_right('', 2)                                     # Pos 193-194: Date variation
                rec += pad_left(str(record_serial), 6, '0')                 # Pos 195-200: Record Serial Number
                
                # Ensure exactly 200 characters
                rec = pad_right(rec, 200)
                out.write(rec + '\n')
                record_serial += 1

                # ===== BUILD TYPE 4 RECORD WITH PROPER PADDING =====
                
                seg = ''
                seg += pad_right('4', 1)                                    # Pos 1: Record Type
                seg += pad_right('', 1)                                     # Pos 2: Operational Suffix
                seg += pad_right(raw_airline_code, 3)                       # Pos 3-5: Airline Code
                seg += pad_left(raw_flight_number, 4)                       # Pos 6-9: Flight Number
                seg += pad_left(raw_itinerary_var, 2, '0')                  # Pos 10-11: Itinerary Variant
                seg += pad_left(raw_leg_seq_num, 2, '0')                    # Pos 12-13: Leg Sequence
                seg += pad_right(raw_service_type, 1)                       # Pos 14: Service Type
                seg += pad_right('', 13)                                    # Pos 15-27: Period fields (empty)
                seg += pad_right('', 1)                                     # Pos 28: Itinerary Variant Overflow
                seg += pad_right('A', 1)                                    # Pos 29: Board Point Indicator
                seg += pad_right('B', 1)                                    # Pos 30: Off Point Indicator
                seg += pad_left('997', 3, '0')                              # Pos 31-33: Data Element ID
                seg += pad_right(raw_dep_airport, 3)                        # Pos 34-36: Board Point
                seg += pad_right(raw_arr_airport, 3)                        # Pos 37-39: Off Point
                
                seg += pad_right(reason_code, 155)                          # Pos 40 - 194: Data (associated with Data Element Identifier)
                
                seg += pad_left(str(record_serial), 6, '0')                 # Pos 195-200: Record Serial Number
                
                # Ensure exactly 200 characters
                seg = pad_right(seg, 200)
                out.write(seg + '\n')
                record_serial += 1

        # SSIM Trailer
        out.write("5                                                                                            000999\n")

    print(f"✅ Wrote SSIM file: {filename}")
    print(f"   Generated {record_serial - 3} records (Type 3 and 4 pairs)")
    print(f"   Time zone mode: {'Local' if time_zone == 'L' else 'UTC'}")

def write_ssim_with_segments(all_legs, shells, filename, schedule_first_date, schedule_last_date):
    """Write SSIM file with Type 3 and Type 4 records"""
    
    # Time zone mode: L for local, U for UTC
    time_zone = 'L'
    
    # Find operation date range (first and last actual operation dates)
    first_op_date, last_op_date = find_operation_date_range(all_legs, time_zone)
    
    print(f"Schedule period: {schedule_first_date.strftime('%d%b%y')} to {schedule_last_date.strftime('%d%b%y')}")
    print(f"Operation period ({'Local' if time_zone == 'L' else 'UTC'}): {first_op_date.strftime('%d%b%y')} to {last_op_date.strftime('%d%b%y')}")
    
    with open(filename, 'w') as out:
        # SSIM Header Type 1
        out.write("1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001\n")
        
        # Add 4 lines of 200 characters with all zeros
        for _ in range(4):
            out.write("0" * 200 + "\n")
        
        # SSIM Header Type 2 - with actual date ranges
        today_date = datetime.now()
        header2 = f"2{time_zone}DL  0008    {first_op_date.strftime('%d%b%y').upper()}{last_op_date.strftime('%d%b%y').upper()}{today_date.strftime('%d%b%y').upper()}                                    PCreated by Python LoW-to-SSIM Converter"
        header2 = pad_right(header2, 188) + "EN1140000002"
        out.write(header2 + "\n")
        
        # Add 4 more lines of 200 characters with all zeros
        for _ in range(4):
            out.write("0" * 200 + "\n")

        record_serial = 3
        
        # ===== NEW CHAINED SHELL PROCESSING =====
        
        # Track which shells have been processed
        processed_shells = set()
        
        # Start with a random shell (or first available shell)
        available_shells = list(shells.keys())
        if not available_shells:
            print("No shells found!")
            return
            
        # Start with the first shell
        current_shell_id = available_shells[0]
        print(f"Starting aircraft chain with Shell {current_shell_id}")
        
        while len(processed_shells) < len(shells):
            # Get current shell and mark as processed
            current_shell_legs = shells[current_shell_id]
            processed_shells.add(current_shell_id)
            
            print(f"Processing Shell {current_shell_id} with {len(current_shell_legs)} flights")
            
            # Process all legs in current shell
            for idx, leg in enumerate(current_shell_legs):
                # Find next leg within current shell
                next_leg_in_shell = current_shell_legs[idx+1] if idx + 1 < len(current_shell_legs) else None
                
                # If this is the last leg of the shell, find the connecting shell
                next_leg = next_leg_in_shell
                if next_leg_in_shell is None:
                    # This is the last leg of current shell, find connecting shell
                    last_arrival_airport = leg.get('Arvl Sta', '')
                    connecting_shell_id = find_connecting_shell(shells, processed_shells, last_arrival_airport)
                    
                    if connecting_shell_id:
                        # Found a connecting shell - use its first flight as next leg
                        connecting_shell_legs = shells[connecting_shell_id]
                        next_leg = connecting_shell_legs[0] if connecting_shell_legs else None
                        print(f"  → Found connection: Shell {current_shell_id} → Shell {connecting_shell_id} at {last_arrival_airport}")
                    else:
                        # No connecting shell found
                        next_leg = None
                        print(f"  → End of chain at {last_arrival_airport}")

                # ===== EXTRACT RAW VALUES (NO PADDING) =====
                
                # Basic flight information
                raw_airline_code = leg.get('Aln', 'DL')
                raw_flight_number = leg.get('Flt Num', '')
                raw_itinerary_var = leg.get('Line Num', '1')
                raw_leg_seq_num = leg.get('Leg Seq num', '1')
                raw_service_type = leg.get('Service Type', 'J')
                
                # Date and time information
                raw_dep_dt = leg["Dept DateTime"]
                raw_arr_dt = leg["Arvl DateTime"]
                raw_dep_date_str = format_ssim_date(raw_dep_dt)
                raw_arr_date_str = format_ssim_date(raw_arr_dt)
                raw_day_of_week = raw_dep_dt.strftime('%u') if raw_dep_dt else '1'
                
                # Airport and time information
                raw_dep_airport = leg.get('Dept Sta', '')
                raw_arr_airport = leg.get('Arvl Sta', '')
                raw_dep_time = leg["Dept Time"]
                raw_arr_time = leg["Arvl Time"]
                
                # UTC offsets
                raw_dep_offset_hours = leg["Dept UTC Offset"]
                raw_arr_offset_hours = leg["Arvl UTC Offset"]
                
                # Aircraft information
                raw_ac_type = leg.get('Equip', '73J')
                raw_ac_config = leg.get('AC Config', '')
                raw_ac_owner = '' #leg.get('A/C Own', '')
                
                # Next flight (onward) information
                raw_onward_airline = next_leg.get('Aln', '') if next_leg else ''
                raw_onward_fltno = next_leg.get('Flt Num', '') if next_leg else ''
                raw_onward_leg_var = '' #next_leg.get('Line Num', '') if next_leg else ''
                raw_onward_leg_seq = '' #next_leg.get('Leg Seq num', '') if next_leg else ''
                
                # Convert raw values to formatted strings
                formatted_dep_time = ssim_time_field(raw_dep_time)
                formatted_arr_time = ssim_time_field(raw_arr_time)
                formatted_dep_offset = format_utc_offset(raw_dep_offset_hours)
                formatted_arr_offset = format_utc_offset(raw_arr_offset_hours)
                formatted_period_of_op = raw_dep_date_str + raw_dep_date_str
                
                # reason code 
                reason_code = leg.get('REASON CODE', ' ')
                if reason_code == 'CRWB':
                    debug_point = 1
                    
                # ===== BUILD TYPE 3 RECORD WITH PROPER PADDING =====
                
                rec = ''
                rec += pad_right('3', 1)                                    # Pos 1: Record Type
                rec += pad_right('', 1)                                     # Pos 2: Operational Suffix
                rec += pad_right(raw_airline_code, 3)                       # Pos 3-5: Airline Code
                rec += pad_left(raw_flight_number, 4)                       # Pos 6-9: Flight Number
                rec += pad_left(raw_itinerary_var, 2, '0')                  # Pos 10-11: Itinerary Variant
                rec += pad_left(raw_leg_seq_num, 2, '0')                    # Pos 12-13: Leg Sequence
                rec += pad_right(raw_service_type, 1)                       # Pos 14: Service Type
                rec += pad_right(formatted_period_of_op, 14)                # Pos 15-28: Period of Operation
                rec += pad_left(raw_day_of_week, 7)                         # Pos 29-35: Days of Operation
                rec += pad_right('', 1)                                     # Pos 36: Frequency Rate
                rec += pad_right(raw_dep_airport, 3)                        # Pos 37-39: Departure Airport
                rec += pad_right(formatted_dep_time, 4)                     # Pos 40-43: Departure Time
                rec += pad_right(formatted_dep_time, 4)                     # Pos 44-47: Departure Time (repeat)
                rec += pad_right(formatted_dep_offset, 5)                   # Pos 48-52: Departure UTC Offset
                rec += pad_right('', 2)                                     # Pos 53-54: Departure Terminal
                rec += pad_right(raw_arr_airport, 3)                        # Pos 55-57: Arrival Airport
                rec += pad_right(formatted_arr_time, 4)                     # Pos 58-61: Arrival Time
                rec += pad_right(formatted_arr_time, 4)                     # Pos 62-65: Arrival Time (repeat)
                rec += pad_right(formatted_arr_offset, 5)                   # Pos 66-70: Arrival UTC Offset
                rec += pad_right('', 2)                                     # Pos 71-72: Arrival Terminal
                rec += pad_right(raw_ac_type, 3)                            # Pos 73-75: Aircraft Type
                rec += pad_right('', 23)                                    # Pos 76-98: Various fields (empty)
                rec += pad_right(raw_ac_owner, 3)                           # Pos 99-101: Aircraft Owner
                rec += pad_right('', 36)                                    # Pos 102-137: More fields (empty)
                rec += pad_right(raw_onward_airline, 3)                     # Pos 138-140: Onward Airline
                rec += pad_left(raw_onward_fltno, 4)                        # Pos 141-144: Onward Flight Number
                rec += pad_left(raw_onward_leg_var, 1)                      # Pos 145-145: Aircraft Rotation Layover
                rec += pad_left(raw_onward_leg_seq, 1)                      # Pos 146-146: Operational Suffix
                rec += pad_right('', 26)                                    # Pos 147-172: More fields (empty)
                rec += pad_right(raw_ac_config + 'VV10', 20)                # Pos 173-192: Aircraft Configuration
                rec += pad_right('', 2)                                     # Pos 193-194: Date variation
                rec += pad_left(str(record_serial), 6, '0')                 # Pos 195-200: Record Serial Number
                
                # Ensure exactly 200 characters
                rec = pad_right(rec, 200)
                out.write(rec + '\n')
                record_serial += 1

                # ===== BUILD TYPE 4 RECORD WITH PROPER PADDING =====
                
                seg = ''
                seg += pad_right('4', 1)                                    # Pos 1: Record Type
                seg += pad_right('', 1)                                     # Pos 2: Operational Suffix
                seg += pad_right(raw_airline_code, 3)                       # Pos 3-5: Airline Code
                seg += pad_left(raw_flight_number, 4)                       # Pos 6-9: Flight Number
                seg += pad_left(raw_itinerary_var, 2, '0')                  # Pos 10-11: Itinerary Variant
                seg += pad_left(raw_leg_seq_num, 2, '0')                    # Pos 12-13: Leg Sequence
                seg += pad_right(raw_service_type, 1)                       # Pos 14: Service Type
                seg += pad_right('', 13)                                    # Pos 15-27: Period fields (empty)
                seg += pad_right('', 1)                                     # Pos 28: Itinerary Variant Overflow
                seg += pad_right('A', 1)                                    # Pos 29: Board Point Indicator
                seg += pad_right('B', 1)                                    # Pos 30: Off Point Indicator
                seg += pad_left('997', 3, '0')                              # Pos 31-33: Data Element ID
                seg += pad_right(raw_dep_airport, 3)                        # Pos 34-36: Board Point
                seg += pad_right(raw_arr_airport, 3)                        # Pos 37-39: Off Point
                
                seg += pad_right(reason_code, 155)                          # Pos 40 - 194: Data (associated with Data Element Identifier)
                
                seg += pad_left(str(record_serial), 6, '0')                 # Pos 195-200: Record Serial Number
                
                # Ensure exactly 200 characters
                seg = pad_right(seg, 200)
                out.write(seg + '\n')
                record_serial += 1
            
            # Move to next shell in chain
            if len(processed_shells) < len(shells):
                # Find the next shell to process
                last_leg = current_shell_legs[-1]
                last_arrival_airport = last_leg.get('Arvl Sta', '')
                next_shell_id = find_connecting_shell(shells, processed_shells, last_arrival_airport)
                
                if next_shell_id:
                    current_shell_id = next_shell_id
                    print(f"Continuing chain to Shell {current_shell_id}")
                else:
                    # No more connections, pick any unprocessed shell
                    unprocessed = [s for s in shells.keys() if s not in processed_shells]
                    if unprocessed:
                        current_shell_id = unprocessed[0]
                        print(f"Starting new chain with Shell {current_shell_id}")


        # Add 4 lines of 200 characters with all zeros
        for _ in range(4):
            out.write("0" * 200 + "\n")
        
        # header 5 
        las = ''
        las += pad_right('5', 1)                                    # Pos 1: Record Type
        las += pad_right('', 1)                                     # Pos 2: Operational Suffix
        las += pad_right('DL', 3)                                   # Pos 3-5: Airline Code
        las += pad_left(today_date.strftime('%d%b%y').upper(), 7)   # Pos 6-12: Today date
        las += pad_left('', 175)                                    # Pos 13-187: Empty space
        las += pad_left('000999', 6)                                # Pos 188-193: Serial Number Check Reference
        las += pad_left('E', 1)                                     # Pos 194: Record Type Indicator
        las += pad_left(str(record_serial), 6, '0')                 # Pos 195-200: Record Serial Number
        # Ensure exactly 200 characters
        las = pad_right(las, 200)
        out.write(las + '\n')
                
        # Add 4 lines of 200 characters with all zeros
        for _ in range(14):
            out.write("0" * 200 + "\n")
        
    print(f"✅ Wrote SSIM file: {filename}")
    print(f"   Generated {record_serial - 3} records (Type 3 and 4 pairs)")
    print(f"   Time zone mode: {'Local' if time_zone == 'L' else 'UTC'}")

def find_connecting_shell(shells, processed_shells, arrival_airport):
    """
    Find a shell whose first flight departs from the given arrival airport.
    Only considers unprocessed shells.
    
    Args:
        shells: Dictionary of all shells {shell_id: [legs]}
        processed_shells: Set of already processed shell IDs
        arrival_airport: Airport code where previous shell ended
    
    Returns:
        shell_id of connecting shell, or None if no connection found
    """
    for shell_id, shell_legs in shells.items():
        if shell_id in processed_shells:
            continue  # Skip already processed shells
            
        if not shell_legs:
            continue  # Skip empty shells
            
        # Check if first flight of this shell starts from arrival_airport
        first_flight = shell_legs[0]
        first_dept_airport = first_flight.get('Dept Sta', '')
        
        if first_dept_airport.upper() == arrival_airport.upper():
            return shell_id
    
    return None  # No connecting shell found

def main():
    """Main conversion workflow"""
    print("=== LoW to SSIM Converter with Dynamic Timezone Calculation ===\n")
    
    print("Step 1: Scanning LoW file for airport codes and date range...")
    airport_codes, first_date, last_date = extract_airport_codes_and_date_range(LOW_FILENAME)
    if not airport_codes:
        print("❌ No airport codes found. Check your LoW file.")
        return
    print(f"Found {len(airport_codes)} unique airports: {sorted(airport_codes)}")
    print(f"First flight date: {first_date.strftime('%d%b%y (%A)')}")
    print(f"Last flight date: {last_date.strftime('%d%b%y (%A)')}\n")
    
    print("Step 2: Calculating UTC offsets for all airports using flight date...")
    airport_offsets = calculate_airport_utc_offsets(airport_codes, first_date)
    print()
    
    print("Step 3: Reading LoW file and organizing aircraft rotations...")
    all_legs, shells = read_low_csv(LOW_FILENAME, airport_offsets)
    print()
    
    print("Step 4: Writing SSIM file with calculated timezones and date ranges...")
    write_ssim_with_segments(all_legs, shells, SSIM_OUT_FILENAME, first_date, last_date)
    
    print(f"\n✅ Conversion complete!")
    print(f"Input:  {LOW_FILENAME}")
    print(f"Output: {SSIM_OUT_FILENAME}")

if __name__ == "__main__":
    main()
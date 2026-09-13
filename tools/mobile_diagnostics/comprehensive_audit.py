import sqlite3
import os
import json

db_path = r'c:\Users\josemir\Desktop\skyFPL-native\ENRCL_FINAL.mbtiles'

def run_audit():
    if not os.path.exists(db_path):
        print(f"Error: File not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    report = {
        "metadata": {},
        "zoom_stats": [],
        "integrity_check": {
            "scheme_verified": False,
            "bounds_verified": False,
            "total_gaps": 0
        }
    }

    # 0. Quick Integrity Check
    print("--- Integrity Quick Check ---")
    cursor.execute("PRAGMA quick_check;")
    status = cursor.fetchone()[0]
    report["integrity_check"]["quick_check"] = status
    print(f"Status: {status}")

    # 1. Metadata Audit
    print("\n--- Metadata Audit ---")
    cursor.execute("SELECT name, value FROM metadata")
    for name, value in cursor.fetchall():
        report["metadata"][name] = value
        print(f"{name}: {value}")
        if name == "scheme" and value == "tms":
            report["integrity_check"]["scheme_verified"] = True

    # 2. Zoom Level Stats (Focused on Z5 to Z11)
    print("\n--- Zoom Level Statistics (Z5-Z11) ---")
    cursor.execute("""
        SELECT zoom_level, 
               count(*) as total,
               min(tile_column) as min_x, 
               max(tile_column) as max_x,
               min(tile_row) as min_y, 
               max(tile_row) as max_y
        FROM tiles 
        WHERE zoom_level >= 5 AND zoom_level <= 11
        GROUP BY zoom_level
        ORDER BY zoom_level
    """)
    rows = cursor.fetchall()
    
    total_gaps_found = 0
    for row in rows:
        z, total, min_x, max_x, min_y, max_y = row
        expected_grid = (max_x - min_x + 1) * (max_y - min_y + 1)
        density = (total / expected_grid) * 100 if expected_grid > 0 else 0
        
        # Simple gap detection: if total < expected, identify holes
        gaps_in_zoom = expected_grid - total
        total_gaps_found += gaps_in_zoom
        
        stat = {
            "zoom": z,
            "total_tiles": total,
            "bounds": {"min_x": min_x, "max_x": max_x, "min_y": min_y, "max_y": max_y},
            "expected_full_grid": expected_grid,
            "gaps": gaps_in_zoom,
            "density_percentage": f"{density:.2f}%"
        }
        report["zoom_stats"].append(stat)
        print(f"Z{z}: {total} tiles | Gaps: {gaps_in_zoom} | Density: {density:.2f}%")

    report["integrity_check"]["total_gaps"] = total_gaps_found

    # 3. Geographic Sampling for known problem areas (Brasília)
    # At Z11, Brasília is roughly around X: 865, Y: 1215 in TMS (need to verify Y inversion)
    print("\n--- Geographic Sampling (Brasília Region) ---")
    # Let's sample a small 3x3 grid around the center of Z11
    if len(rows) > 0 and rows[-1][0] == 11:
        z11 = [r for r in rows if r[0] == 11][0]
        mid_x = (z11[2] + z11[3]) // 2
        mid_y = (z11[4] + z11[5]) // 2
        
        samples = []
        for dx in range(-1, 2):
            for dy in range(-1, 2):
                tx, ty = mid_x + dx, mid_y + dy
                cursor.execute("SELECT 1 FROM tiles WHERE zoom_level=11 AND tile_column=? AND tile_row=?", (tx, ty))
                exists = cursor.fetchone() is not None
                samples.append({"x": tx, "y": ty, "exists": exists})
        
        report["brasilia_sample"] = samples
        missing = [s for s in samples if not s["exists"]]
        print(f"Brasília Sample (Z11 Center): {9 - len(missing)}/9 tiles present.")
        if missing:
            print(f"Missing sample tiles: {missing}")

    conn.close()
    
    # Save report
    with open("audit_report.json", "w") as f:
        json.dump(report, f, indent=4)
    print("\nAudit report saved to audit_report.json")

if __name__ == "__main__":
    run_audit()

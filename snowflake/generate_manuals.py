"""
Generate PDF Operating Manuals for Midstream PDM Demo
Creates realistic maintenance manuals for 8 equipment models.
"""
import os
from fpdf import FPDF

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "manuals")
os.makedirs(OUTPUT_DIR, exist_ok=True)

EQUIPMENT = [
    {
        "type": "PUMP", "model": "Flowserve HPRT", "manufacturer": "Flowserve",
        "full_name": "Flowserve HPRT High-Pressure Ring-Section Pump",
        "sections": {
            "Overview": (
                "The Flowserve HPRT is a multistage, radially split, ring-section centrifugal pump "
                "designed for high-pressure applications in midstream oil and gas operations. "
                "Operating range: 50-2500 GPM, pressures up to 3500 PSI. "
                "Typical applications include pipeline boosting, water injection, and produced water handling."
            ),
            "Maintenance Procedures": (
                "BEARING INSPECTION (Every 2000 hours or 90 days)\n"
                "1. Shut down and lock out pump per API 610 Section 8.3\n"
                "2. Remove bearing housing end covers\n"
                "3. Inspect radial bearings for wear marks, discoloration, or pitting\n"
                "4. Measure bearing clearance with feeler gauge - replace if > 0.004 inch\n"
                "5. Check thrust bearing wear ring clearance\n"
                "6. Inspect oil condition - dark or metallic particles indicate wear\n"
                "7. Replace bearings if vibration trending shows increase > 2mm/s RMS over baseline\n\n"
                "SEAL REPLACEMENT (When seal temperature exceeds 180degF or visible leakage)\n"
                "1. Isolate pump and drain casing\n"
                "2. Remove coupling guard and disconnect coupling\n"
                "3. Remove gland plate bolts and slide gland back\n"
                "4. Extract old mechanical seal cartridge assembly\n"
                "5. Inspect shaft sleeve for scoring - replace if groove depth > 0.002 inch\n"
                "6. Install new cartridge seal per Flowserve installation guide FSG-100\n"
                "7. Set seal face compression to 0.125 - 0.010 inch\n"
                "8. Reconnect coupling and verify alignment within 0.002 inch TIR\n\n"
                "VALVE CALIBRATION (Annually or when flow deviation > 5%)\n"
                "1. Verify suction and discharge pressure gauge calibration\n"
                "2. Check minimum flow recirculation valve setting\n"
                "3. Adjust discharge control valve to design point\n"
                "4. Verify check valve operation - no backflow permitted\n"
                "5. Test pressure relief valve at 110% of design pressure"
            ),
            "Troubleshooting Guide": (
                "EXCESSIVE VIBRATION (> 5.5 mm/s RMS)\n"
                "- Check coupling alignment - misalignment is #1 cause\n"
                "- Inspect impeller for erosion, buildup, or imbalance\n"
                "- Verify bearing condition - worn bearings amplify vibration\n"
                "- Check for cavitation (low NPSH) - reduce speed or increase suction head\n"
                "- Foundation bolt torque - re-torque to specification\n\n"
                "HIGH TEMPERATURE (> 210degF bearing temperature)\n"
                "- Verify lubrication oil level and condition\n"
                "- Check oil cooler performance - clean or replace\n"
                "- Inspect bearing for signs of overload\n"
                "- Verify pump is not operating at dead-head or extreme off-BEP\n\n"
                "PRESSURE FLUCTUATIONS\n"
                "- Check suction strainer for blockage\n"
                "- Verify adequate NPSH available\n"
                "- Inspect impeller wear rings - replace if clearance > 2x nominal\n"
                "- Check for air entrainment in suction line"
            ),
            "Parts List": (
                "Flowserve HPRT Recommended Spare Parts:\n"
                "- Mechanical seal cartridge (P/N: HPRT-SEAL-001) - Qty: 2\n"
                "- Radial bearing set (P/N: HPRT-BRG-RAD) - Qty: 2\n"
                "- Thrust bearing (P/N: HPRT-BRG-THR) - Qty: 1\n"
                "- Impeller wear ring set (P/N: HPRT-WR-SET) - Qty: 1\n"
                "- Shaft sleeve (P/N: HPRT-SLVE-001) - Qty: 1\n"
                "- O-ring kit, casing (P/N: HPRT-ORING-KIT) - Qty: 2\n"
                "- Coupling elastomer element (P/N: HPRT-CPL-001) - Qty: 1\n"
                "- Bearing housing gasket set (P/N: HPRT-GSKT-BH) - Qty: 2\n"
                "- ISO VG 68 bearing lubricant - 5 gallon"
            ),
            "Safety Procedures": (
                "LOCKOUT/TAGOUT REQUIREMENTS (Per OSHA 29 CFR 1910.147)\n"
                "1. Notify all affected personnel before pump shutdown\n"
                "2. Shut down pump using normal operating procedure\n"
                "3. Isolate all energy sources: electrical, pressure, thermal\n"
                "4. Close and lock suction and discharge isolation valves\n"
                "5. Verify zero energy state - bleed pressure to atmospheric\n"
                "6. Apply personal locks and tags to all isolation points\n"
                "7. Attempt restart to verify isolation\n\n"
                "H2S EXPOSURE PROTOCOL\n"
                "- H2S monitors required within 10 feet of pump during maintenance\n"
                "- SCBA required if H2S > 10 ppm\n"
                "- Buddy system mandatory for confined space entry\n"
                "- Emergency shower and eyewash within 50 feet"
            ),
        }
    },
    {
        "type": "PUMP", "model": "Grundfos CRN", "manufacturer": "Grundfos",
        "full_name": "Grundfos CRN Multistage Centrifugal Pump",
        "sections": {
            "Overview": (
                "The Grundfos CRN is a vertical multistage centrifugal pump constructed entirely from stainless steel "
                "(AISI 304 or 316). Designed for clean, non-aggressive liquids. "
                "Flow range: 1-180 m3/h, head up to 250 meters. "
                "Applications include produced water transfer, chemical injection, and booster service."
            ),
            "Maintenance Procedures": (
                "SHAFT SEAL INSPECTION (Every 1500 hours)\n"
                "1. Check for visible leakage at seal area\n"
                "2. Monitor seal temperature - normal range 120-170degF\n"
                "3. If temperature > 180degF, schedule seal replacement\n"
                "4. Mechanical seal type: HQQE (SiC/SiC/EPDM)\n"
                "5. Replace seal if leakage exceeds 1 drop per minute\n\n"
                "MOTOR BEARING SERVICE (Every 8000 hours)\n"
                "1. Re-grease motor bearings with lithium-based grease\n"
                "2. Check bearing temperature with IR thermometer\n"
                "3. Normal operating temperature: 140-180degF\n"
                "4. Replace bearings if noise or vibration increases\n\n"
                "IMPELLER INSPECTION (Annually)\n"
                "1. Disassemble pump stack\n"
                "2. Inspect impellers for erosion or pitting\n"
                "3. Check wear ring clearances\n"
                "4. Replace impellers if efficiency drops > 5%"
            ),
            "Troubleshooting Guide": (
                "PUMP WON'T START\n"
                "- Verify power supply and motor overload\n"
                "- Check for seized impeller (try rotating shaft manually)\n"
                "- Verify control system enable signal\n\n"
                "LOW FLOW/PRESSURE\n"
                "- Air lock in suction line - vent pump casing\n"
                "- Worn impellers - measure head vs. curve\n"
                "- Blocked strainer - clean suction screen\n"
                "- Wrong rotation - verify 3-phase sequence\n\n"
                "EXCESSIVE NOISE\n"
                "- Cavitation - increase NPSHA or reduce speed\n"
                "- Bearing wear - check with vibration analyzer\n"
                "- Coupling misalignment - re-align to 0.002 inch TIR"
            ),
            "Parts List": (
                "Grundfos CRN Recommended Spare Parts:\n"
                "- Shaft seal HQQE (P/N: 96511844) - Qty: 2\n"
                "- Bearing kit (P/N: 96510070) - Qty: 1\n"
                "- Impeller (P/N: varies by stage) - Qty: 1 per stage\n"
                "- Wear ring set (P/N: 96510080) - Qty: 1\n"
                "- Shaft (P/N: 96510065) - Qty: 1\n"
                "- O-ring kit (P/N: 96510090) - Qty: 2"
            ),
        }
    },
    {
        "type": "COMPRESSOR", "model": "Ariel JGK/4", "manufacturer": "Ariel",
        "full_name": "Ariel JGK/4 Four-Throw Reciprocating Compressor",
        "sections": {
            "Overview": (
                "The Ariel JGK/4 is a four-throw, separable reciprocating gas compressor "
                "designed for natural gas gathering, gas lift, and pipeline boosting. "
                "Frame rating: 1350 HP. Max rod load: 27,500 lbs. Speed range: 750-1200 RPM. "
                "Configured for sour gas service (H2S up to 100,000 ppm) with appropriate metallurgy."
            ),
            "Maintenance Procedures": (
                "VALVE INSPECTION (Every 4000 hours or 6 months)\n"
                "1. Remove valve cover and unloader assemblies\n"
                "2. Inspect suction and discharge valve plates for erosion, cracking\n"
                "3. Check valve springs for fatigue - replace if height loss > 10%\n"
                "4. Measure valve seat sealing surface - lap if scratched\n"
                "5. Re-torque valve cover bolts to specification\n"
                "6. Document valve condition in maintenance log\n\n"
                "PISTON RING AND RIDER BAND REPLACEMENT (Per Ariel SB-132)\n"
                "1. Remove cylinder head and valve assemblies\n"
                "2. Extract piston and rod assembly\n"
                "3. Measure piston ring end gaps - replace if > 0.035 inch\n"
                "4. Inspect rider bands for wear - replace if groove visible\n"
                "5. Check cylinder bore - hone if scoring present\n"
                "6. Install new rings with gaps staggered 90 degrees\n"
                "7. Verify piston end clearance per data sheet\n\n"
                "OIL SYSTEM SERVICE (Every 2000 hours)\n"
                "1. Sample and analyze frame lubricating oil\n"
                "2. Replace oil filter elements\n"
                "3. Check oil pressure - minimum 40 PSI at idle\n"
                "4. Inspect oil cooler - clean tubes if deltaT > spec"
            ),
            "Troubleshooting Guide": (
                "HIGH DISCHARGE TEMPERATURE (> 350degF)\n"
                "- Check compression ratio - may exceed stage design\n"
                "- Inspect suction valve for leakage (re-expansion heating)\n"
                "- Verify intercooler performance\n"
                "- Check for excessive cylinder wear increasing clearance\n\n"
                "LOW OIL PRESSURE (< 38 PSI)\n"
                "- Check oil level and condition\n"
                "- Inspect oil pump relief valve setting\n"
                "- Check oil cooler for blockage\n"
                "- Verify bearing clearances in specification\n\n"
                "ABNORMAL VIBRATION\n"
                "- Crosshead pin or wrist pin wear - check clearances\n"
                "- Piston rod runout - measure at packing\n"
                "- Foundation anchor bolt looseness\n"
                "- Unbalanced loading between throws"
            ),
            "Parts List": (
                "Ariel JGK/4 Recommended Spare Parts:\n"
                "- Suction valve assembly (P/N: A-21163) - Qty: 4\n"
                "- Discharge valve assembly (P/N: A-21164) - Qty: 4\n"
                "- Piston ring set (P/N: A-10892) - Qty: 4\n"
                "- Rider band set (P/N: A-10895) - Qty: 4\n"
                "- Rod packing set (P/N: A-11003) - Qty: 4\n"
                "- Crosshead bearing (P/N: A-10782) - Qty: 4\n"
                "- Wrist pin bushing (P/N: A-10785) - Qty: 4\n"
                "- Oil filter element (P/N: A-20055) - Qty: 4\n"
                "- Valve spring kit (P/N: A-21170) - Qty: 2"
            ),
            "Safety Procedures": (
                "COMPRESSOR PURGE PROCEDURE (Required before maintenance)\n"
                "1. Isolate compressor from process with block valves\n"
                "2. Vent process gas to flare system\n"
                "3. Purge cylinders and piping with nitrogen (3 volumes minimum)\n"
                "4. Verify gas-free with combustible gas detector (< 10% LEL)\n"
                "5. Maintain nitrogen blanket during hot work\n\n"
                "PACKING LEAK RESPONSE\n"
                "- If packing leakage detected, reduce speed and notify operations\n"
                "- For H2S service: activate area gas monitors, don SCBA if > 10 ppm\n"
                "- Isolate and depressure cylinder before packing replacement\n"
                "- Never tighten packing gland nuts while compressor is running"
            ),
        }
    },
    {
        "type": "COMPRESSOR", "model": "Atlas Copco GA-90", "manufacturer": "Atlas Copco",
        "full_name": "Atlas Copco GA-90 Rotary Screw Compressor",
        "sections": {
            "Overview": (
                "The Atlas Copco GA-90 is a 90kW oil-injected rotary screw compressor "
                "for instrument air and utility gas applications. "
                "Capacity: 250-930 CFM at 100-217 PSI. Variable speed drive option available. "
                "Built-in air treatment with integrated dryer and filtration."
            ),
            "Maintenance Procedures": (
                "OIL AND FILTER SERVICE (Every 4000 hours)\n"
                "1. Drain compressor oil while warm\n"
                "2. Replace oil filter element\n"
                "3. Replace oil separator element\n"
                "4. Refill with Atlas Copco Roto-Inject Fluid (9 gallons)\n"
                "5. Run for 10 minutes and check oil level\n\n"
                "AIR FILTER SERVICE (Every 2000 hours or when dP > 25 mbar)\n"
                "1. Remove air filter housing cover\n"
                "2. Extract and inspect filter element\n"
                "3. Replace if dust loading is heavy or damaged\n"
                "4. Verify housing seal is intact\n\n"
                "BEARING INSPECTION (Every 20000 hours)\n"
                "1. Measure vibration at drive end and non-drive end\n"
                "2. If vibration > 4.5 mm/s, schedule bearing replacement\n"
                "3. Replace bearings as a set (both ends simultaneously)"
            ),
            "Troubleshooting Guide": (
                "HIGH OUTLET TEMPERATURE (> 100degC / 212degF)\n"
                "- Clean oil cooler core - compressed air from clean side\n"
                "- Check ambient temperature and ventilation\n"
                "- Verify oil level - low oil reduces cooling capacity\n"
                "- Check minimum pressure valve - should close at 3.5 bar\n\n"
                "OIL CARRYOVER IN COMPRESSED AIR\n"
                "- Replace oil separator element\n"
                "- Check oil level - overfilling causes carryover\n"
                "- Inspect minimum pressure valve for correct operation\n"
                "- Verify scavenge line not blocked"
            ),
            "Parts List": (
                "Atlas Copco GA-90 Recommended Spare Parts:\n"
                "- Oil filter (P/N: 1613610500) - Qty: 2\n"
                "- Oil separator (P/N: 1613984000) - Qty: 1\n"
                "- Air filter (P/N: 1613740800) - Qty: 2\n"
                "- Roto-Inject Fluid 20L (P/N: 2901052200) - Qty: 1\n"
                "- Bearing kit DE/NDE (P/N: 2906095600) - Qty: 1\n"
                "- Inlet valve kit (P/N: 1613679300) - Qty: 1"
            ),
        }
    },
    {
        "type": "PUMP", "model": "Sulzer MSD", "manufacturer": "Sulzer",
        "full_name": "Sulzer MSD Axially Split Multistage Pump",
        "sections": {
            "Overview": (
                "The Sulzer MSD is a between-bearings, axially split multistage centrifugal pump "
                "conforming to API 610 BB3 configuration. Designed for high-pressure pipeline "
                "service. Flow range: 100-4000 GPM, head to 6000 feet."
            ),
            "Maintenance Procedures": (
                "COUPLING ALIGNMENT CHECK (Quarterly)\n"
                "1. Perform laser alignment measurement\n"
                "2. Acceptable: offset < 0.002 in, angularity < 0.001 in/in\n"
                "3. Document and trend alignment readings\n\n"
                "BEARING INSPECTION (Per vibration-based schedule)\n"
                "1. Replace when vibration exceeds 4.5 mm/s RMS\n"
                "2. Use SKF/FAG equivalent bearings per OEM specification\n"
                "3. Pre-heat bearings to 250degF for installation"
            ),
            "Troubleshooting Guide": (
                "SEAL FAILURE (Repeated)\n"
                "- Check shaft runout at seal chamber - max 0.002 TIR\n"
                "- Verify flush plan operation (API Plan 11 or 32)\n"
                "- Check operating point vs. BEP - avoid < 60% BEP\n"
                "- Inspect shaft sleeve for scoring or erosion"
            ),
        }
    },
    {
        "type": "PUMP", "model": "Sundyne LMV-311", "manufacturer": "Sundyne",
        "full_name": "Sundyne LMV-311 Integrally Geared Centrifugal Pump",
        "sections": {
            "Overview": (
                "The Sundyne LMV-311 is a high-speed, integrally geared centrifugal pump "
                "designed for low-flow, high-head applications. "
                "Speed: up to 25,000 RPM via integral speed increaser. "
                "Ideal for chemical injection and lean amine circulation."
            ),
            "Maintenance Procedures": (
                "SPEED INCREASER GEARBOX SERVICE (Every 8000 hours)\n"
                "1. Drain and sample gearbox oil\n"
                "2. Replace with Sundyne-approved ISO VG 32 synthetic\n"
                "3. Inspect gear teeth for pitting or wear patterns\n"
                "4. Check pinion bearing clearances\n\n"
                "INDUCER INSPECTION (When cavitation index > 0.16)\n"
                "1. Remove pump casing\n"
                "2. Inspect inducer for erosion or pitting\n"
                "3. Check inducer-to-casing clearance\n"
                "4. Replace inducer if erosion depth > 0.020 inch"
            ),
            "Troubleshooting Guide": (
                "HIGH CAVITATION INDEX\n"
                "- Verify NPSHA vs. NPSHR at operating speed\n"
                "- Check suction strainer condition\n"
                "- Reduce pump speed if possible\n"
                "- Inspect inducer for damage"
            ),
        }
    },
    {
        "type": "COMPRESSOR", "model": "Dresser-Rand DATUM", "manufacturer": "Dresser-Rand",
        "full_name": "Dresser-Rand DATUM Centrifugal Compressor",
        "sections": {
            "Overview": (
                "The Dresser-Rand DATUM is an integrally geared centrifugal compressor "
                "with up to 8 stages. Designed for process gas and pipeline compression. "
                "API 617 compliant. Flow range: 2,000-100,000 ACFM."
            ),
            "Maintenance Procedures": (
                "DRY GAS SEAL MONITORING\n"
                "1. Monitor primary seal vent flow continuously\n"
                "2. Normal leakage: < 5 SCFM\n"
                "3. Alert at 10 SCFM, alarm at 15 SCFM\n"
                "4. Check seal gas filter differential pressure daily\n"
                "5. Verify buffer gas supply pressure > 15 PSI above reference\n\n"
                "IMPELLER INSPECTION (During turnaround)\n"
                "1. Remove upper casing half\n"
                "2. Inspect impeller surfaces for erosion\n"
                "3. Check labyrinth seal clearances\n"
                "4. Measure rotor axial float - replace thrust bearing if > spec"
            ),
            "Troubleshooting Guide": (
                "SURGE DETECTION AND RECOVERY\n"
                "- Surge identified by rapid flow/pressure oscillations\n"
                "- Open anti-surge valve immediately\n"
                "- Check suction strainer for blockage\n"
                "- Verify control system anti-surge response time < 200ms\n"
                "- Inspect impeller for damage after surge event"
            ),
        }
    },
    {
        "type": "COMPRESSOR", "model": "Ingersoll Rand Centac", "manufacturer": "Ingersoll Rand",
        "full_name": "Ingersoll Rand Centac Centrifugal Air Compressor",
        "sections": {
            "Overview": (
                "The Ingersoll Rand Centac is an oil-free centrifugal air compressor "
                "with 2-4 stages of compression. Designed for instrument air and process air. "
                "Capacity: 500-35,000 CFM. ISO 8573-1 Class 0 oil-free certification."
            ),
            "Maintenance Procedures": (
                "INTERCOOLER CLEANING (Annually or when approach temp > 15degF)\n"
                "1. Isolate cooler water supply\n"
                "2. Open drain valves to remove condensate\n"
                "3. Chemical clean tube bundles with approved solvent\n"
                "4. Rinse thoroughly with clean water\n"
                "5. Inspect tube sheet for corrosion or tube wall thinning\n\n"
                "INLET GUIDE VANE CALIBRATION (Annually)\n"
                "1. Check IGV actuator stroke and response\n"
                "2. Calibrate 4-20mA signal to 0-100% vane position\n"
                "3. Verify smooth operation through full range\n"
                "4. Lubricate linkage per maintenance manual"
            ),
            "Troubleshooting Guide": (
                "COMPRESSOR SURGE\n"
                "- Open blow-off valve to unload\n"
                "- Check inlet filter restriction - replace if dP > 6 inH2O\n"
                "- Verify IGV calibration and response\n"
                "- Check for fouled intercoolers reducing capacity\n\n"
                "HIGH VIBRATION\n"
                "- Check for impeller fouling - chemical wash if needed\n"
                "- Inspect bull gear and pinion gear mesh\n"
                "- Verify bearing condition via proximity probes\n"
                "- Check coupling alignment"
            ),
        }
    },
]

class ManualPDF(FPDF):
    def __init__(self, model_name, manufacturer, asset_type):
        super().__init__()
        self.model_name = model_name
        self.manufacturer = manufacturer
        self.asset_type = asset_type

    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, f"{self.manufacturer} - {self.model_name} Operating Manual", align="R", new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()} - CONFIDENTIAL - {self.manufacturer}", align="C")

def create_manual(eq):
    pdf = ManualPDF(eq["model"], eq["manufacturer"], eq["type"])
    pdf.set_auto_page_break(auto=True, margin=20)

    pdf.add_page()
    pdf.set_font("Helvetica", "B", 24)
    pdf.set_text_color(30, 58, 138)
    pdf.ln(30)
    pdf.cell(0, 15, eq["full_name"], align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 10, "Operating & Maintenance Manual", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, f"Equipment Type: {eq['type']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Manufacturer: {eq['manufacturer']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Applicable Standards: {'API 610' if eq['type'] == 'PUMP' else 'API 618/617'}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(20)
    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(128, 128, 128)
    pdf.cell(0, 8, "Document Classification: INTERNAL USE ONLY", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, "Revision: 3.2 | Date: January 2026", align="C", new_x="LMARGIN", new_y="NEXT")

    for section_title, content in eq["sections"].items():
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 16)
        pdf.set_text_color(30, 58, 138)
        pdf.cell(0, 12, section_title, new_x="LMARGIN", new_y="NEXT")
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(6)

        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(40, 40, 40)
        for paragraph in content.split("\n"):
            paragraph = paragraph.strip()
            if not paragraph:
                pdf.ln(3)
                continue
            if paragraph[0].isupper() and paragraph == paragraph.upper().rstrip("0123456789./ "):
                pdf.set_font("Helvetica", "B", 11)
                pdf.set_text_color(60, 60, 60)
                pdf.ln(4)
                pdf.multi_cell(0, 6, paragraph)
                pdf.set_font("Helvetica", "", 10)
                pdf.set_text_color(40, 40, 40)
            elif paragraph.startswith(("- ", "- ")):
                pdf.set_x(15)
                pdf.multi_cell(0, 5.5, f"  {paragraph}")
            elif paragraph[0].isdigit() and paragraph[1] in ".)" :
                pdf.set_x(15)
                pdf.multi_cell(0, 5.5, f"  {paragraph}")
            else:
                pdf.multi_cell(0, 5.5, paragraph)

    safe_name = eq["model"].replace("/", "-").replace(" ", "_")
    filename = f"{safe_name}_manual.pdf"
    filepath = os.path.join(OUTPUT_DIR, filename)
    pdf.output(filepath)
    return filepath

if __name__ == "__main__":
    print(f"Generating {len(EQUIPMENT)} equipment manuals...")
    for eq in EQUIPMENT:
        path = create_manual(eq)
        pages = FPDF()
        print(f"  Created: {os.path.basename(path)} ({eq['type']} - {eq['model']})")
    print(f"\nAll manuals saved to: {OUTPUT_DIR}")

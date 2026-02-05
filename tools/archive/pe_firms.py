#!/usr/bin/env python3
"""
PE Firms Database Manager

Manages a database of private equity firms focused on energy sector investments.
Supports tracking outreach, contacts, and deal activity.

Usage:
    python3 pe_firms.py --list                    # List all firms
    python3 pe_firms.py --list --tier 3           # List Tier 3 firms
    python3 pe_firms.py --search "solar"          # Search firms by focus
    python3 pe_firms.py --export firms.csv        # Export to CSV
    python3 pe_firms.py --add-contact             # Add a contact (interactive)
    python3 pe_firms.py --update-status <id>      # Update outreach status
"""

import argparse
import csv
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Database path
DB_PATH = Path(__file__).parent / ".data" / "pe_firms.db"


def init_database():
    """Initialize the PE firms database with schema and seed data."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create firms table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS firms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            tier INTEGER NOT NULL,
            tier_description TEXT,
            focus TEXT,
            aum TEXT,
            aum_numeric REAL,
            website TEXT,
            linkedin TEXT,
            headquarters TEXT,
            key_contact_titles TEXT,
            target_check_size TEXT,
            recent_deals TEXT,
            notes TEXT,
            priority INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create contacts table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            title TEXT,
            email TEXT,
            phone TEXT,
            linkedin TEXT,
            notes TEXT,
            is_primary BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (firm_id) REFERENCES firms(id)
        )
    """)

    # Create outreach table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS outreach (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id INTEGER NOT NULL,
            contact_id INTEGER,
            outreach_type TEXT,
            outreach_date DATE,
            status TEXT DEFAULT 'pending',
            response_date DATE,
            response_notes TEXT,
            follow_up_date DATE,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (firm_id) REFERENCES firms(id),
            FOREIGN KEY (contact_id) REFERENCES contacts(id)
        )
    """)

    # Create deals table (for tracking their recent transactions)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id INTEGER NOT NULL,
            deal_name TEXT,
            deal_type TEXT,
            deal_size TEXT,
            deal_date DATE,
            technology TEXT,
            region TEXT,
            notes TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (firm_id) REFERENCES firms(id)
        )
    """)

    conn.commit()
    conn.close()

    # Seed with initial data
    seed_firms()

    print(f"Database initialized at {DB_PATH}")


def seed_firms():
    """Seed the database with PE firm data."""

    firms_data = [
        # Tier 1: Mega-Funds
        {
            "name": "Brookfield Renewable",
            "tier": 1,
            "tier_description": "Mega-Fund (>$50B AUM)",
            "focus": "Hydro, wind, solar, storage, nuclear",
            "aum": "$100B+ deployed",
            "aum_numeric": 100000,
            "website": "https://bep.brookfield.com/",
            "linkedin": "https://www.linkedin.com/company/brookfield-renewable",
            "headquarters": "Toronto, Canada",
            "key_contact_titles": "Managing Partner, Investment Director",
            "target_check_size": "$500M - $5B+",
            "recent_deals": "Neoen acquisition (EUR 6.1B)",
            "notes": "World's largest renewable power owner; Connor Teskey leads",
            "priority": 3
        },
        {
            "name": "KKR Infrastructure",
            "tier": 1,
            "tier_description": "Mega-Fund (>$50B AUM)",
            "focus": "Renewables, digital infrastructure, data centers",
            "aum": "$95B infrastructure AUM",
            "aum_numeric": 95000,
            "website": "https://www.kkr.com/invest/infrastructure",
            "linkedin": "https://www.linkedin.com/company/kkr",
            "headquarters": "New York, NY",
            "key_contact_titles": "Partner, Managing Director",
            "target_check_size": "$200M - $2B+",
            "recent_deals": "$50B ECP partnership; Avantus acquisition",
            "notes": "$35B invested in climate since 2010; 50 GW dev pipeline",
            "priority": 3
        },
        {
            "name": "Blackstone Infrastructure",
            "tier": 1,
            "tier_description": "Mega-Fund (>$50B AUM)",
            "focus": "Data centers, power, energy transition",
            "aum": "$140B infrastructure platform",
            "aum_numeric": 140000,
            "website": "https://www.blackstone.com/our-businesses/infrastructure/",
            "linkedin": "https://www.linkedin.com/company/blackstone",
            "headquarters": "New York, NY",
            "key_contact_titles": "Senior Managing Director, Managing Director",
            "target_check_size": "$500M - $5B+",
            "recent_deals": "$16B AirTrunk; $25B Pennsylvania infrastructure",
            "notes": "World's largest alt asset manager; Sean Klimczak heads infra",
            "priority": 2
        },
        {
            "name": "Global Infrastructure Partners",
            "tier": 1,
            "tier_description": "Mega-Fund (>$50B AUM)",
            "focus": "Power, renewables, midstream, transport",
            "aum": "$100B+ AUM",
            "aum_numeric": 100000,
            "website": "https://www.global-infra.com/",
            "linkedin": "https://www.linkedin.com/company/global-infrastructure-partners",
            "headquarters": "New York, NY",
            "key_contact_titles": "Managing Director, Principal",
            "target_check_size": "$500M - $3B+",
            "recent_deals": "Acquired by BlackRock for $12.5B",
            "notes": "Now part of BlackRock; major infrastructure player",
            "priority": 2
        },
        {
            "name": "Macquarie Asset Management",
            "tier": 1,
            "tier_description": "Mega-Fund (>$50B AUM)",
            "focus": "Full spectrum infrastructure",
            "aum": "$735B+ AUM",
            "aum_numeric": 735000,
            "website": "https://www.macquarie.com/",
            "linkedin": "https://www.linkedin.com/showcase/macquarie-asset-management/",
            "headquarters": "Sydney, Australia",
            "key_contact_titles": "Division Director, Executive Director",
            "target_check_size": "$200M - $2B",
            "recent_deals": "190+ portfolio companies globally",
            "notes": "World's largest infrastructure asset manager",
            "priority": 2
        },
        {
            "name": "EQT Infrastructure",
            "tier": 1,
            "tier_description": "Mega-Fund (>$50B AUM)",
            "focus": "Energy transition, digital, utilities",
            "aum": "EUR 266B total AUM",
            "aum_numeric": 285000,
            "website": "https://eqtgroup.com/infrastructure",
            "linkedin": "https://www.linkedin.com/company/eqt-group",
            "headquarters": "Stockholm, Sweden",
            "key_contact_titles": "Partner, Director",
            "target_check_size": "$200M - $2B",
            "recent_deals": "OX2 renewable platform; EUR 21.5B Fund VI",
            "notes": "3rd largest PE firm globally; 9 GW renewable portfolio",
            "priority": 3
        },

        # Tier 2: Large Infrastructure Funds
        {
            "name": "Stonepeak",
            "tier": 2,
            "tier_description": "Large Infrastructure ($15-50B AUM)",
            "focus": "Power, renewables, transport, digital",
            "aum": "$65B AUM",
            "aum_numeric": 65000,
            "website": "https://stonepeak.com",
            "linkedin": "https://www.linkedin.com/company/stonepeakpartners",
            "headquarters": "New York, NY",
            "key_contact_titles": "Managing Director, Principal",
            "target_check_size": "$200M - $2B",
            "recent_deals": "TerraWind Renewables; Madison Energy",
            "notes": "Strong US infrastructure focus; energy transition active",
            "priority": 4
        },
        {
            "name": "I Squared Capital",
            "tier": 2,
            "tier_description": "Large Infrastructure ($15-50B AUM)",
            "focus": "Power, utilities, energy transition",
            "aum": "$50B+ AUM",
            "aum_numeric": 50000,
            "website": "https://isquaredcapital.com/",
            "linkedin": "https://www.linkedin.com/company/i-squared-capital",
            "headquarters": "Miami, FL",
            "key_contact_titles": "Managing Director, Partner",
            "target_check_size": "$100M - $1B",
            "recent_deals": "First Ireland investment (AI/energy)",
            "notes": "Global Fund Manager of Year; 90+ companies in 70 countries",
            "priority": 4
        },
        {
            "name": "Ares Infrastructure",
            "tier": 2,
            "tier_description": "Large Infrastructure ($15-50B AUM)",
            "focus": "Energy transition, renewables, digital",
            "aum": "$419B total firm AUM",
            "aum_numeric": 14000,
            "website": "https://www.aresmgmt.com/our-business/infrastructure-opportunities",
            "linkedin": "https://www.linkedin.com/company/ares-management",
            "headquarters": "Los Angeles, CA",
            "key_contact_titles": "Partner, Managing Director",
            "target_check_size": "$100M - $1B",
            "recent_deals": "$2.9B EDPR portfolio; 2.7 GW ENGIE deal",
            "notes": "Very active in renewables M&A; $14B deployed in 350 assets",
            "priority": 5
        },
        {
            "name": "Copenhagen Infrastructure Partners",
            "tier": 2,
            "tier_description": "Large Infrastructure ($15-50B AUM)",
            "focus": "Offshore wind, solar, storage, P2X",
            "aum": "EUR 35B AUM",
            "aum_numeric": 38000,
            "website": "https://www.cip.com/",
            "linkedin": "https://www.linkedin.com/company/copenhagen-infrastructure-partners-k-s/",
            "headquarters": "Copenhagen, Denmark",
            "key_contact_titles": "Partner, Investment Director",
            "target_check_size": "$100M - $1B+",
            "recent_deals": "13 funds; 200+ institutional investors",
            "notes": "World's largest greenfield renewable energy fund manager",
            "priority": 4
        },
        {
            "name": "Apollo Clean Transition",
            "tier": 2,
            "tier_description": "Large Infrastructure ($15-50B AUM)",
            "focus": "Clean energy, infrastructure credit",
            "aum": "$69B deployed; $100B target",
            "aum_numeric": 69000,
            "website": "https://www.apollo.com/",
            "linkedin": "https://www.linkedin.com/company/apollo-global-management-inc",
            "headquarters": "New York, NY",
            "key_contact_titles": "Partner, Managing Director",
            "target_check_size": "$100M - $1B+",
            "recent_deals": "$3B Standard Chartered partnership",
            "notes": "Targeting $100B in energy transition by 2030",
            "priority": 3
        },
        {
            "name": "TPG Rise Climate",
            "tier": 2,
            "tier_description": "Large Infrastructure ($15-50B AUM)",
            "focus": "Climate PE, transition infrastructure",
            "aum": "$14B+ impact platform",
            "aum_numeric": 14000,
            "website": "https://www.tpg.com/platforms/impact/rise-climate",
            "linkedin": "https://www.linkedin.com/company/the-rise-fund",
            "headquarters": "San Francisco, CA",
            "key_contact_titles": "Partner, Principal",
            "target_check_size": "$50M - $500M",
            "recent_deals": "30-member corporate coalition",
            "notes": "Largest PE impact platform; dedicated climate fund",
            "priority": 4
        },

        # Tier 3: Energy Specialists
        {
            "name": "Energy Capital Partners",
            "tier": 3,
            "tier_description": "Energy Specialist ($5-20B AUM)",
            "focus": "Power generation, renewables, storage",
            "aum": "74 GW owned/operated",
            "aum_numeric": 15000,
            "website": "https://www.ecpgp.com/",
            "linkedin": "https://www.linkedin.com/company/ecpgp",
            "headquarters": "Summit, NJ",
            "key_contact_titles": "Partner, Principal, Vice President",
            "target_check_size": "$100M - $1B",
            "recent_deals": "$50B KKR partnership; $25B ADQ partnership",
            "notes": "Top priority - active development buyer; 300+ plants owned",
            "priority": 5
        },
        {
            "name": "ArcLight Capital",
            "tier": 3,
            "tier_description": "Energy Specialist ($5-20B AUM)",
            "focus": "Power, renewables, gas infrastructure",
            "aum": "$27B committed; 65 GW portfolio",
            "aum_numeric": 27000,
            "website": "https://arclight.com/",
            "linkedin": "https://www.linkedin.com/company/arclight-capital-partners",
            "headquarters": "Boston, MA",
            "key_contact_titles": "Managing Director, Principal, Vice President",
            "target_check_size": "$50M - $500M",
            "recent_deals": "$1B CPP investment in AlphaGen",
            "notes": "TOP PRIORITY - Development stage specialists; 110 transactions",
            "priority": 5
        },
        {
            "name": "Quinbrook Infrastructure",
            "tier": 3,
            "tier_description": "Energy Specialist ($5-20B AUM)",
            "focus": "Solar, storage, data centers",
            "aum": "$4.3B+ raised",
            "aum_numeric": 4300,
            "website": "https://www.quinbrook.com/",
            "linkedin": "https://www.linkedin.com/company/quinbrook-infrastructure-partners",
            "headquarters": "London, UK",
            "key_contact_titles": "Managing Director, Investment Director",
            "target_check_size": "$50M - $500M",
            "recent_deals": "$3B Net Zero Power Fund; Gemini solar/storage",
            "notes": "TOP PRIORITY - Aggressive solar/storage; data center focus",
            "priority": 5
        },
        {
            "name": "Generate Capital",
            "tier": 3,
            "tier_description": "Energy Specialist ($5-20B AUM)",
            "focus": "Distributed solar, storage, sustainable infra",
            "aum": "$14B+ raised",
            "aum_numeric": 14000,
            "website": "https://generatecapital.com/",
            "linkedin": "https://www.linkedin.com/company/generatecapital",
            "headquarters": "San Francisco, CA",
            "key_contact_titles": "Vice President, Director, Principal",
            "target_check_size": "$10M - $200M",
            "recent_deals": "$1B credit raise; 2,000+ assets",
            "notes": "TOP PRIORITY - High volume; platform builder; Infrastructure-as-a-Service",
            "priority": 5
        },
        {
            "name": "Capital Dynamics Clean Energy",
            "tier": 3,
            "tier_description": "Energy Specialist ($5-20B AUM)",
            "focus": "Solar, wind, storage (utility & distributed)",
            "aum": "$6B+ clean energy AUM",
            "aum_numeric": 6000,
            "website": "https://www.capdyn.com/",
            "linkedin": "https://www.linkedin.com/company/capital-dynamics",
            "headquarters": "Zug, Switzerland",
            "key_contact_titles": "Director, Vice President",
            "target_check_size": "$25M - $200M",
            "recent_deals": "7.3 GW across 150+ projects",
            "notes": "TOP PRIORITY - Pure-play renewables; strong track record",
            "priority": 5
        },
        {
            "name": "EnCap Energy Transition",
            "tier": 3,
            "tier_description": "Energy Specialist ($5-20B AUM)",
            "focus": "Renewables, carbon capture, clean fuels",
            "aum": "Part of $47B+ platform",
            "aum_numeric": 5000,
            "website": "https://www.encapinvestments.com/",
            "linkedin": "https://www.linkedin.com/company/encap-investments-energy-transition",
            "headquarters": "Houston, TX",
            "key_contact_titles": "Managing Partner, Partner, Principal",
            "target_check_size": "$25M - $300M",
            "recent_deals": "Energy transition fund since 2019",
            "notes": "Traditional O&G expanding to renewables; Houston-based relationships",
            "priority": 4
        },

        # Tier 4: Mid-Market
        {
            "name": "Greenbacker Capital",
            "tier": 4,
            "tier_description": "Mid-Market ($1-5B AUM)",
            "focus": "Solar, wind, storage IPP",
            "aum": "3.4 GW; 450+ projects",
            "aum_numeric": 2000,
            "website": "https://www.greenbackercapital.com/",
            "linkedin": "https://www.linkedin.com/company/greenbacker",
            "headquarters": "New York, NY",
            "key_contact_titles": "Managing Director, Director, Vice President",
            "target_check_size": "$5M - $100M",
            "recent_deals": "Sold portfolios to CleanCapital, Altus Power",
            "notes": "Active buyer AND seller; good for portfolio deals",
            "priority": 5
        },
        {
            "name": "CleanCapital",
            "tier": 4,
            "tier_description": "Mid-Market ($1-5B AUM)",
            "focus": "Distributed solar, C&I, storage",
            "aum": "$1B+ deployed",
            "aum_numeric": 1000,
            "website": "https://cleancapital.com/",
            "linkedin": "https://www.linkedin.com/company/cleancapital",
            "headquarters": "New York, NY",
            "key_contact_titles": "Managing Director, Director, Vice President",
            "target_check_size": "$5M - $50M",
            "recent_deals": "64-project Greenbacker acquisition",
            "notes": "Mid-market distributed specialist; 350+ assets",
            "priority": 5
        },
        {
            "name": "NGP Energy Capital",
            "tier": 4,
            "tier_description": "Mid-Market ($1-5B AUM)",
            "focus": "Oil/gas + energy transition",
            "aum": "$12B+ AUM",
            "aum_numeric": 12000,
            "website": "https://ngpenergy.com/",
            "linkedin": "https://www.linkedin.com/company/ngpenergycapital",
            "headquarters": "Irving, TX",
            "key_contact_titles": "Partner, Principal",
            "target_check_size": "$25M - $200M",
            "recent_deals": "$5.3B fund close",
            "notes": "Carlyle affiliate; transitioning to include renewables",
            "priority": 3
        },
        {
            "name": "Clearway Energy Group",
            "tier": 4,
            "tier_description": "Mid-Market ($1-5B AUM)",
            "focus": "Utility-scale renewables",
            "aum": "Major IPP",
            "aum_numeric": 5000,
            "website": "https://www.clearwayenergygroup.com/",
            "linkedin": "https://www.linkedin.com/company/clearway-energy-group",
            "headquarters": "San Francisco, CA",
            "key_contact_titles": "Vice President, Director",
            "target_check_size": "$20M - $200M",
            "recent_deals": "Active developer and acquirer",
            "notes": "Major US renewable IPP; NYSE listed (CWEN)",
            "priority": 4
        },

        # Tier 5: Growth/Venture
        {
            "name": "Energy Impact Partners",
            "tier": 5,
            "tier_description": "Growth/Venture",
            "focus": "Climate tech, grid, mobility",
            "aum": "$4.5B+ AUM",
            "aum_numeric": 4500,
            "website": "https://www.energyimpactpartners.com/",
            "linkedin": "https://www.linkedin.com/company/energy-impact-partners",
            "headquarters": "New York, NY",
            "key_contact_titles": "Partner, Principal",
            "target_check_size": "$5M - $50M",
            "recent_deals": "80+ corporate partners",
            "notes": "Corporate-backed; good for tech-enabled platforms",
            "priority": 3
        },
        {
            "name": "Ara Partners",
            "tier": 5,
            "tier_description": "Growth/Venture",
            "focus": "Industrial decarbonization",
            "aum": "$3B+",
            "aum_numeric": 3000,
            "website": "https://www.arapartners.com/",
            "linkedin": "https://www.linkedin.com/company/ara-partners",
            "headquarters": "Houston, TX",
            "key_contact_titles": "Partner, Principal",
            "target_check_size": "$25M - $150M",
            "recent_deals": "Industrial decarb focus",
            "notes": "Industrial angle; Houston-based",
            "priority": 3
        },
    ]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for firm in firms_data:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO firms
                (name, tier, tier_description, focus, aum, aum_numeric, website, linkedin,
                 headquarters, key_contact_titles, target_check_size, recent_deals, notes, priority)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                firm["name"], firm["tier"], firm["tier_description"], firm["focus"],
                firm["aum"], firm["aum_numeric"], firm["website"], firm["linkedin"],
                firm["headquarters"], firm["key_contact_titles"], firm["target_check_size"],
                firm["recent_deals"], firm["notes"], firm["priority"]
            ))
        except sqlite3.IntegrityError:
            pass  # Skip duplicates

    conn.commit()
    conn.close()
    print(f"Seeded {len(firms_data)} firms")


def list_firms(tier: Optional[int] = None, priority: Optional[int] = None) -> List[Dict]:
    """List firms with optional filtering."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = "SELECT * FROM firms WHERE 1=1"
    params = []

    if tier:
        query += " AND tier = ?"
        params.append(tier)

    if priority:
        query += " AND priority >= ?"
        params.append(priority)

    query += " ORDER BY priority DESC, tier ASC, name ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def search_firms(search_term: str) -> List[Dict]:
    """Search firms by name, focus, or notes."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    search_pattern = f"%{search_term}%"
    cursor.execute("""
        SELECT * FROM firms
        WHERE name LIKE ? OR focus LIKE ? OR notes LIKE ?
        ORDER BY priority DESC, tier ASC
    """, (search_pattern, search_pattern, search_pattern))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def export_to_csv(output_path: str):
    """Export firms to CSV."""
    firms = list_firms()

    if not firms:
        print("No firms to export")
        return

    fieldnames = [
        'name', 'tier', 'tier_description', 'focus', 'aum', 'website', 'linkedin',
        'headquarters', 'key_contact_titles', 'target_check_size', 'recent_deals',
        'notes', 'priority', 'outreach_status', 'last_contact', 'next_followup', 'contact_name', 'contact_email'
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        for firm in firms:
            # Add empty outreach tracking fields
            firm['outreach_status'] = ''
            firm['last_contact'] = ''
            firm['next_followup'] = ''
            firm['contact_name'] = ''
            firm['contact_email'] = ''
            writer.writerow(firm)

    print(f"Exported {len(firms)} firms to {output_path}")


def add_contact(firm_name: str, contact_name: str, title: str = None,
                email: str = None, linkedin: str = None, is_primary: bool = False):
    """Add a contact for a firm."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Find firm
    cursor.execute("SELECT id FROM firms WHERE name LIKE ?", (f"%{firm_name}%",))
    result = cursor.fetchone()

    if not result:
        print(f"Firm not found: {firm_name}")
        conn.close()
        return

    firm_id = result[0]

    cursor.execute("""
        INSERT INTO contacts (firm_id, name, title, email, linkedin, is_primary)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (firm_id, contact_name, title, email, linkedin, is_primary))

    conn.commit()
    conn.close()
    print(f"Added contact {contact_name} for {firm_name}")


def log_outreach(firm_name: str, outreach_type: str, notes: str = None,
                 follow_up_date: str = None):
    """Log an outreach attempt."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Find firm
    cursor.execute("SELECT id FROM firms WHERE name LIKE ?", (f"%{firm_name}%",))
    result = cursor.fetchone()

    if not result:
        print(f"Firm not found: {firm_name}")
        conn.close()
        return

    firm_id = result[0]

    cursor.execute("""
        INSERT INTO outreach (firm_id, outreach_type, outreach_date, notes, follow_up_date)
        VALUES (?, ?, ?, ?, ?)
    """, (firm_id, outreach_type, datetime.now().date(), notes, follow_up_date))

    conn.commit()
    conn.close()
    print(f"Logged {outreach_type} outreach for {firm_name}")


def get_outreach_summary() -> List[Dict]:
    """Get outreach summary by firm."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            f.name,
            f.tier,
            f.priority,
            COUNT(o.id) as total_outreach,
            MAX(o.outreach_date) as last_outreach,
            MIN(CASE WHEN o.follow_up_date >= date('now') THEN o.follow_up_date END) as next_followup
        FROM firms f
        LEFT JOIN outreach o ON f.id = o.firm_id
        GROUP BY f.id
        ORDER BY f.priority DESC, last_outreach ASC NULLS FIRST
    """)

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def print_firms_table(firms: List[Dict]):
    """Print firms in a formatted table."""
    if not firms:
        print("No firms found")
        return

    # Header
    print(f"\n{'='*120}")
    print(f"{'Firm':<30} {'Tier':<6} {'Focus':<35} {'Priority':<10} {'AUM':<20}")
    print(f"{'='*120}")

    for firm in firms:
        name = firm['name'][:28]
        focus = (firm['focus'] or '')[:33]
        print(f"{name:<30} {firm['tier']:<6} {focus:<35} {firm['priority']:<10} {firm['aum'] or '':<20}")

    print(f"{'='*120}")
    print(f"Total: {len(firms)} firms\n")


def main():
    parser = argparse.ArgumentParser(description="PE Firms Database Manager")
    parser.add_argument('--init', action='store_true', help='Initialize database')
    parser.add_argument('--list', action='store_true', help='List all firms')
    parser.add_argument('--tier', type=int, help='Filter by tier (1-5)')
    parser.add_argument('--priority', type=int, help='Filter by minimum priority')
    parser.add_argument('--search', type=str, help='Search firms')
    parser.add_argument('--export', type=str, help='Export to CSV')
    parser.add_argument('--outreach-summary', action='store_true', help='Show outreach summary')

    args = parser.parse_args()

    # Initialize if needed
    if args.init or not DB_PATH.exists():
        init_database()

    if args.list:
        firms = list_firms(tier=args.tier, priority=args.priority)
        print_firms_table(firms)

    elif args.search:
        firms = search_firms(args.search)
        print_firms_table(firms)

    elif args.export:
        export_to_csv(args.export)

    elif args.outreach_summary:
        summary = get_outreach_summary()
        print("\nOutreach Summary:")
        print(f"{'='*100}")
        print(f"{'Firm':<30} {'Tier':<6} {'Priority':<10} {'Outreach':<10} {'Last':<12} {'Next F/U':<12}")
        print(f"{'='*100}")
        for row in summary:
            print(f"{row['name'][:28]:<30} {row['tier']:<6} {row['priority']:<10} "
                  f"{row['total_outreach'] or 0:<10} {row['last_outreach'] or 'Never':<12} "
                  f"{row['next_followup'] or '-':<12}")

    elif not args.init:
        # Default: show high-priority firms
        print("\nHigh-Priority Firms (Priority >= 5):")
        firms = list_firms(priority=5)
        print_firms_table(firms)


if __name__ == "__main__":
    main()

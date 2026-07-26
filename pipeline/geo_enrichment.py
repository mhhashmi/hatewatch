#!/usr/bin/env python3
"""
geo_enrichment.py — HateWatch geo enrichment
=============================================
Fills in missing state data for incidents using:
  1. City → State lookup (covers most J&K, NE states, etc.)
  2. Postal code prefix → State lookup (covers records with only postal code)
  3. OpenStreetMap Nominatim API (for any remaining records with lat/lng)

Usage:
    uv run python pipeline/geo_enrichment.py           # dry-run — shows what would change
    uv run python pipeline/geo_enrichment.py --fix     # apply changes to DB
    uv run python pipeline/geo_enrichment.py --fix --batch 200
"""

import os
import sys
import time
import logging
import argparse

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise SystemExit('ERROR: DATABASE_URL missing from .env')

# ---------------------------------------------------------------------------
# City → State lookup
# Covers major cities, district HQs, and towns commonly appearing in records
# ---------------------------------------------------------------------------
CITY_TO_STATE = {
    # Jammu & Kashmir (most common missing state)
    'jammu': 'Jammu & Kashmir',
    'srinagar': 'Jammu & Kashmir',
    'kulgam': 'Jammu & Kashmir',
    'anantnag': 'Jammu & Kashmir',
    'baramulla': 'Jammu & Kashmir',
    'sopore': 'Jammu & Kashmir',
    'pulwama': 'Jammu & Kashmir',
    'shopian': 'Jammu & Kashmir',
    'budgam': 'Jammu & Kashmir',
    'badgam': 'Jammu & Kashmir',
    'ganderbal': 'Jammu & Kashmir',
    'bandipora': 'Jammu & Kashmir',
    'kupwara': 'Jammu & Kashmir',
    'rajouri': 'Jammu & Kashmir',
    'poonch': 'Jammu & Kashmir',
    'kathua': 'Jammu & Kashmir',
    'udhampur': 'Jammu & Kashmir',
    'reasi': 'Jammu & Kashmir',
    'doda': 'Jammu & Kashmir',
    'ramban': 'Jammu & Kashmir',
    'kishtwar': 'Jammu & Kashmir',
    'khour': 'Jammu & Kashmir',
    'akhnoor': 'Jammu & Kashmir',
    'nagrota': 'Jammu & Kashmir',

    # Ladakh
    'leh': 'Ladakh',
    'kargil': 'Ladakh',

    # Arunachal Pradesh
    'naharlagun': 'Arunachal Pradesh',
    'itanagar': 'Arunachal Pradesh',
    'tawang': 'Arunachal Pradesh',
    'bomdila': 'Arunachal Pradesh',
    'pasighat': 'Arunachal Pradesh',
    'ziro': 'Arunachal Pradesh',
    'along': 'Arunachal Pradesh',
    'tezu': 'Arunachal Pradesh',

    # Manipur
    'imphal': 'Manipur',
    'churachandpur': 'Manipur',
    'bishnupur': 'Manipur',
    'thoubal': 'Manipur',
    'senapati': 'Manipur',
    'ukhrul': 'Manipur',

    # Meghalaya
    'shillong': 'Meghalaya',
    'tura': 'Meghalaya',
    'jowai': 'Meghalaya',
    'nongstoin': 'Meghalaya',

    # Mizoram
    'aizawl': 'Mizoram',
    'lunglei': 'Mizoram',
    'champhai': 'Mizoram',

    # Nagaland
    'kohima': 'Nagaland',
    'dimapur': 'Nagaland',
    'mokokchung': 'Nagaland',
    'wokha': 'Nagaland',

    # Sikkim
    'gangtok': 'Sikkim',
    'namchi': 'Sikkim',
    'gyalshing': 'Sikkim',

    # Tripura
    'agartala': 'Tripura',
    'udaipur': 'Tripura',  # note: also a city in Rajasthan — handled by postal code
    'dharmanagar': 'Tripura',

    # Andaman & Nicobar
    'port blair': 'Andaman & Nicobar',

    # Lakshadweep
    'kavaratti': 'Lakshadweep',

    # Chandigarh
    'chandigarh': 'Chandigarh',

    # Puducherry
    'puducherry': 'Puducherry',
    'pondicherry': 'Puducherry',
    'mahe': 'Puducherry',
    'yanam': 'Puducherry',

    # Delhi
    'new delhi': 'Delhi',
    'delhi': 'Delhi',

    # Common major cities (for records where state field was skipped)
    'mumbai': 'Maharashtra',
    'pune': 'Maharashtra',
    'nagpur': 'Maharashtra',
    'nashik': 'Maharashtra',
    'aurangabad': 'Maharashtra',
    'solapur': 'Maharashtra',
    'kolhapur': 'Maharashtra',
    'thane': 'Maharashtra',
    'navi mumbai': 'Maharashtra',
    'pimpri': 'Maharashtra',
    'pimpri-chinchwad': 'Maharashtra',
    'ahmednagar': 'Maharashtra',
    'satara': 'Maharashtra',
    'sangli': 'Maharashtra',
    'jalgaon': 'Maharashtra',
    'akola': 'Maharashtra',
    'latur': 'Maharashtra',
    'amravati': 'Maharashtra',

    'bengaluru': 'Karnataka',
    'bangalore': 'Karnataka',
    'mysuru': 'Karnataka',
    'mysore': 'Karnataka',
    'hubli': 'Karnataka',
    'dharwad': 'Karnataka',
    'mangaluru': 'Karnataka',
    'mangalore': 'Karnataka',
    'belagavi': 'Karnataka',
    'belgaum': 'Karnataka',
    'kalaburagi': 'Karnataka',
    'gulbarga': 'Karnataka',
    'davanagere': 'Karnataka',
    'shivamogga': 'Karnataka',
    'tumkur': 'Karnataka',
    'udupi': 'Karnataka',

    'chennai': 'Tamil Nadu',
    'coimbatore': 'Tamil Nadu',
    'madurai': 'Tamil Nadu',
    'tiruchirappalli': 'Tamil Nadu',
    'salem': 'Tamil Nadu',
    'tirunelveli': 'Tamil Nadu',
    'vellore': 'Tamil Nadu',
    'erode': 'Tamil Nadu',
    'thoothukudi': 'Tamil Nadu',

    'hyderabad': 'Telangana',
    'warangal': 'Telangana',
    'nizamabad': 'Telangana',
    'karimnagar': 'Telangana',
    'khammam': 'Telangana',

    'visakhapatnam': 'Andhra Pradesh',
    'vijayawada': 'Andhra Pradesh',
    'guntur': 'Andhra Pradesh',
    'nellore': 'Andhra Pradesh',
    'kurnool': 'Andhra Pradesh',
    'kakinada': 'Andhra Pradesh',
    'tirupati': 'Andhra Pradesh',
    'rajahmundry': 'Andhra Pradesh',

    'thiruvananthapuram': 'Kerala',
    'trivandrum': 'Kerala',
    'kochi': 'Kerala',
    'kozhikode': 'Kerala',
    'calicut': 'Kerala',
    'thrissur': 'Kerala',
    'kollam': 'Kerala',
    'palakkad': 'Kerala',
    'malappuram': 'Kerala',
    'kannur': 'Kerala',
    'alappuzha': 'Kerala',
    'kasaragod': 'Kerala',
    'wayanad': 'Kerala',
    'idukki': 'Kerala',
    'pathanamthitta': 'Kerala',

    'kolkata': 'West Bengal',
    'howrah': 'West Bengal',
    'siliguri': 'West Bengal',
    'asansol': 'West Bengal',
    'durgapur': 'West Bengal',
    'bardhaman': 'West Bengal',
    'burdwan': 'West Bengal',
    'malda': 'West Bengal',
    'murshidabad': 'West Bengal',
    'jalpaiguri': 'West Bengal',
    'cooch behar': 'West Bengal',

    'bhubaneswar': 'Odisha',
    'cuttack': 'Odisha',
    'rourkela': 'Odisha',
    'berhampur': 'Odisha',
    'sambalpur': 'Odisha',
    'puri': 'Odisha',

    'patna': 'Bihar',
    'gaya': 'Bihar',
    'bhagalpur': 'Bihar',
    'muzaffarpur': 'Bihar',
    'darbhanga': 'Bihar',
    'purnia': 'Bihar',
    'arrah': 'Bihar',
    'begusarai': 'Bihar',
    'katihar': 'Bihar',
    'munger': 'Bihar',

    'ranchi': 'Jharkhand',
    'jamshedpur': 'Jharkhand',
    'dhanbad': 'Jharkhand',
    'bokaro': 'Jharkhand',
    'deoghar': 'Jharkhand',
    'hazaribagh': 'Jharkhand',
    'dumka': 'Jharkhand',

    'lucknow': 'Uttar Pradesh',
    'kanpur': 'Uttar Pradesh',
    'agra': 'Uttar Pradesh',
    'varanasi': 'Uttar Pradesh',
    'allahabad': 'Uttar Pradesh',
    'prayagraj': 'Uttar Pradesh',
    'meerut': 'Uttar Pradesh',
    'bareilly': 'Uttar Pradesh',
    'aligarh': 'Uttar Pradesh',
    'moradabad': 'Uttar Pradesh',
    'gorakhpur': 'Uttar Pradesh',
    'noida': 'Uttar Pradesh',
    'ghaziabad': 'Uttar Pradesh',
    'mathura': 'Uttar Pradesh',
    'ayodhya': 'Uttar Pradesh',
    'faizabad': 'Uttar Pradesh',
    'muzaffarnagar': 'Uttar Pradesh',
    'saharanpur': 'Uttar Pradesh',
    'jhansi': 'Uttar Pradesh',
    'shahjahanpur': 'Uttar Pradesh',
    'rampur': 'Uttar Pradesh',
    'firozabad': 'Uttar Pradesh',
    'hapur': 'Uttar Pradesh',
    'sitapur': 'Uttar Pradesh',
    'hathras': 'Uttar Pradesh',
    'bulandshahr': 'Uttar Pradesh',
    'bahraich': 'Uttar Pradesh',
    'lakhimpur': 'Uttar Pradesh',
    'hardoi': 'Uttar Pradesh',
    'unnao': 'Uttar Pradesh',
    'rae bareli': 'Uttar Pradesh',
    'raebareli': 'Uttar Pradesh',
    'amroha': 'Uttar Pradesh',
    'shamli': 'Uttar Pradesh',
    'bijnor': 'Uttar Pradesh',
    'kasganj': 'Uttar Pradesh',
    'etah': 'Uttar Pradesh',
    'mainpuri': 'Uttar Pradesh',
    'farrukhabad': 'Uttar Pradesh',
    'kannauj': 'Uttar Pradesh',
    'auraiya': 'Uttar Pradesh',
    'etawah': 'Uttar Pradesh',
    'hamirpur': 'Uttar Pradesh',
    'banda': 'Uttar Pradesh',
    'chitrakoot': 'Uttar Pradesh',
    'mahoba': 'Uttar Pradesh',
    'lalitpur': 'Uttar Pradesh',
    'ballia': 'Uttar Pradesh',
    'mau': 'Uttar Pradesh',
    'azamgarh': 'Uttar Pradesh',
    'jaunpur': 'Uttar Pradesh',
    'sultanpur': 'Uttar Pradesh',
    'ambedkar nagar': 'Uttar Pradesh',
    'pratapgarh': 'Uttar Pradesh',
    'mirzapur': 'Uttar Pradesh',
    'sonbhadra': 'Uttar Pradesh',
    'chandauli': 'Uttar Pradesh',
    'ghazipur': 'Uttar Pradesh',
    'deoria': 'Uttar Pradesh',
    'kushinagar': 'Uttar Pradesh',
    'maharajganj': 'Uttar Pradesh',
    'siddharthnagar': 'Uttar Pradesh',
    'basti': 'Uttar Pradesh',
    'sant kabir nagar': 'Uttar Pradesh',
    'balrampur': 'Uttar Pradesh',
    'shravasti': 'Uttar Pradesh',
    'gonda': 'Uttar Pradesh',
    'barabanki': 'Uttar Pradesh',

    'jaipur': 'Rajasthan',
    'jodhpur': 'Rajasthan',
    'kota': 'Rajasthan',
    'ajmer': 'Rajasthan',
    'bikaner': 'Rajasthan',
    'alwar': 'Rajasthan',
    'bharatpur': 'Rajasthan',
    'sikar': 'Rajasthan',
    'tonk': 'Rajasthan',
    'nagaur': 'Rajasthan',
    'barmer': 'Rajasthan',
    'jaisalmer': 'Rajasthan',
    'chittorgarh': 'Rajasthan',
    'bhilwara': 'Rajasthan',
    'dungarpur': 'Rajasthan',
    'banswara': 'Rajasthan',
    'karauli': 'Rajasthan',
    'sawai madhopur': 'Rajasthan',
    'dausa': 'Rajasthan',
    'dholpur': 'Rajasthan',

    'ahmedabad': 'Gujarat',
    'surat': 'Gujarat',
    'vadodara': 'Gujarat',
    'rajkot': 'Gujarat',
    'bhavnagar': 'Gujarat',
    'jamnagar': 'Gujarat',
    'anand': 'Gujarat',
    'gandhinagar': 'Gujarat',
    'mehsana': 'Gujarat',
    'patan': 'Gujarat',
    'morbi': 'Gujarat',
    'kutch': 'Gujarat',
    'bhuj': 'Gujarat',
    'junagadh': 'Gujarat',
    'porbandar': 'Gujarat',
    'amreli': 'Gujarat',
    'godhra': 'Gujarat',
    'kheda': 'Gujarat',
    'ankleshwar': 'Gujarat',
    'bharuch': 'Gujarat',
    'navsari': 'Gujarat',
    'valsad': 'Gujarat',
    'dahod': 'Gujarat',

    'bhopal': 'Madhya Pradesh',
    'indore': 'Madhya Pradesh',
    'jabalpur': 'Madhya Pradesh',
    'gwalior': 'Madhya Pradesh',
    'ujjain': 'Madhya Pradesh',
    'sagar': 'Madhya Pradesh',
    'rewa': 'Madhya Pradesh',
    'satna': 'Madhya Pradesh',
    'katni': 'Madhya Pradesh',
    'chhindwara': 'Madhya Pradesh',
    'ratlam': 'Madhya Pradesh',
    'mandsaur': 'Madhya Pradesh',
    'neemuch': 'Madhya Pradesh',
    'vidisha': 'Madhya Pradesh',
    'raisen': 'Madhya Pradesh',
    'hoshangabad': 'Madhya Pradesh',
    'narmadapuram': 'Madhya Pradesh',
    'betul': 'Madhya Pradesh',
    'seoni': 'Madhya Pradesh',
    'balaghat': 'Madhya Pradesh',
    'mandla': 'Madhya Pradesh',
    'dindori': 'Madhya Pradesh',
    'damoh': 'Madhya Pradesh',
    'panna': 'Madhya Pradesh',
    'chhatarpur': 'Madhya Pradesh',
    'tikamgarh': 'Madhya Pradesh',
    'shivpuri': 'Madhya Pradesh',
    'guna': 'Madhya Pradesh',
    'ashok nagar': 'Madhya Pradesh',
    'morena': 'Madhya Pradesh',
    'bhind': 'Madhya Pradesh',
    'sheopur': 'Madhya Pradesh',
    'datia': 'Madhya Pradesh',
    'dewas': 'Madhya Pradesh',
    'shajapur': 'Madhya Pradesh',
    'agar malwa': 'Madhya Pradesh',
    'rajgarh': 'Madhya Pradesh',
    'sehore': 'Madhya Pradesh',
    'khandwa': 'Madhya Pradesh',
    'burhanpur': 'Madhya Pradesh',
    'khargone': 'Madhya Pradesh',
    'barwani': 'Madhya Pradesh',
    'dhar': 'Madhya Pradesh',
    'alirajpur': 'Madhya Pradesh',
    'jhabua': 'Madhya Pradesh',
    'sidhi': 'Madhya Pradesh',
    'singrauli': 'Madhya Pradesh',
    'umaria': 'Madhya Pradesh',
    'anuppur': 'Madhya Pradesh',
    'shahdol': 'Madhya Pradesh',

    'dehradun': 'Uttarakhand',
    'haridwar': 'Uttarakhand',
    'rishikesh': 'Uttarakhand',
    'roorkee': 'Uttarakhand',
    'haldwani': 'Uttarakhand',
    'nainital': 'Uttarakhand',
    'almora': 'Uttarakhand',
    'pithoragarh': 'Uttarakhand',
    'rudrapur': 'Uttarakhand',
    'kashipur': 'Uttarakhand',
    'mussoorie': 'Uttarakhand',
    'kotdwar': 'Uttarakhand',
    'pauri': 'Uttarakhand',
    'tehri': 'Uttarakhand',
    'uttarkashi': 'Uttarakhand',
    'champawat': 'Uttarakhand',
    'bageshwar': 'Uttarakhand',
    'chamoli': 'Uttarakhand',

    'guwahati': 'Assam',
    'dibrugarh': 'Assam',
    'silchar': 'Assam',
    'jorhat': 'Assam',
    'nagaon': 'Assam',
    'tinsukia': 'Assam',
    'sivasagar': 'Assam',
    'bongaigaon': 'Assam',
    'dhubri': 'Assam',
    'karimganj': 'Assam',
    'hailakandi': 'Assam',
    'cachar': 'Assam',
    'barpeta': 'Assam',
    'nalbari': 'Assam',
    'kamrup': 'Assam',
    'morigaon': 'Assam',
    'hojai': 'Assam',
    'goalpara': 'Assam',
    'kokrajhar': 'Assam',
    'chirang': 'Assam',
    'baksa': 'Assam',
    'darrang': 'Assam',
    'udalguri': 'Assam',
    'sonitpur': 'Assam',
    'tezpur': 'Assam',
    'lakhimpur': 'Assam',
    'dhemaji': 'Assam',

    'amritsar': 'Punjab',
    'ludhiana': 'Punjab',
    'jalandhar': 'Punjab',
    'patiala': 'Punjab',
    'bathinda': 'Punjab',
    'mohali': 'Punjab',
    'pathankot': 'Punjab',
    'hoshiarpur': 'Punjab',
    'gurdaspur': 'Punjab',
    'kapurthala': 'Punjab',
    'firozpur': 'Punjab',
    'faridkot': 'Punjab',
    'muktsar': 'Punjab',
    'moga': 'Punjab',
    'sangrur': 'Punjab',
    'barnala': 'Punjab',
    'fatehgarh sahib': 'Punjab',
    'rupnagar': 'Punjab',
    'nawanshahr': 'Punjab',
    'tarn taran': 'Punjab',
    'fazilka': 'Punjab',

    'shimla': 'Himachal Pradesh',
    'dharamshala': 'Himachal Pradesh',
    'mandi': 'Himachal Pradesh',
    'solan': 'Himachal Pradesh',
    'kullu': 'Himachal Pradesh',
    'bilaspur': 'Himachal Pradesh',
    'hamirpur': 'Himachal Pradesh',
    'una': 'Himachal Pradesh',
    'kangra': 'Himachal Pradesh',
    'chamba': 'Himachal Pradesh',
    'kinnaur': 'Himachal Pradesh',
    'lahaul': 'Himachal Pradesh',
    'spiti': 'Himachal Pradesh',
    'sirmaur': 'Himachal Pradesh',

    'faridabad': 'Haryana',
    'gurugram': 'Haryana',
    'gurgaon': 'Haryana',
    'ambala': 'Haryana',
    'panipat': 'Haryana',
    'sonipat': 'Haryana',
    'hisar': 'Haryana',
    'rohtak': 'Haryana',
    'karnal': 'Haryana',
    'yamunanagar': 'Haryana',
    'panchkula': 'Haryana',
    'bhiwani': 'Haryana',
    'sirsa': 'Haryana',
    'fatehabad': 'Haryana',
    'jind': 'Haryana',
    'kaithal': 'Haryana',
    'kurukshetra': 'Haryana',
    'mahendragarh': 'Haryana',
    'rewari': 'Haryana',
    'jhajjar': 'Haryana',
    'nuh': 'Haryana',
    'mewat': 'Haryana',
    'palwal': 'Haryana',

    'raipur': 'Chhattisgarh',
    'bhilai': 'Chhattisgarh',
    'bilaspur': 'Chhattisgarh',
    'durg': 'Chhattisgarh',
    'korba': 'Chhattisgarh',
    'rajnandgaon': 'Chhattisgarh',
    'jagdalpur': 'Chhattisgarh',
    'ambikapur': 'Chhattisgarh',
    'raigarh': 'Chhattisgarh',
    'mahasamund': 'Chhattisgarh',
    'dhamtari': 'Chhattisgarh',
    'kanker': 'Chhattisgarh',
    'bastar': 'Chhattisgarh',
    'kondagaon': 'Chhattisgarh',
    'narayanpur': 'Chhattisgarh',
    'bijapur': 'Chhattisgarh',
    'sukma': 'Chhattisgarh',
    'dantewada': 'Chhattisgarh',
    'kabirdham': 'Chhattisgarh',
    'mungeli': 'Chhattisgarh',
    'balod': 'Chhattisgarh',
    'baloda bazar': 'Chhattisgarh',
    'gariaband': 'Chhattisgarh',
    'bemetara': 'Chhattisgarh',
    'balrampur': 'Chhattisgarh',
    'surajpur': 'Chhattisgarh',
    'korea': 'Chhattisgarh',
    'surguja': 'Chhattisgarh',
    'jashpur': 'Chhattisgarh',
}

# ---------------------------------------------------------------------------
# Postal code prefix → State lookup
# Indian postal codes are 6 digits; first 2-3 digits identify the state
# ---------------------------------------------------------------------------
PINCODE_PREFIX_TO_STATE = {
    '11': 'Delhi',
    '12': 'Haryana',
    '13': 'Haryana',
    '14': 'Punjab',
    '15': 'Punjab',
    '16': 'Punjab',
    '17': 'Himachal Pradesh',
    '18': 'Jammu & Kashmir',
    '19': 'Jammu & Kashmir',
    '20': 'Uttar Pradesh',
    '21': 'Uttar Pradesh',
    '22': 'Uttar Pradesh',
    '23': 'Uttar Pradesh',
    '24': 'Uttar Pradesh',
    '25': 'Uttar Pradesh',
    '26': 'Uttarakhand',
    '27': 'Uttar Pradesh',
    '28': 'Uttar Pradesh',
    '30': 'Rajasthan',
    '31': 'Rajasthan',
    '32': 'Rajasthan',
    '33': 'Rajasthan',
    '34': 'Rajasthan',
    '36': 'Gujarat',
    '37': 'Gujarat',
    '38': 'Gujarat',
    '39': 'Gujarat',
    '40': 'Maharashtra',
    '41': 'Maharashtra',
    '42': 'Maharashtra',
    '43': 'Maharashtra',
    '44': 'Maharashtra',
    '45': 'Madhya Pradesh',
    '46': 'Madhya Pradesh',
    '47': 'Madhya Pradesh',
    '48': 'Madhya Pradesh',
    '49': 'Chhattisgarh',
    '50': 'Telangana',
    '51': 'Telangana',
    '52': 'Andhra Pradesh',
    '53': 'Andhra Pradesh',
    '54': 'Andhra Pradesh',
    '56': 'Karnataka',
    '57': 'Karnataka',
    '58': 'Karnataka',
    '59': 'Karnataka',
    '60': 'Tamil Nadu',
    '61': 'Tamil Nadu',
    '62': 'Tamil Nadu',
    '63': 'Tamil Nadu',
    '64': 'Tamil Nadu',
    '65': 'Tamil Nadu',
    '66': 'Tamil Nadu',
    '67': 'Kerala',
    '68': 'Kerala',
    '69': 'Kerala',
    '70': 'West Bengal',
    '71': 'West Bengal',
    '72': 'West Bengal',
    '73': 'West Bengal',
    '74': 'West Bengal',
    '75': 'Odisha',
    '76': 'Odisha',
    '77': 'Odisha',
    '78': 'Assam',
    '79': 'Arunachal Pradesh',
    '80': 'Bihar',
    '81': 'Bihar',
    '82': 'Bihar',
    '83': 'Jharkhand',
    '84': 'Bihar',
    '85': 'Bihar',
    '90': 'Arunachal Pradesh',
    '91': 'Arunachal Pradesh',
    '92': 'Manipur',
    '93': 'Nagaland',
    '94': 'Manipur',
    '95': 'Mizoram',
    '96': 'Tripura',
    '97': 'Meghalaya',
    '98': 'Sikkim',
    '99': 'Arunachal Pradesh',
}

INDIA_STATE_CODES = {
    'AP': 'Andhra Pradesh', 'AR': 'Arunachal Pradesh', 'AS': 'Assam',
    'BR': 'Bihar', 'CG': 'Chhattisgarh', 'GA': 'Goa', 'GJ': 'Gujarat',
    'HR': 'Haryana', 'HP': 'Himachal Pradesh', 'JH': 'Jharkhand',
    'KA': 'Karnataka', 'KL': 'Kerala', 'MP': 'Madhya Pradesh',
    'MH': 'Maharashtra', 'MN': 'Manipur', 'ML': 'Meghalaya',
    'MZ': 'Mizoram', 'NL': 'Nagaland', 'OD': 'Odisha', 'OR': 'Odisha',
    'PB': 'Punjab', 'RJ': 'Rajasthan', 'SK': 'Sikkim', 'TN': 'Tamil Nadu',
    'TS': 'Telangana', 'TR': 'Tripura', 'UP': 'Uttar Pradesh',
    'UK': 'Uttarakhand', 'WB': 'West Bengal', 'DL': 'Delhi',
    'JK': 'Jammu & Kashmir', 'LA': 'Ladakh', 'PY': 'Puducherry',
    'CH': 'Chandigarh', 'AN': 'Andaman & Nicobar',
}


def lookup_state_by_city(city: str) -> str | None:
    if not city:
        return None
    return CITY_TO_STATE.get(city.strip().lower())


def lookup_state_by_pincode(pincode: str) -> str | None:
    if not pincode:
        return None
    pincode = str(pincode).strip().replace(' ', '')
    if len(pincode) >= 2:
        return PINCODE_PREFIX_TO_STATE.get(pincode[:2])
    return None


def lookup_state_by_address(address: str) -> tuple[str | None, str | None]:
    """
    Try to extract state from address text.
    Returns (state, state_code).
    """
    if not address:
        return None, None
    # Look for State: XX pattern first
    import re
    m = re.search(r'state\s*:\s*([A-Za-z &]+)', address, re.I)
    if m:
        val = m.group(1).strip()
        if len(val) <= 3:
            full = INDIA_STATE_CODES.get(val.upper())
            if full:
                return full, val.upper()[:2]
        return val, None
    return None, None


def reverse_geocode(lat: float, lng: float) -> str | None:
    """Use OpenStreetMap Nominatim to get state from coordinates."""
    try:
        r = requests.get(
            'https://nominatim.openstreetmap.org/reverse',
            params={'lat': lat, 'lon': lng, 'format': 'json'},
            headers={'User-Agent': 'HateWatch/1.0 (hatewatch@example.com)'},
            timeout=10,
        )
        data = r.json()
        address = data.get('address', {})
        return (
            address.get('state') or
            address.get('province') or
            address.get('region')
        )
    except Exception as e:
        log.warning('Reverse geocode failed for %s,%s: %s', lat, lng, e)
        return None


# ---------------------------------------------------------------------------
# Main enrichment
# ---------------------------------------------------------------------------

def fetch_missing_state_records(conn, batch_size: int, offset: int) -> list:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, city, address, geolocation_raw, postal_code, latitude, longitude
            FROM incidents
            WHERE state IS NULL
            AND retracted = FALSE
            AND deleted_at IS NULL
            ORDER BY id
            LIMIT %s OFFSET %s
        """, (batch_size, offset))
        return [dict(r) for r in cur.fetchall()]


def count_missing_state(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM incidents
            WHERE state IS NULL AND retracted = FALSE AND deleted_at IS NULL
        """)
        return cur.fetchone()[0]


def enrich_record(row: dict) -> dict | None:
    """
    Try all methods to find state for a record.
    Returns dict with state (and optionally state_code) or None if not found.
    """
    result = {}

    # Method 1: city lookup
    state = lookup_state_by_city(row.get('city'))
    if state:
        result['state'] = state
        result['method'] = 'city_lookup'
        return result

    # Method 2: address block parsing
    state, code = lookup_state_by_address(row.get('address', ''))
    if state:
        result['state'] = state
        if code:
            result['state_code'] = code
        result['method'] = 'address_parse'
        return result

    # Method 3: postal code prefix
    state = lookup_state_by_pincode(row.get('postal_code'))
    if state:
        result['state'] = state
        result['method'] = 'pincode_lookup'
        return result

    # Method 4: reverse geocode (only if coordinates exist)
    if row.get('latitude') and row.get('longitude'):
        state = reverse_geocode(row['latitude'], row['longitude'])
        if state:
            result['state'] = state
            result['method'] = 'reverse_geocode'
            time.sleep(1)  # Nominatim rate limit: 1 req/sec
            return result

    return None


def apply_enrichments(conn, enrichments: list[dict], dry_run: bool) -> int:
    if dry_run or not enrichments:
        return 0

    updates = []
    for e in enrichments:
        updates.append((
            e['state'],
            e.get('state_code'),
            e['id'],
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE incidents
            SET state = %s,
                state_code = COALESCE(%s, state_code),
                updated_at = NOW()
            WHERE id = %s
            """,
            updates,
            page_size=200,
        )
    conn.commit()
    return len(updates)


def run(dry_run: bool, batch_size: int):
    mode = 'DRY RUN' if dry_run else 'LIVE'
    log.info('=== geo_enrichment.py started (%s) ===', mode)

    conn  = psycopg2.connect(DATABASE_URL)
    total = count_missing_state(conn)
    log.info('Records missing state: %d', total)

    offset        = 0
    total_fixed   = 0
    total_skipped = 0
    method_counts = {}

    while True:
        batch = fetch_missing_state_records(conn, batch_size, offset)
        if not batch:
            break

        enrichments = []
        for row in batch:
            result = enrich_record(row)
            if result:
                result['id'] = row['id']
                enrichments.append(result)
                method_counts[result['method']] = method_counts.get(result['method'], 0) + 1
            else:
                total_skipped += 1

        updated = apply_enrichments(conn, enrichments, dry_run)
        total_fixed += len(enrichments) if dry_run else updated

        log.info(
            'Batch %d–%d: %d/%d enriched',
            offset + 1, offset + len(batch),
            len(enrichments), len(batch),
        )
        offset += len(batch)

    print()
    print('=' * 50)
    print(f'  Geo Enrichment Report ({mode})')
    print('=' * 50)
    print(f'  Total missing state:  {total}')
    print(f'  {"Would fix" if dry_run else "Fixed"}:  {total_fixed}')
    print(f'  Still missing:        {total_skipped}')
    print()
    print('  Methods used:')
    for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
        print(f'    {count:>4}  {method}')
    print('=' * 50)
    if dry_run:
        print('  Run with --fix to apply changes')
    print()

    conn.close()
    log.info('=== geo_enrichment.py complete ===')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='HateWatch geo enrichment')
    parser.add_argument('--fix', action='store_true',
                        help='Apply state enrichment to DB (default: dry-run)')
    parser.add_argument('--batch', type=int, default=500,
                        help='Batch size (default: 500)')
    args = parser.parse_args()
    run(dry_run=not args.fix, batch_size=args.batch)

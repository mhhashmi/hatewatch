#!/usr/bin/env python3
"""
Quick test: verify Jotform API connection and inspect your form data.
Run this BEFORE running the full sync script.
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get('JOTFORM_API_KEY')
FORM_ID = os.environ.get('JOTFORM_FORM_ID')

if not API_KEY or not FORM_ID:
    raise SystemExit("ERROR: JOTFORM_API_KEY or JOTFORM_FORM_ID missing from .env")

BASE_URL = 'https://api.jotform.com'
HEADERS  = {'APIKEY': API_KEY}


def test_1_api_connection():
    """Test 1: Can we reach the Jotform API at all?"""
    print("\n── Test 1: API connection ──────────────────────────")
    r = requests.get(f"{BASE_URL}/user", headers=HEADERS, timeout=10)
    data = r.json()
    if data.get('responseCode') == 200:
        user = data['content']
        print(f"  ✓ Connected successfully")
        print(f"  Account: {user.get('username')} ({user.get('email')})")
        print(f"  Plan:    {user.get('plan')}")
        return True
    else:
        print(f"  ✗ Failed: {data}")
        return False


def test_2_form_exists():
    """Test 2: Does the Form ID exist and is it accessible?"""
    print("\n── Test 2: Form access ─────────────────────────────")
    r = requests.get(f"{BASE_URL}/form/{FORM_ID}", headers=HEADERS, timeout=10)
    data = r.json()
    if data.get('responseCode') == 200:
        form = data['content']
        print(f"  ✓ Form found")
        print(f"  Title:       {form.get('title')}")
        print(f"  Status:      {form.get('status')}")
        print(f"  Submissions: {form.get('count')}")
        print(f"  Created:     {form.get('created_at')}")
        return True
    else:
        print(f"  ✗ Failed: {data}")
        return False


def test_3_form_questions():
    """Test 3: List all question fields in the form."""
    print("\n── Test 3: Form fields ─────────────────────────────")
    r = requests.get(f"{BASE_URL}/form/{FORM_ID}/questions", headers=HEADERS, timeout=10)
    data = r.json()
    if data.get('responseCode') == 200:
        questions = data['content']
        print(f"  ✓ Found {len(questions)} fields:\n")
        print(f"  {'#':<4} {'Label':<45} {'Type':<20}")
        print(f"  {'-'*4} {'-'*45} {'-'*20}")
        for qid, q in sorted(questions.items(), key=lambda x: int(x[0])):
            label = q.get('text') or q.get('name') or '(no label)'
            qtype = q.get('type', '')
            print(f"  {qid:<4} {label:<45} {qtype:<20}")
        return True
    else:
        print(f"  ✗ Failed: {data}")
        return False


def test_4_sample_submission():
    """Test 4: Fetch one submission and show its raw structure."""
    print("\n── Test 4: Sample submission ───────────────────────")
    r = requests.get(
        f"{BASE_URL}/form/{FORM_ID}/submissions",
        headers=HEADERS,
        params={'limit': 1, 'orderby': 'id', 'direction': 'DESC'},
        timeout=10,
    )
    data = r.json()
    if data.get('responseCode') == 200 and data['content']:
        sub = data['content'][0]
        print(f"  ✓ Got submission ID: {sub.get('id')}")
        print(f"  Submitted:  {sub.get('created_at')}")
        print(f"  Status:     {sub.get('status')}")
        print(f"\n  Fields in this submission:")
        print(f"  {'Label':<45} {'Value':<50}")
        print(f"  {'-'*45} {'-'*50}")
        for qid, q in sub.get('answers', {}).items():
            label  = q.get('text') or '(no label)'
            answer = q.get('answer')
            if answer is None:
                continue
            # Truncate long values for display
            val_str = json.dumps(answer) if isinstance(answer, (list, dict)) else str(answer)
            if len(val_str) > 48:
                val_str = val_str[:45] + '...'
            print(f"  {label:<45} {val_str:<50}")
        return True
    else:
        print(f"  ✗ No submissions found or error: {data.get('message')}")
        return False


def test_5_submission_count():
    """Test 5: How many total submissions are there?"""
    print("\n── Test 5: Submission count ────────────────────────")
    r = requests.get(
        f"{BASE_URL}/form/{FORM_ID}/submissions",
        headers=HEADERS,
        params={'limit': 1},
        timeout=10,
    )
    data = r.json()
    if data.get('responseCode') == 200:
        total = data.get('resultSet', {}).get('count', 'unknown')
        print(f"  ✓ Total submissions in form: {total}")
        return True
    else:
        print(f"  ✗ Failed: {data}")
        return False


if __name__ == '__main__':
    print("=" * 60)
    print("Jotform API Connection Test")
    print("=" * 60)

    results = [
        test_1_api_connection(),
        test_2_form_exists(),
        test_3_form_questions(),
        test_4_sample_submission(),
        test_5_submission_count(),
    ]

    print("\n" + "=" * 60)
    passed = sum(results)
    print(f"Results: {passed}/{len(results)} tests passed")
    if passed == len(results):
        print("✓ All good — ready to run jotform_sync.py")
    else:
        print("✗ Fix the errors above before running the sync")
    print("=" * 60)
import requests
import pandas as pd
import time
import json

BASE_URL = "https://ag.wd3.myworkdayjobs.com"
SEARCH_URL = f"{BASE_URL}/wday/cxs/Airbus/Airbus/jobs"
DETAIL_URL_TEMPLATE = f"{BASE_URL}/wday/cxs/Airbus/Airbus/job/{{job_id}}"

HEADERS = {
    "Content-Type": "application/json"
}

# Fetch job listings (summary)
def fetch_job_listings(limit=100):
    jobs = []
    offset = 0

    while True:
        payload = {
            "appliedFacets": {
                "locationCountry": ["a30a87ed25634629aa6c3958aa2b91ea"]  # France
            },
            "limit": limit,
            "offset": offset
        }

        response = requests.post(SEARCH_URL, json=payload, headers=HEADERS)

        if response.status_code != 200:
            print(f"❌ Failed to fetch job list: {response.status_code}")
            print(response.text)
            break

        data = response.json()
        postings = data.get("jobPostings", [])

        if not postings:
            break

        jobs.extend(postings)
        offset += limit
        time.sleep(0.5)  # Be polite

    return jobs

# Fetch full job details by job ID
def fetch_job_details(job_id):
    url = DETAIL_URL_TEMPLATE.format(job_id=job_id)
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"❌ Failed to fetch job {job_id}: {response.status_code}")
        return {}

    return response.json()

# Extract job summaries and enrich with full details
def extract_all_jobs_with_details():
    job_summaries = fetch_job_listings()
    all_jobs = []

    print(f"📄 Found {len(job_summaries)} jobs. Fetching details...")

    for i, job in enumerate(job_summaries, start=1):
        job_id = job.get("jobPostingId")
        job_detail = fetch_job_details(job_id)
        job_info = job_detail.get("jobPostingInfo", {})

        details = {
            "Job Title": job.get("title", ""),
            "Location": job.get("locationsText", ""),
            "Job ID": job_id,
            "Job URL": f"{BASE_URL}/en-US/Airbus{job.get('externalPath', '')}",
            "Job Description": job_info.get("jobDescription", ""),
            "Qualifications": job_info.get("qualifications", ""),
            "Category": job_info.get("jobFamily", {}).get("label", ""),
            "Posted Date": job_info.get("startDate", ""),
            "Employment Type": job_info.get("employmentType", {}).get("label", "")
        }

        all_jobs.append(details)
        print(f"✅ Processed {i}/{len(job_summaries)}: {details['Job Title']}")
        time.sleep(0.2)

    return all_jobs

# Main execution
if __name__ == "__main__":
    job_data = extract_all_jobs_with_details()

    df = pd.DataFrame(job_data)
    df.to_csv("airbus_jobs11.csv", index=False, encoding="utf-8-sig")
    print("\n✅ All job data exported to 'airbus_jobs.csv'")
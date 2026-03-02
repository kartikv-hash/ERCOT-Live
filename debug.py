import streamlit as st
import requests

st.title("ERCOT API Debug")

if st.button("Test"):
    hdrs = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://data.ercot.com/",
        "Origin": "https://data.ercot.com",
    }

    urls = [
        "https://data.ercot.com/api/1/services/search/archive/downloadable-files?toc_id=NP4-183-CD&size=3",
        "https://data.ercot.com/api/1/services/search/archive/downloadable-files?toc_id=NP4-183-CD&size=3&startDate=2026-02-01&endDate=2026-02-28",
        "https://data.ercot.com/api/1/services/search/archive?toc_id=NP4-183-CD&size=3",
        "https://data.ercot.com/api/1/action/datastore_search?resource_id=NP4-183-CD&limit=3",
    ]

    for url in urls:
        try:
            r = requests.get(url, headers=hdrs, timeout=15)
            st.markdown(f"**{r.status_code}** `{url}`")
            st.code(r.text[:500])
        except Exception as e:
            st.error(f"{url} → {e}")

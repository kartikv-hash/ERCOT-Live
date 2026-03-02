import streamlit as st
import requests

st.set_page_config(page_title="ERCOT Debug", page_icon="⚡")
st.title("⚡ ERCOT API Debugger")

api_key = st.text_input("API Key", value="69dc2329959147e98eff168605575938", type="password")

URLS = [
    "https://api.ercot.com/api/public-reports/np4-183-cd",
    "https://api.ercot.com/api/public-reports/NP4-183-CD",
    "https://api.ercot.com/api/public/np4-183-cd",
    "https://api.ercot.com/api/public/NP4-183-CD",
    "https://api.ercot.com/api/public-reports",
    "https://api.ercot.com/api/public-reports/dam/hourly/lmp",
    "https://api.ercot.com/api/public-reports/da-hrl-lmp",
]

if st.button("🔍 TEST ALL"):
    for url in URLS:
        for key_method, headers, params in [
            ("header",      {"Ocp-Apim-Subscription-Key": api_key}, {}),
            ("query param", {},                                      {"subscription-key": api_key}),
        ]:
            try:
                r = requests.get(url, headers=headers,
                                 params={"size": 1, **params}, timeout=10)
                color = "green" if r.ok else "red"
                st.markdown(f"**{r.status_code}** `{url}` via *{key_method}*")
                if r.ok:
                    st.success(r.text[:500])
                    st.stop()
                else:
                    st.error(r.text[:300])
            except Exception as e:
                st.error(f"❌ {url} — {e}")


import streamlit as st
import requests

st.set_page_config(page_title="ERCOT Auth Debug", page_icon="🔍")
st.title("🔍 ERCOT Auth Debugger")

# ── Read secrets ───────────────────────────────────────────────────────────────
sub_key  = st.secrets.get("ERCOT_API_KEY",  "").strip()
username = st.secrets.get("ERCOT_USERNAME", "").strip()
password = st.secrets.get("ERCOT_PASSWORD", "").strip()

st.markdown("### Step 1 — Secrets Check")
st.write("ERCOT_API_KEY present:",  "✅" if sub_key  else "❌ MISSING")
st.write("ERCOT_USERNAME present:", "✅" if username else "❌ MISSING")
st.write("ERCOT_PASSWORD present:", "✅" if password else "❌ MISSING")

if not (sub_key and username and password):
    st.error("One or more secrets are missing. Add them in Manage App → Settings → Secrets")
    st.code("""ERCOT_API_KEY  = "your-subscription-key"\nERCOT_USERNAME = "your@email.com"\nERCOT_PASSWORD = "yourPassword" """, language="toml")
    st.stop()

st.success("All 3 secrets found!")

# ── Test token endpoint ────────────────────────────────────────────────────────
st.markdown("### Step 2 — OAuth2 Token Request")
CLIENT_ID = "fec253ea-0d06-4272-a5e6-941d3816b6cc"
TOKEN_URL = "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"

if st.button("🔐 Test Authentication", type="primary"):
    with st.spinner("Contacting ERCOT auth server..."):
        try:
            payload = {
                "grant_type":    "password",
                "username":      username,
                "password":      password,
                "response_type": "id_token",
                "scope":         f"openid {CLIENT_ID} offline_access",
                "client_id":     CLIENT_ID,
            }
            resp = requests.post(TOKEN_URL, data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=15)

            st.write(f"**HTTP Status:** {resp.status_code}")
            st.write("**Response:**")
            try:
                j = resp.json()
                # Hide sensitive parts
                safe = {k: (v[:20]+"..." if isinstance(v,str) and len(v)>20 and "token" in k.lower() else v)
                        for k,v in j.items()}
                st.json(safe)

                if resp.ok:
                    token = j.get("id_token") or j.get("access_token","")
                    if token:
                        st.success(f"✅ Token obtained! Length: {len(token)} chars")

                        # ── Test API call ──────────────────────────────────────
                        st.markdown("### Step 3 — Test API Call")
                        with st.spinner("Calling DAM LMP endpoint..."):
                            api_resp = requests.get(
                                "https://api.ercot.com/api/public-reports/np4-183-cd/dam_hourly_lmp",
                                headers={
                                    "Authorization": f"Bearer {token}",
                                    "Ocp-Apim-Subscription-Key": sub_key,
                                    "Accept": "application/json"
                                },
                                params={"busName":"HB_HOUSTON","deliveryDateFrom":"2026-02-01",
                                        "deliveryDateTo":"2026-02-02","size":5},
                                timeout=20
                            )
                        st.write(f"**API HTTP Status:** {api_resp.status_code}")
                        try:
                            api_json = api_resp.json()
                            st.json(api_json)
                            if api_resp.ok:
                                st.success("🎉 LIVE DATA WORKING! Real ERCOT LMP data is flowing.")
                            else:
                                st.error(f"API call failed: {api_resp.status_code}")
                        except:
                            st.write("Raw response:", api_resp.text[:500])
                    else:
                        st.warning(f"Response OK but no token found. Keys: {list(j.keys())}")
                else:
                    st.error(f"Auth failed: {resp.status_code}")
                    if "error_description" in j:
                        st.error(f"Reason: {j['error_description']}")

            except Exception as je:
                st.write("Raw response text:", resp.text[:500])

        except requests.exceptions.ConnectionError:
            st.error("❌ Connection error — cannot reach ERCOT auth server")
        except Exception as e:
            st.error(f"❌ Exception: {e}")

st.markdown("---")
st.caption("This is a temporary debug page. Replace with main dashboard once auth works.")

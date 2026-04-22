import streamlit as st
import uuid
from datetime import datetime
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
import os

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Partner Interview Portal",
    page_icon="📋",
    layout="wide"
)

# --------------------------------------------------
# PREMIUM STYLING
# --------------------------------------------------
st.markdown("""
<style>

/* Background */
[data-testid="stAppViewContainer"] {
    background: linear-gradient(135deg, #f4f6f9 0%, #eef2f7 100%);
}

/* Spacing */
.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
}

/* Header Banner */
.header-banner {
    background: linear-gradient(90deg, #FF3621, #ff6b4a);
    padding: 1.5rem;
    border-radius: 14px;
    color: white;
    text-align: center;
    font-size: 26px;
    font-weight: 600;
    margin-bottom: 2rem;
    box-shadow: 0 6px 20px rgba(0,0,0,0.08);
}

/* Form Card */
div[data-testid="stForm"] {
    background-color: white;
    padding: 2rem;
    border-radius: 16px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.06);
    border: 1px solid #f0f0f0;
}

/* Section Titles */
.section-title {
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 1rem;
    color: #333;
}

/* Input fields */
input, textarea, select {
    border-radius: 8px !important;
}

/* Buttons */
.stButton>button {
    background: linear-gradient(90deg, #FF3621, #ff6b4a);
    color: white;
    border-radius: 10px;
    height: 3.2em;
    font-weight: 600;
    font-size: 16px;
    border: none;
    transition: 0.3s ease-in-out;
}

.stButton>button:hover {
    background: linear-gradient(90deg, #e22e1a, #ff5030);
    transform: translateY(-2px);
    box-shadow: 0 6px 14px rgba(255, 54, 33, 0.3);
}

/* Success */
div[data-testid="stAlert-success"] {
    background-color: #e8f5e9;
    border-left: 6px solid #2e7d32;
    border-radius: 10px;
}

/* Error */
div[data-testid="stAlert-error"] {
    background-color: #fdecea;
    border-left: 6px solid #c62828;
    border-radius: 10px;
}

</style>
""", unsafe_allow_html=True)

# Header Banner
st.markdown(
    '<div class="header-banner">🚀 Databricks Partner Interview Portal</div>',
    unsafe_allow_html=True
)
#----------
#volume path
#-----------
VOLUME_PATH = "/dbfs/Volumes/main/dpp360/resumes/"
# os.makedirs(VOLUME_PATH, exist_ok=True)

from base64 import b64encode

def save_resume_to_volume(file, candidate_name):
    if file is None:
        return None

    try:
        unique_id = str(uuid.uuid4())
        safe_name = candidate_name.replace(" ", "_")

        filename = f"{file.name}"

        volume_path = f"/Volumes/main/dpp360/resumes/{filename}"

        # ✅ Upload using Databricks SDK
        w.files.upload(
            file_path=volume_path,          # destination
            contents=file.getvalue(),       # file bytes
            overwrite=True
        )

        return volume_path  # ✅ store in Delta

    except Exception as e:
        st.error(f"File upload failed: {str(e)}")
        return None
    
# --------------------------------------------------
# CONFIG
# --------------------------------------------------
FEEDBACK_HTTP_PATH = "/sql/1.0/warehouses/d539b79e8ef0cddf"
INTERVIEW_HTTP_PATH = "/sql/1.0/warehouses/d539b79e8ef0cddf"

INTERVIEW_WORKFLOW_JOB_ID = 1020077429148788

cfg = Config()
w = WorkspaceClient()

query_params = st.query_params
form_type = query_params.get("form", "feedback")

# ==================================================
# FEEDBACK FORM
# ==================================================
if form_type == "feedback":

    st.markdown('<div class="section-title">📝 Interview Feedback</div>', unsafe_allow_html=True)

    with st.form("feedback_form"):

        col1, col2 = st.columns(2)

        with col1:
            PartnerResourceName = st.text_input("Partner Resource Name")
            PartnerName = st.text_input("Partner Organization Name")
            Role = st.selectbox("Role", [
                "Solutions Consultant(SC)",
                "Senior Solutions Consultant(Sr.SC)",
                "Resident Solution Architect(RSA)",
                "Project Manager(PM)",
                "Senior Engineer",
                "Technical Program Manager"
            ])

        with col2:
            InterviewerName = st.text_input("Interviewer Name")
            InterviewDate = st.date_input("Interview Date")
            InterviewResult = st.selectbox(
                "Interview Result",
                ["Selected", "Rejected", "Ramp Up Required"]
            )

        DetailFeedback = st.text_area("Detailed Feedback", height=150)

        submitted = st.form_submit_button("✅ Submit Feedback")

    if submitted:
        if PartnerResourceName and PartnerName and InterviewerName:

            interview_ts = datetime.combine(InterviewDate, datetime.min.time())

            connection = sql.connect(
                server_hostname=cfg.host,
                http_path=FEEDBACK_HTTP_PATH,
                credentials_provider=lambda: cfg.authenticate,
            )

            cursor = connection.cursor()

            cursor.execute("""
                INSERT INTO main.dpp360.feedback
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()),
                PartnerResourceName,
                PartnerName,
                Role,
                InterviewerName,
                interview_ts,
                InterviewResult,
                DetailFeedback
            ))

            connection.commit()
            cursor.close()
            connection.close()

            st.success("🎉 Feedback submitted successfully!")
        else:
            st.error("Please fill required fields.")

# ==================================================
# INTERVIEW FORM
# ==================================================
elif form_type == "interview":

    st.markdown('<div class="section-title">📅 Interview Scheduling</div>', unsafe_allow_html=True)

    with st.form("interview_form"):

        col1, col2 = st.columns(2)

        with col1:
            PartnerOrganisationName = st.text_input("Partner Organisation Name")
            RequestorEmail = st.text_input("Requestor Email")
            ResourceName = st.text_input("Candidate Name")
            ResourceEmail = st.text_input("Candidate Email")
            ResourceResume = st.file_uploader("Upload Resume", type=["pdf", "doc", "docx"])

        with col2:
            ResourceLocation = st.text_input("Resource Location")
            Role = st.selectbox("Role", [
                "-- Select Role --",
                "Solutions Consultant(SC)",
                "Senior Solutions Consultant(Sr.SC)",
                "Resident Solution Architect(RSA)",
                "Project Manager(PM)",
                "Tooling Expert",
                "Technical Program Manager",
                "Others"],index =0
                )

            DatabricksManager = st.selectbox(
                "Databricks Manager",
                ["-- Select Databricks Manager --","VJ (AMER)", "Joseph (APAC)", "Aniket (IND)",
                 "Databricks Partner Coordinator (AMER)",
                 "Kelly (AMER)", "Sumit (GDC)",
                 "Databricks Partner Coordinator (NEMEA)",
                 "Hunter (AMER)", "Parul (EMEA)",
                 "Vincent De Stoecklin (ANZ)"],index = 0
            )

            PreferredWorkingTimeZone = st.selectbox(
                "Preferred Working Time Zone",
                ["-- Select Timezone -- ", "AEST", "CET", "CST", "EST",
                 "GMT", "IST", "MST", "PST"],index=0
            )

            NamedCustomer = st.selectbox("Named Customer", ["-- Select --","Yes", "No"],index=0)
            Interview_date = st.date_input("📅 Date")
            Interview_time = st.time_input("⏰ Time")
            InterviewSlot1 = datetime.combine(Interview_date, Interview_time)


        submitted = st.form_submit_button("🚀 Schedule Interview")

    if submitted:
        if PartnerOrganisationName and ResourceName:

            interview_slot_ts = datetime.combine(InterviewSlot1, datetime.min.time())

             # ✅ SAVE FILE TO VOLUME
            resume_content = save_resume_to_volume(
                ResourceResume, ResourceName
            )
            interview_id = str(uuid.uuid4())

            connection = sql.connect(
                server_hostname=cfg.host,
                http_path=INTERVIEW_HTTP_PATH,
                credentials_provider=lambda: cfg.authenticate,
            )

            cursor = connection.cursor()

            cursor.execute("""
                INSERT INTO main.dpp360.interview(partner_name, partner_requestor_email_id, partner_resource_name,
     resource_email_id, resource_resume, resource_location, role,
     preferred_working_timezone, named_customer, interview_slot_1)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                PartnerOrganisationName,
                RequestorEmail,
                ResourceName,
                ResourceEmail,
                resume_content,
                ResourceLocation,
                Role,
                PreferredWorkingTimeZone,
                NamedCustomer,
                interview_slot_ts
            ))

            connection.commit()
            cursor.close()
            connection.close()

            # Trigger Email Job
            # try:
            #     w.jobs.run_now(
            #         job_id=INTERVIEW_WORKFLOW_JOB_ID,
            #         job_parameters={
            #             "interview_id": interview_id,
            #             "partner_org": PartnerOrganisationName,
            #             "candidate_name": ResourceName,
            #             "requestor_email": RequestorEmail,
            #             "interview_slot_ts": interview_slot_ts,
            #             "resource_email" :ResourceEmail,
            #             "named_customer" : NamedCustomer
            #         }
            #     )
            #     st.success("🎉 Interview scheduled successfully! Email sent.")
            # except Exception as e:
            #     st.success("Interview scheduled successfully!")
            #     st.warning(f"Email notification failed: {str(e)}")

        else:
            st.error("Please fill required fields.")

elif form_type == "manage":

    import pandas as pd

    st.markdown('<div class="section-title">📊 Manage Interviews</div>', unsafe_allow_html=True)

    # -------------------------------
    # CONNECT
    # -------------------------------
    conn = sql.connect(
        server_hostname=cfg.host,
        http_path=INTERVIEW_HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )
    cursor = conn.cursor()

    # -------------------------------
    # LOAD DATA
    # -------------------------------
    cursor.execute("""
    SELECT id,
           partner_resource_name,
           interview_status,
           interview_slot_1,
           databricks_manager,
           partner_requestor_email_id,
           resource_email_id
    FROM main.dpp360.interview
    ORDER BY id DESC
    """)

    rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=[
    "id",
    "Candidate Name",
    "Status",
    "Interview Slot",
    "Manager",
    "Requestor Email",
    "Candidate Email"
    ])

    if df.empty:
        st.info("No records found")

    else:
        # -------------------------------
        # FILTER
        # -------------------------------
        st.write("### 🔍 Filter")

        status_filter = st.selectbox(
            "Filter by Status",
            ["All", "To be Scheduled", "Scheduled", "Completed", "Rejected"]
        )

        if status_filter != "All":
            df = df[df["Status"] == status_filter]

        # -------------------------------
        # DOWNLOAD BUTTON
        # -------------------------------
        st.download_button(
            "📥 Download CSV",
            df.to_csv(index=False),
            "interviews.csv"
        )

        st.write("### ✏️ Edit Interview Records")

        # -------------------------------
        # TABLE EDITOR
        # -------------------------------
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "Status": st.column_config.SelectboxColumn(
                    "Interview Status",
                    options=[
                        "To be Scheduled",
                        "Scheduled",
                        "Completed",
                        "Rejected"
                    ]
                ),
                "Interview Slot": st.column_config.DatetimeColumn(
                    "Interview Slot",
                    format="YYYY-MM-DD HH:mm"
                )
            },
            disabled=["id"]
        )

        # -------------------------------
        # SAVE BUTTON
        # -------------------------------
        if st.button("💾 Save Changes"):

            updates_made = 0

            for i in range(len(df)):
                original = df.iloc[i]
                updated = edited_df.iloc[i]

                if not original.equals(updated):

                    slot = updated["Interview Slot"]

                    # ✅ FIX timestamp issue
                    if pd.notna(slot):
                        slot = slot.to_pydatetime().replace(tzinfo=None)
                    else:
                        slot = None

                    cursor.execute("""
                        UPDATE main.dpp360.interview
                        SET interview_status = ?, interview_slot_1 = ?
                        WHERE id = ?
                    """, (
                        updated["Status"],
                        slot,
                        int(updated["id"])
                    ))

                    updates_made += 1
                    new_status = updated["Status"]
                    old_status = original["Status"]
                    manager = updated["Manager"]

                    # 🚀 Trigger ONLY when status changes to Scheduled
                    if old_status != "Scheduled" and new_status == "Scheduled":

                        # Optional: manager condition
                        if manager in ["VJ (AMER)", "Joseph (APAC)", "Aniket (IND)"]:

                            try:
                                w.jobs.run_now(
                                    job_id=INTERVIEW_WORKFLOW_JOB_ID,
                                    job_parameters={
                                        "interview_id": str(updated["id"]),
                                        "candidate_name": updated["Candidate Name"],
                                        "manager": manager,
                                        "requestor_email": updated["Requestor Email"],
                                        "candidate_email": updated["Candidate Email"],
                                        "interview_slot": str(slot)
                                    }
                                )

                                st.info(f"📧 Email triggered for {updated['Candidate Name']}")

                            except Exception as e:
                                st.warning(f"⚠️ Email failed: {str(e)}")
                    

            conn.commit()

            # -------------------------------
            # SUCCESS MESSAGE (VISIBLE)
            # -------------------------------
            if updates_made > 0:
                st.success(f"✅ {updates_made} record(s) updated successfully!")
            else:
                st.info("No changes detected.")

            # # -------------------------------
            # # LIGHT REFRESH (NO RERUN BUG)
            # # -------------------------------
            # st.experimental_set_query_params(refresh=str(uuid.uuid4()))

    # -------------------------------
    # CLEANUP
    # -------------------------------
    cursor.close()
    conn.close()

else:
    st.error("Unknown form type.")